"""Replay-safe Bronze -> validated Silver v3 -> Gold P0/P1 materialization.

Bounded batch adapter follows existing finite job conventions. No-op input
fingerprints skip all writes; unchanged text reuses classifications. Delta
reconciliation changes rows by key with explicit schemas, preserving history.
"""

import argparse
import json
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from delta.tables import DeltaTable
from dsp_collector.runtime.bronze import DATASETS, load_policies
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ...io.delta import read_delta, reconcile_delta, replace_delta
from ...processing.clean.contracts import contract_documents, spark_schema, validate
from ...processing.intelligence.catalog import GOLD, KEYS, SILVER, VERSION
from ...processing.intelligence.common import (
    active_observations,
    digest,
    instant,
    timestamp,
    vn_date,
)
from ...processing.intelligence.geography import Geography, load_reference
from ...processing.intelligence.gold import build_gold, supply
from ...processing.intelligence.quality import profile, table_digest, validate_tables
from ...processing.intelligence.silver import build_silver
from ...runtime.config import SparkSettings
from ...runtime.spark import create_spark

MAX_BATCH_ROWS = 250_000
MANIFEST_SCHEMA = T.StructType(
    [
        T.StructField("slot", T.StringType(), False),
        T.StructField("fingerprint", T.StringType(), False),
        T.StructField("payload_json", T.StringType(), False),
    ]
)


def delta_version(spark, path):
    return (
        int(DeltaTable.forPath(spark, path).history(1).first().version)
        if DeltaTable.isDeltaTable(spark, path)
        else None
    )


def rows_from_delta(spark, path, *, limit=MAX_BATCH_ROWS, version=None):
    if not DeltaTable.isDeltaTable(spark, path):
        return []
    frame = (
        spark.read.format("delta").option("versionAsOf", version).load(path)
        if version is not None
        else read_delta(spark, path)
    )
    # Avoid Python host timezone conversion by transferring epoch microseconds.
    timestamps = {
        f.name for f in frame.schema if isinstance(f.dataType, T.TimestampType)
    }
    frame = frame.select(
        *(
            (
                F.unix_micros(f.name).alias(f.name)
                if f.name in timestamps
                else F.col(f.name)
            )
            for f in frame.schema
        )
    )
    result = []
    for record in frame.limit(limit + 1).toLocalIterator():
        if len(result) == limit:
            raise ValueError(
                f"finite intelligence batch exceeds {limit} rows; distributed adapter required"
            )
        row = record.asDict(recursive=True)
        for field in timestamps:
            row[field] = (
                timestamp(
                    datetime(1970, 1, 1, tzinfo=UTC)
                    + timedelta(microseconds=row[field])
                )
                if row[field] is not None
                else None
            )
        for field in frame.schema:
            if (
                isinstance(field.dataType, T.DecimalType)
                and row[field.name] is not None
            ):
                row[field.name] = float(row[field.name])
        result.append(row)
    return result


def typed_frame(spark, name, rows):
    schema = spark_schema(name)
    decimal_fields = {f.name for f in schema if isinstance(f.dataType, T.DecimalType)}
    converted = []
    for row in rows:
        item = dict(row)
        for field in decimal_fields:
            if item[field] is not None:
                value = Decimal(str(item[field]))
                if value != value.quantize(Decimal("0.01")):
                    raise ValueError(f"{name}.{field} exceeds two decimal places")
                item[field] = value
        converted.append(item)
    return spark.createDataFrame(converted, schema)


def prior_money_schema(name):
    # Explicit one-time migration for new intelligence r1-r3 Double money
    # schemas. This whitelist never targets legacy v1 or Bronze datasets.
    return T.StructType(
        [
            T.StructField(
                f.name,
                T.DoubleType() if isinstance(f.dataType, T.DecimalType) else f.dataType,
                f.nullable,
            )
            for f in spark_schema(name)
        ]
    )


def read_manifest(spark, settings):
    rows = rows_from_delta(spark, settings.paths.intelligence_manifest)
    if len(rows) > 1:
        raise ValueError("intelligence manifest must have one current generation")
    return json.loads(rows[0]["payload_json"]) if rows else None


def assert_generation(spark, settings, manifest):
    if not manifest or manifest.get("status") != "VALIDATED":
        raise ValueError("Gold publication requires a completed validated generation")
    current_policies = load_policies(os.getenv("DSP_SOURCE_POLICY_REGISTRY"))
    if manifest.get("policy_fingerprint") != digest(current_policies):
        raise ValueError("source policies changed after materialization")
    if manifest.get("contract_fingerprint") != digest(contract_documents()):
        raise ValueError("contracts changed after materialization")
    cutoff = manifest.get("next_retention_cutoff")
    if cutoff and instant(cutoff) <= datetime.now(UTC):
        raise ValueError("retention cutoff passed; rebuild Gold before publication")
    for name, version in manifest["output_versions"].items():
        if delta_version(spark, getattr(settings.paths, name)) != version:
            raise ValueError(f"{name} changed after validated generation")
    for name, version in manifest["input_versions"].items():
        if delta_version(spark, getattr(settings.paths, name)) != version:
            raise ValueError(f"upstream {name} changed after materialization")


def build_intelligence(
    spark,
    settings,
    *,
    as_of=None,
    reference=None,
    excluded_runs=(),
    policies=None,
    force=False,
    input_versions=None,
    previous_settings=None,
    previous_versions=None,
):
    as_of = instant(as_of or datetime.now(UTC))
    policies = (
        policies
        if policies is not None
        else load_policies(os.getenv("DSP_SOURCE_POLICY_REGISTRY"))
    )
    geography = Geography(
        reference or load_reference(os.getenv("DSP_ADMIN_REFERENCE_PATH"))
    )
    current_versions = {
        name: delta_version(spark, getattr(settings.paths, name)) for name in DATASETS
    }
    if any(current_versions[n] is None for n in DATASETS):
        raise ValueError(
            "all six Bronze observation tables must exist; run Bronze ingestion/backfill first"
        )
    before_versions = current_versions if input_versions is None else input_versions
    if set(before_versions) != set(DATASETS) or any(
        type(v) is not int
        or v < 0
        or current_versions[n] is None
        or v > current_versions[n]
        for n, v in before_versions.items()
    ):
        raise ValueError("invalid pinned Bronze version vector")
    if any(before_versions[n] is None for n in DATASETS):
        raise ValueError(
            "all six Bronze observation tables must exist; run Bronze ingestion/backfill first"
        )
    datasets = {
        name: rows_from_delta(
            spark, getattr(settings.paths, name), version=before_versions[name]
        )
        for name in DATASETS
    }
    for name, rows in datasets.items():
        for row in rows:
            validate(row, name)
    permitted, policy_quality = active_observations(
        datasets, as_of=as_of, policies=policies, excluded_runs=excluded_runs
    )
    fingerprint = digest(
        [
            VERSION,
            contract_documents(),
            geography.reference_hash,
            policies,
            sorted(excluded_runs),
            vn_date(as_of),
            {n: table_digest(r) for n, r in permitted.items()},
            # Eligibility can change inside one VN day without a new Bronze
            # commit (e.g. future-dated source content or measurement clocks).
            sorted(
                r["observation_id"]
                for name in ("bronze_social_posts", "bronze_social_comments")
                for r in permitted[name]
                if r.get("published_at") and instant(r["published_at"]) <= as_of
            ),
            sorted(
                r["observation_id"]
                for r in permitted["bronze_engagement_snapshots"]
                if instant(r["observed_at"]) <= as_of
            ),
        ]
    )
    old_manifest = read_manifest(spark, settings)
    if (
        not force
        and old_manifest
        and old_manifest["fingerprint"] == fingerprint
        and old_manifest.get("policy_fingerprint") == digest(policies)
        and old_manifest.get("contract_fingerprint") == digest(contract_documents())
    ):
        # Output version checks catch a partial failed build or external writer.
        intact = all(
            delta_version(spark, getattr(settings.paths, n)) == v
            for n, v in old_manifest["output_versions"].items()
        )
        if intact:
            # Harmless Bronze duplicate/status commits may change physical
            # versions without changing permitted inputs. Refresh only manifest.
            if old_manifest["input_versions"] != before_versions:
                old_manifest["input_versions"] = before_versions
                replace_delta(
                    spark.createDataFrame(
                        [
                            (
                                "current",
                                fingerprint,
                                json.dumps(old_manifest, sort_keys=True),
                            )
                        ],
                        MANIFEST_SCHEMA,
                    ),
                    settings.paths.intelligence_manifest,
                )
            return {**old_manifest, "execution": "UNCHANGED_INPUT_NO_WRITES"}
    previous = {
        name: rows_from_delta(
            spark,
            getattr((previous_settings or settings).paths, name),
            version=(previous_versions or {}).get(name),
        )
        for name in SILVER
    }
    silver, work = build_silver(permitted, geography, previous=previous)
    bronze_ids = {r["observation_id"] for rows in permitted.values() for r in rows}
    quality = validate_tables(silver, bronze_ids=bronze_ids)
    old_supply = rows_from_delta(
        spark,
        (previous_settings or settings).paths.court_supply_by_admin_version_v2,
        version=(previous_versions or {}).get("court_supply_by_admin_version_v2"),
    )
    generated = max(
        (r["collected_at"] for rows in permitted.values() for r in rows),
        default=timestamp(as_of),
    )
    # Recompute retained historical supply dates from still-permitted evidence;
    # deletion of one source cannot erase unrelated venue contribution/history.
    historical = []
    for day in sorted(
        {r["snapshot_date"] for r in old_supply if r["snapshot_date"] != vn_date(as_of)}
    ):
        past = {
            name: [r for r in rows if vn_date(r["collected_at"]) <= day]
            for name, rows in permitted.items()
        }
        historical_silver, _ = build_silver(past, geography, previous=previous)
        historical.extend(
            supply(
                historical_silver["silver_court_venues"],
                historical_silver["silver_venue_candidates"],
                snapshot_date=day,
                generated_at=generated,
            )
        )
    accepted_by_run = {}
    for name in DATASETS[:3]:
        for row in permitted[name]:
            accepted_by_run.setdefault(
                (row["source_platform"], row["collection_run_id"]), []
            ).append(row["observation_id"])
    runs = [
        {
            **r,
            "accepted_observation_ids": accepted_by_run.get(
                (r["source_platform"], r["collection_run_id"]), []
            ),
        }
        for r in permitted["bronze_collection_runs"]
    ]
    gold = build_gold(
        silver,
        runs,
        geography,
        as_of=as_of,
        generated_at=generated,
        historical_supply=historical,
    )
    gold["court_supply_by_admin_version_v2"] = (
        historical + gold["court_supply_by_admin_version_v2"]
    )
    validate_tables(gold, bronze_ids=bronze_ids)
    profiles = {name: profile(name, rows) for name, rows in (silver | gold).items()}
    # All schemas/quality and all analytical rows validate BEFORE mutation.
    # The single writer then commits Silver before Gold; publish manifest last.
    for name, rows in silver.items():
        reconcile_delta(
            typed_frame(spark, name, rows),
            getattr(settings.paths, name),
            keys=KEYS[name],
            previous_schema=prior_money_schema(name),
        )
    # Verify persisted Silver values before serving downstream analytics.
    for name, rows in silver.items():
        if table_digest(
            rows_from_delta(spark, getattr(settings.paths, name))
        ) != table_digest(rows):
            raise ValueError(f"persisted Silver mismatch: {name}")
    for name, rows in gold.items():
        reconcile_delta(
            typed_frame(spark, name, rows),
            getattr(settings.paths, name),
            keys=KEYS[name],
            previous_schema=prior_money_schema(name),
        )
        if table_digest(
            rows_from_delta(spark, getattr(settings.paths, name))
        ) != table_digest(rows):
            raise ValueError(f"persisted Gold mismatch: {name}")
    after_versions = {
        name: delta_version(spark, getattr(settings.paths, name)) for name in DATASETS
    }
    if input_versions is None and before_versions != after_versions:
        raise RuntimeError(
            "Bronze changed during materialization; retry under the pipeline writer lock"
        )
    retention_cutoffs = []
    for rows in permitted.values():
        for row in rows:
            if row.get("retention_expires_at"):
                retention_cutoffs.append(instant(row["retention_expires_at"]))
            days = policies.get(row["source_platform"], {}).get("retention_days")
            if days:
                retention_cutoffs.append(
                    instant(row["collected_at"]) + timedelta(days=days)
                )
    report = {
        "status": "VALIDATED",
        "execution": "MATERIALIZED",
        "fingerprint": fingerprint,
        "processing_version": VERSION,
        "policy_fingerprint": digest(policies),
        "contract_fingerprint": digest(contract_documents()),
        "next_retention_cutoff": (
            timestamp(min(retention_cutoffs)) if retention_cutoffs else None
        ),
        "as_of": timestamp(as_of),
        "generated_at": generated,
        "input_versions": before_versions,
        "output_versions": {
            n: delta_version(spark, getattr(settings.paths, n))
            for n in (*SILVER, *GOLD)
        },
        "before": {n: profile(n, r) for n, r in datasets.items()},
        "after": profiles,
        "excluded_runs": sorted(excluded_runs),
        "policy_quality": policy_quality,
        "classification_work": work,
        "data_quality": quality,
        "blockers": [
            "No licensed POI geocoder or open POI collector observations verified",
            "No complete official codes, polygons, district/ward master or ward crosswalk",
            "Uncalibrated NLP rules; labeled evaluation and source engagement definitions absent",
            "Physical archive/Kafka/backup erasure requires reviewed retention execution",
            "Finite driver adapter limited to 250000 rows per dataset",
            "Live cloud independence NOT VERIFIED",
        ],
    }
    replace_delta(
        spark.createDataFrame(
            [("current", fingerprint, json.dumps(report, sort_keys=True))],
            MANIFEST_SCHEMA,
        ),
        settings.paths.intelligence_manifest,
    )
    return report


def main(argv=None, *, spark=None, settings=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of")
    parser.add_argument("--exclude-runs-file", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    settings = settings or SparkSettings.from_env()
    owned = spark is None
    spark = create_spark(settings) if owned else spark
    try:
        excluded = (
            json.loads(args.exclude_runs_file.read_text())
            if args.exclude_runs_file
            else json.loads(os.getenv("DSP_INTELLIGENCE_EXCLUDED_RUNS", "[]"))
        )
        report = build_intelligence(
            spark, settings, as_of=args.as_of, excluded_runs=excluded, force=args.force
        )
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(
                json.dumps(report, indent=2, ensure_ascii=False) + "\n"
            )
        print(
            json.dumps(
                {
                    "status": report["status"],
                    "execution": report["execution"],
                    "rows": {n: v["rows"] for n, v in report["after"].items()},
                }
            )
        )
    finally:
        if owned:
            spark.stop()


if __name__ == "__main__":
    main()
