"""Managed Bronze outbox ingestion and immutable Silver/Gold generations.

No service lifecycle changes, crawling, ClickHouse publication or ML. Cloud
writes are explicit opt-in and are forbidden under any bootstrap namespace.
"""

import argparse
import json
import time
from datetime import UTC, datetime
from pathlib import Path

from dsp_collector.runtime.bronze import DATASETS, extract_observations, load_policies
from dsp_collector.runtime.outbox import atomic_json, drain, enqueue
from pyspark.sql import functions as F

from ...io.delta import merge_delta, read_delta
from ...io.lakehouse_control import control_store, writable_root
from ...processing.clean.bronze import persist_collection_runs
from ...processing.clean.contracts import validate
from ...processing.intelligence.catalog import GOLD, SILVER
from ...processing.intelligence.common import digest
from ...runtime.config import LakehousePaths, SparkSettings
from ...runtime.spark import create_spark
from .build_intelligence import delta_version, rows_from_delta
from .ingest_bronze import ingest_bronze
from .lakehouse_generation import (
    build_generation,
    rollback,
    verify_generation,
    version_vector,
)
from .verify_s3_snapshot import frame_digest

ENVELOPES = ("pbplus_bronze", "threads_bronze", "youtube_bronze")


def require_policy(policies, source, *, synthetic=False, derivatives=False):
    policy = policies.get(source, {})
    if (
        not policy.get("policy_reviewed_at")
        or policy.get("storage_permission") is not True
    ):
        raise ValueError(f"reviewed storage permission required: {source}")
    datetime.fromisoformat(policy["policy_reviewed_at"])
    if bool(policy.get("synthetic_only")) != bool(synthetic):
        raise ValueError("synthetic policies cannot authorize real source data")
    if derivatives and policy.get("derived_processing_permission") is not True:
        raise ValueError("explicit derived-processing permission required")
    if "retention_days" not in policy or (
        policy["retention_days"] is not None
        and (type(policy["retention_days"]) is not int or policy["retention_days"] <= 0)
    ):
        raise ValueError(
            "explicit retention policy required (positive days or reviewed null)"
        )


def require_phase_one(path, root):
    report = json.loads(Path(path).read_text())
    if report.get("status") != "PASS" or len(report.get("tables", {})) != 44:
        raise ValueError("Phase 1 requires all 44 snapshot tables to pass")
    if report.get("remote_root", "").rstrip("/") != root.rstrip("/"):
        raise ValueError("Phase 1 report belongs to another bootstrap root")
    if not all(r.get("content_match") is True for r in report["tables"].values()):
        raise ValueError("Phase 1 requires content, schema and count verification")


def authorize_cloud_write(
    base,
    *,
    allow,
    supervised_root,
    command,
    synthetic,
    profile,
    region,
    verification_report,
    bootstrap_root,
):
    """Shared CLI guard. Flags record prior operator approval, never grant it."""
    if not base.startswith("s3a:"):
        return
    if not allow:
        raise ValueError(
            "cloud mutations require explicit --allow-cloud-write after approval"
        )
    require_phase_one(verification_report, bootstrap_root)
    import boto3

    identity = (
        boto3.Session(profile_name=profile, region_name=region)
        .client("sts")
        .get_caller_identity()
    )
    if identity["Arn"].endswith(":root") and (
        not supervised_root or command == "watch"
    ):
        raise ValueError(
            "Root is allowed only for explicitly supervised finite development commands"
        )
    if synthetic and "/staging/" not in base + "/":
        raise ValueError("synthetic cloud writes require an isolated staging prefix")


def union_observations(left, right, keys):
    if left.schema != right.schema:
        raise ValueError("bootstrap/catch-up schema mismatch")
    # Transport metadata may legitimately differ after Kafka replay, but source
    # business values for one immutable observation must never conflict.
    if keys == ("observation_id",):
        fields = [
            n
            for n in left.columns
            if n not in ("ingested_at", "ingestion_metadata", "artifact_uri")
        ]
        a, b = left.select(*fields), right.select(*fields)
        common = a.join(b.select(*keys), list(keys), "left_semi")
        compared = b.join(a.select(*keys), list(keys), "left_semi")
        if (
            common.exceptAll(compared).limit(1).count()
            or compared.exceptAll(common).limit(1).count()
        ):
            raise ValueError("conflicting source values in bootstrap/catch-up overlap")
    return left.unionByName(right.join(left.select(*keys), list(keys), "left_anti"))


def bootstrap(
    spark, settings, control, historical, *, catchup=None, policies, verified=None
):
    """Caller holds writer lock; retry preserves any partially written tables."""
    existing, _ = control.get("bootstrap.json")
    if existing:
        raise ValueError("bootstrap already completed; use catch-up ingestion")
    names = (*DATASETS, *ENVELOPES)
    identity = {
        "historical_root": historical.base,
        "catchup_root": catchup.base if catchup else None,
        "policy_digest": digest(policies),
    }
    plan, _ = control.get("bootstrap-plan.json")
    if plan:
        if any(plan[k] != v for k, v in identity.items()):
            raise ValueError(
                "partial bootstrap must resume with its original roots and policies"
            )
    else:
        if any(
            delta_version(spark, getattr(settings.paths, n)) is not None for n in names
        ):
            raise ValueError("bootstrap requires a new empty authoritative namespace")
        plan = {**identity, "historical_versions": {}, "catchup_versions": {}}
        for name in names:
            path = getattr(historical, name)
            relative = path.removeprefix(historical.base.rstrip("/") + "/")
            version = (
                verified["tables"][relative]["delta_version"]
                if verified
                else delta_version(spark, path)
            )
            if version is None:
                raise ValueError(f"missing historical table: {name}")
            plan["historical_versions"][name] = version
            plan["catchup_versions"][name] = (
                delta_version(spark, getattr(catchup, name)) if catchup else None
            )
        control.put("bootstrap-plan.json", plan, absent=True)
    report = {"status": "VALIDATED", "tables": {}, "historical_root": historical.base}
    if verified:
        report["historical_output_versions"] = {
            name: verified["tables"][
                getattr(historical, name).removeprefix(
                    historical.base.rstrip("/") + "/"
                )
            ]["delta_version"]
            for name in SILVER + GOLD
        }
    for name in names:
        origin = getattr(historical, name)
        version = plan["historical_versions"][name]
        frame = spark.read.format("delta").option("versionAsOf", version).load(origin)
        source_column = "source" if name in ENVELOPES else "source_platform"
        for row in frame.select(source_column).distinct().collect():
            require_policy(policies, row[0])
        keys = (
            ("source", "run_id")
            if name in ENVELOPES
            else (
                ("source_platform", "collection_run_id")
                if name == "bronze_collection_runs"
                else ("observation_id",)
            )
        )
        catchup_version = plan["catchup_versions"][name]
        if catchup:
            path = getattr(catchup, name)
            if catchup_version is not None:
                added = (
                    spark.read.format("delta")
                    .option("versionAsOf", catchup_version)
                    .load(path)
                )
                for row in added.select(source_column).distinct().collect():
                    require_policy(policies, row[0])
                if name == "bronze_collection_runs":
                    frame = frame.unionByName(added)
                else:
                    if name in ENVELOPES:
                        shared = frame.join(
                            added.select(*keys), list(keys), "left_semi"
                        )
                        other = added.join(frame.select(*keys), list(keys), "left_semi")
                        if (
                            shared.select(*keys, "payload_json")
                            .exceptAll(other.select(*keys, "payload_json"))
                            .limit(1)
                            .count()
                        ):
                            raise ValueError("conflicting historical source envelopes")
                    frame = union_observations(frame, added, keys)
        target = getattr(settings.paths, name)
        if name == "bronze_collection_runs":
            persist_collection_runs(frame, target)
            # The persistence rule selects the latest terminal status per run.
            from pyspark.sql import Window

            window = Window.partitionBy(*keys).orderBy(
                F.col("ended_at").desc_nulls_last(), F.col("payload_hash").desc()
            )
            frame = (
                frame.withColumn("_rank", F.row_number().over(window))
                .filter("_rank = 1")
                .drop("_rank")
            )
        else:
            merge_delta(frame, target, keys=keys, initialize_empty=True)
        expected_count, expected_digest = frame_digest(frame)
        count, actual_digest = frame_digest(read_delta(spark, target))
        if (count, actual_digest) != (expected_count, expected_digest):
            raise ValueError(f"bootstrap read-back mismatch: {name}")
        report["tables"][name] = {
            "historical_version": version,
            "catchup_version": catchup_version,
            "target_path": target,
            "target_version": delta_version(spark, target),
            "rows": count,
            "digest": actual_digest,
        }
    report["completed_at"] = datetime.now(UTC).isoformat()
    control.put("bootstrap.json", report, absent=True)
    return report


def commit_envelope(spark, settings, control, state, *, policies, synthetic=False):
    artifact = Path(state["artifact"])
    envelope = json.loads(artifact.read_text())
    require_policy(policies, state["source"], synthetic=synthetic)
    if synthetic and not envelope["run_id"].startswith("synthetic-"):
        raise ValueError("synthetic staging only accepts synthetic run IDs")
    receipt_key = "receipts/" + state["id"] + ".json"
    saved, _ = control.get(receipt_key)
    if saved:
        if saved["payload_sha256"] != state["payload_sha256"]:
            raise ValueError("committed run identity conflicts with pending payload")
        return saved
    validate(envelope, state["source"] + "_bronze")
    records, errors = extract_observations(
        envelope, artifact_uri=artifact.as_uri(), policies=policies
    )
    if errors:
        raise ValueError(
            "source envelope contains rejected records; review before cloud ingestion"
        )
    result = ingest_bronze(
        spark, artifact, settings, source=state["source"], policies=policies
    )
    if result["rejected_count"]:
        raise ValueError("Bronze ingestion reported rejected records")
    for name in DATASETS:
        expected = {r["observation_id"]: r for r in records[name]}
        if not expected:
            continue
        actual = {
            r["observation_id"]: r
            for r in rows_from_delta(spark, getattr(settings.paths, name))
            if r["observation_id"] in expected
        }
        if set(actual) != set(expected) or any(
            any(
                actual[key].get(k) != v
                for k, v in row.items()
                if k not in ("ingested_at", "ingestion_metadata")
            )
            for key, row in expected.items()
        ):
            raise ValueError(f"Bronze commit verification failed: {name}")
    receipt = {
        "status": "committed",
        "run_id": state["run_id"],
        "source": state["source"],
        "payload_sha256": state["payload_sha256"],
        "input_versions": version_vector(spark, settings.paths),
        "committed_at": datetime.now(UTC).isoformat(),
    }
    control.put(receipt_key, receipt, absent=True)
    return receipt


def process_once(
    spark,
    settings,
    control,
    outbox,
    *,
    policies,
    synthetic=False,
    as_of=None,
    force=False,
    notify=None,
):
    with control.writer():
        pointer, _ = control.get("current.json")
        if pointer and pointer.get("paused"):
            raise ValueError(
                "rollback paused processing; explicitly resume before updates"
            )
        ready, _ = control.get("bootstrap.json")
        if not synthetic and not ready:
            raise ValueError(
                "authoritative historical bootstrap must be validated first"
            )

        def durable_notification(state):
            key = "notifications/" + state["id"] + ".json"
            event, _ = control.get(key)
            if event is None:
                control.put(
                    key,
                    {
                        "event": "BRONZE_COMMITTED",
                        "id": state["id"],
                        "receipt": state["receipt"],
                    },
                    absent=True,
                )

        ingested = drain(
            outbox,
            lambda s: commit_envelope(
                spark, settings, control, s, policies=policies, synthetic=synthetic
            ),
            notify=notify or durable_notification,
        )
        if any(r["status"] != "committed" for r in ingested):
            return {"status": "PENDING_OR_FAILED", "ingestion": ingested}
        for name in DATASETS:
            for row in (
                read_delta(spark, getattr(settings.paths, name))
                .select("source_platform")
                .distinct()
                .collect()
            ):
                require_policy(policies, row[0], synthetic=synthetic, derivatives=True)
        generation = build_generation(
            spark, settings, control, policies=policies, as_of=as_of, force=force
        )
        return {"status": "VALIDATED", "ingestion": ingested, "generation": generation}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "enqueue",
            "status",
            "bootstrap",
            "process",
            "watch",
            "verify",
            "rollback",
            "resume",
        ),
    )
    parser.add_argument("--base", required=True)
    parser.add_argument(
        "--outbox", type=Path, default=Path(".local/s3-lakehouse/outbox")
    )
    parser.add_argument("--artifact", type=Path)
    parser.add_argument("--profile", default="dsp391m-spark")
    parser.add_argument("--region", default="ap-southeast-2")
    parser.add_argument("--policies", type=Path)
    parser.add_argument(
        "--bootstrap-root",
        default="s3a://dsp391m-data-lake/bootstrap/20261028T064025Z/lakehouse",
    )
    parser.add_argument(
        "--verification-report",
        type=Path,
        default=Path(".local/s3-lakehouse/snapshot-verification.json"),
    )
    parser.add_argument("--catchup-root")
    parser.add_argument("--generation-id")
    parser.add_argument("--as-of")
    parser.add_argument("--interval", type=int, default=300)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--allow-cloud-write", action="store_true")
    parser.add_argument("--supervised-root", action="store_true")
    parser.add_argument("--synthetic", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    base = writable_root(args.base)
    if args.command == "enqueue":
        if not args.artifact:
            parser.error("enqueue requires --artifact")
        result = enqueue(args.artifact, args.outbox)
        print(json.dumps({"id": result["id"], "status": result["status"]}))
        return
    mutation = args.command not in ("status", "verify")
    if base.startswith("s3a:") and mutation:
        authorize_cloud_write(
            base,
            allow=args.allow_cloud_write,
            supervised_root=args.supervised_root,
            command=args.command,
            synthetic=args.synthetic,
            profile=args.profile,
            region=args.region,
            verification_report=args.verification_report,
            bootstrap_root=args.bootstrap_root,
        )
    control = control_store(base, profile=args.profile, region=args.region)
    if args.command == "status":
        pointer, _ = control.get("current.json")
        print(json.dumps(pointer))
        return
    if mutation and args.command not in ("rollback", "resume") and not args.policies:
        parser.error("reviewed policies are required")
    policies = load_policies(args.policies) if args.policies else {}
    settings = SparkSettings(
        LakehousePaths(base), s3_profile=args.profile, s3_region=args.region
    )
    spark = create_spark(settings)
    try:
        if args.command == "bootstrap":
            with control.writer():
                verified = (
                    json.loads(args.verification_report.read_text())
                    if base.startswith("s3a:")
                    else None
                )
                result = bootstrap(
                    spark,
                    settings,
                    control,
                    LakehousePaths(args.bootstrap_root),
                    catchup=(
                        LakehousePaths(args.catchup_root) if args.catchup_root else None
                    ),
                    policies=policies,
                    verified=verified,
                )
        elif args.command in ("verify", "rollback", "resume"):
            pointer, _ = control.get("current.json")
            key = (
                "generations/" + args.generation_id + ".json"
                if args.generation_id
                else pointer["manifest_key"] if pointer else None
            )
            if not key:
                parser.error("no current generation")
            result, _ = control.get(key)
            if not result:
                parser.error("generation manifest does not exist")
            if args.command == "verify":
                verify_generation(spark, settings, result)
            elif args.command == "rollback":
                with control.writer():
                    result = rollback(spark, settings, control, result["generation_id"])
            else:
                with control.writer():
                    current, etag = control.get("current.json")
                    current.pop("paused", None)
                    control.put("current.json", current, expected=etag)
                    result = current
        else:
            if args.interval < 30:
                parser.error("processing interval must be at least 30 seconds")
            while True:
                try:
                    result = process_once(
                        spark,
                        settings,
                        control,
                        args.outbox,
                        policies=policies,
                        synthetic=args.synthetic,
                        as_of=args.as_of,
                        force=args.force,
                    )
                except Exception as exc:
                    result = {
                        "status": "FAILED",
                        "error_type": type(exc).__name__,
                        "attempted_at": datetime.now(UTC).isoformat(),
                    }
                    if args.report:
                        atomic_json(args.report, result)
                    if args.command != "watch":
                        raise
                if args.report:
                    atomic_json(args.report, result)
                print(
                    json.dumps(
                        {
                            "status": result["status"],
                            "generation_id": result.get("generation", {}).get(
                                "generation_id"
                            ),
                        }
                    ),
                    flush=True,
                )
                if args.command != "watch":
                    if result["status"] != "VALIDATED":
                        raise SystemExit(1)
                    break
                time.sleep(args.interval)
        if args.report:
            atomic_json(args.report, result)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
