"""Explicit native companion ingestion on the existing Bronze/ownership boundary.

No stage engine, new transport or lock. Legacy raw and native Delta commits are
separate: failed native commit/readback cannot acknowledge or publish; retry
reconciles the retained raw commit using the same native IDs.
"""

import argparse
import hashlib
from datetime import UTC, datetime
from pathlib import Path

from delta.tables import DeltaTable
from dsp_collector.normalizers.observation import (
    CONTEXT_FIELD,
    canonical_json,
    parse_json,
    project_envelope,
    timestamp,
    validate_observation,
)
from pyspark.sql import functions as F

from ...io.delta import merge_delta, read_delta
from ...processing.clean.contracts import spark_schema, validate
from ...runtime.config import SparkSettings
from ...runtime.ownership import job_ownership
from ...runtime.spark import create_spark


def _comparable(row):
    # Exact retained bytes may be replayed from a new URI. The first URI wins;
    # hashes, native identity, source pointer and all other facts must agree.
    return {k: v for k, v in row.items() if k != "artifact_uri"}


def artifact_id(source, run, context):
    identity = [
        "observation-artifact.v1",
        source,
        run,
        context["query_id"],
        context["fetch_batch_id"],
        context["provenance_scope"],
    ]
    return (
        "sha256:" + hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()
    )


def artifact_record(path, raw, envelope, rows):
    context = envelope[CONTEXT_FIELD]
    return {
        "schema_version": "1",
        "artifact_id": artifact_id(envelope["source"], envelope["run_id"], context),
        "source_platform": envelope["source"],
        "collection_run_id": envelope["run_id"],
        **{
            k: context[k]
            for k in (
                "query_id",
                "fetch_batch_id",
                "provenance_scope",
                "origin",
                "permission_reference",
                "retention_policy_id",
                "retention_expires_at",
            )
        },
        "collected_at": timestamp(envelope["collected_at"]),
        "artifact_uri": path.as_uri(),
        "artifact_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        "payload_json": raw,
        "native_record_count": len(rows),
    }


def _saved_artifact(spark, settings, incoming):
    validate(incoming, "observation_artifact")
    path = settings.paths.observation_artifacts
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    frame = read_delta(spark, path)
    if [(f.name, f.dataType.simpleString()) for f in frame.schema] != [
        (f.name, f.dataType.simpleString())
        for f in spark_schema("observation_artifact")
    ]:
        raise ValueError("Native artifact schema drift")
    rows = (
        frame.filter(F.col("artifact_id") == incoming["artifact_id"]).limit(2).collect()
    )
    if len(rows) > 1:
        raise ValueError("Duplicate native acquisition artifact")
    if not rows:
        return None
    saved = rows[0].asDict(recursive=True)
    validate(saved, "observation_artifact")
    if _comparable(saved) != _comparable(incoming):
        raise ValueError("Conflicting native acquisition artifact")
    return saved


def persist_artifact(spark, path, raw, envelope, rows, settings):
    incoming = artifact_record(path, raw, envelope, rows)
    _saved_artifact(spark, settings, incoming)
    merge_delta(
        spark.createDataFrame([incoming], spark_schema("observation_artifact")),
        settings.paths.observation_artifacts,
        keys=("artifact_id",),
    )
    if _saved_artifact(spark, settings, incoming) is None:
        raise ValueError("Native artifact commit/readback incomplete")
    return {
        "artifact_id": incoming["artifact_id"],
        "artifact_path": settings.paths.observation_artifacts,
        "artifact_version": DeltaTable.forPath(
            spark, settings.paths.observation_artifacts
        )
        .history(1)
        .first()
        .version,
    }


def _saved(spark, settings, rows):
    path = settings.paths.record_observations
    if not DeltaTable.isDeltaTable(spark, path):
        return {}
    frame = read_delta(spark, path)
    # Delta reads nested fields as nullable; compare physical types and enforce
    # required values with the authoritative validator, as existing I/O does.
    expected = [
        (f.name, f.dataType.simpleString()) for f in spark_schema("observation")
    ]
    if [(f.name, f.dataType.simpleString()) for f in frame.schema] != expected:
        raise ValueError("Native Bronze schema drift; implicit evolution denied")
    ids = [r["observation_id"] for r in rows]
    values = frame.filter(F.col("observation_id").isin(ids)).collect() if ids else []
    result = {}
    for value in values:
        row = value.asDict(recursive=True)
        validate_observation(row)
        if row["observation_id"] in result:
            raise ValueError("Duplicate stored native observation")
        result[row["observation_id"]] = row
    for row in rows:
        prior = result.get(row["observation_id"])
        if prior is not None and _comparable(prior) != _comparable(row):
            raise ValueError("Conflicting native observation; nothing may overwrite it")
    return result


def prepare_observations(spark, path, raw, envelope, settings):
    context = envelope[CONTEXT_FIELD]
    expiry = context.get("retention_expires_at")
    if expiry and datetime.fromisoformat(expiry) <= datetime.now(UTC):
        raise ValueError("Native replay retention expired; no write permitted")
    rows = project_envelope(
        envelope,
        artifact_uri=path.as_uri(),
        artifact_sha256=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
    )
    for row in rows:
        validate(row, "observation")
    _saved(spark, settings, rows)  # Conflict detection before any legacy/native write.
    _saved_artifact(spark, settings, artifact_record(path, raw, envelope, rows))
    return rows


def persist_observations(spark, rows, settings):
    """Internal continuation after original envelope commit/readback succeeds."""
    groups = {}
    for row in rows:
        validate(row, "observation")
        groups.setdefault(
            (row["source_platform"], row["collection_run_id"]), []
        ).append(row)
    for (source, run), incoming in groups.items():
        context = incoming[0]
        ident = artifact_id(source, run, context)
        archives = (
            read_delta(spark, settings.paths.observation_artifacts)
            .filter(F.col("artifact_id") == ident)
            .limit(2)
            .collect()
        )
        if len(archives) != 1:
            raise ValueError("Committed native acquisition artifact missing")
        carrier = archives[0].asDict(recursive=True)
        raw = carrier["payload_json"]
        envelope = parse_json(raw)
        from urllib.parse import unquote, urlsplit

        path = Path(unquote(urlsplit(carrier["artifact_uri"]).path))
        expected = project_envelope(
            envelope,
            artifact_uri=carrier["artifact_uri"],
            artifact_sha256=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        )
        if (
            _saved_artifact(
                spark, settings, artifact_record(path, raw, envelope, expected)
            )
            is None
        ):
            raise ValueError("Native artifact integrity failed")
        if sorted(canonical_json(_comparable(r)) for r in expected) != sorted(
            canonical_json(_comparable(r)) for r in incoming
        ):
            raise ValueError(
                "Native observations disagree with captured fetch artifact"
            )
        if context["provenance_scope"] == "FETCH_BATCH":
            continue
        archive = (
            read_delta(spark, getattr(settings.paths, source + "_bronze"))
            .filter((F.col("source") == source) & (F.col("run_id") == run))
            .select("payload_json", "artifact_uri")
            .limit(2)
            .collect()
        )
        if len(archive) != 1:
            raise ValueError(
                "Native observation persistence requires its committed acquisition"
            )
        raw = archive[0].payload_json
        envelope = parse_json(raw)
        expected = project_envelope(
            envelope,
            artifact_uri=archive[0].artifact_uri,
            artifact_sha256=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        )
        if sorted(canonical_json(_comparable(r)) for r in expected) != sorted(
            canonical_json(_comparable(r)) for r in incoming
        ):
            raise ValueError(
                "Native observations disagree with committed acquisition bytes"
            )
    _saved(spark, settings, rows)
    frame = spark.createDataFrame(rows, spark_schema("observation"))
    merge_delta(
        frame,
        settings.paths.record_observations,
        keys=("observation_id",),
        initialize_empty=True,
    )
    saved = _saved(spark, settings, rows)
    if set(saved) != {r["observation_id"] for r in rows}:
        raise ValueError("Native Bronze commit/readback incomplete")
    version = (
        DeltaTable.forPath(spark, settings.paths.record_observations)
        .history(1)
        .first()
        .version
    )
    return {
        "observation_count": len(rows),
        "observation_path": settings.paths.record_observations,
        "observation_version": version,
        "observation_ids": [r["observation_id"] for r in rows],
    }


def verify_silver_lineage(spark, batch, settings):
    """Require full source/run/native/byte and unchanged Silver-value lineage.

    No matching a native ID in another run; no fabricated historical repair.
    New batches only; legacy transport remains an explicit compatibility path.
    """
    if not batch.get("native_bronze_required"):
        return
    reference = batch.get("native_bronze_artifact")
    if not isinstance(reference, dict):
        raise TypeError("Native Silver batch lacks immutable acquisition reference")
    from urllib.parse import unquote, urlsplit

    uri = urlsplit(reference["artifact_uri"])
    if (
        uri.scheme != "file"
        or uri.netloc not in ("", "localhost")
        or uri.query
        or uri.fragment
    ):
        raise ValueError("Unsafe native acquisition reference")
    path = Path(unquote(uri.path)).resolve()
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != reference["content_sha256"]:
        raise ValueError("Native Silver acquisition bytes changed")
    envelope = parse_json(raw)
    source = envelope["source"]
    if (
        source != batch["source"]
        or envelope["run_id"] != batch["run_id"]
        or CONTEXT_FIELD not in envelope
    ):
        raise ValueError("Native Silver acquisition lineage disagrees")
    validate(envelope, source + "_bronze")
    if envelope[CONTEXT_FIELD]["provenance_scope"] != "RUN_ENVELOPE":
        raise ValueError(
            "Fetch-scoped batches require a new Silver/transport adapter; v1 omits fetch identity"
        )
    if not DeltaTable.isDeltaTable(spark, getattr(settings.paths, source + "_bronze")):
        raise ValueError("Verified native acquisition Bronze missing")
    archives = (
        read_delta(spark, getattr(settings.paths, source + "_bronze"))
        .filter((F.col("source") == source) & (F.col("run_id") == envelope["run_id"]))
        .select("payload_json")
        .limit(2)
        .collect()
    )
    if len(archives) != 1 or archives[0].payload_json.encode("utf-8") != raw:
        raise ValueError("Verified native acquisition Bronze missing")
    rows = prepare_observations(spark, path, raw.decode("utf-8"), envelope, settings)
    if (
        _saved_artifact(
            spark, settings, artifact_record(path, raw.decode("utf-8"), envelope, rows)
        )
        is None
    ):
        raise ValueError("Verified native acquisition artifact missing")
    saved = _saved(spark, settings, rows)
    if len(saved) != len(rows):
        raise ValueError("Verified native Bronze observations missing")
    from dsp_collector.normalizers.court import normalize_court
    from dsp_collector.normalizers.social_post import normalize_social_post
    from dsp_collector.normalizers.youtube import normalize_youtube

    by_id = {
        r["legacy_record_id"]: r for r in rows if r["legacy_record_id"] is not None
    }
    records = batch["records"]
    if type(batch.get("count")) is not int or batch["count"] != len(records):
        raise ValueError("Native Silver count disagrees")
    seen = set()
    for record in records:
        kind = "court" if source == "pbplus" else "social"
        validate(record, kind)
        row = by_id.get(record["record_id"])
        if (
            row is None
            or record["run_id"] != envelope["run_id"]
            or record["record_id"] in seen
        ):
            raise ValueError("Uncovered native Silver observation")
        seen.add(record["record_id"])
        payload = parse_json(row["raw_payload"])
        kw = {"run_id": envelope["run_id"], "collected_at": envelope["collected_at"]}
        if source == "threads":
            expected = normalize_social_post(
                payload, normalized_at=record["normalized_at"], **kw
            )
        elif source == "youtube":
            expected = normalize_youtube(
                payload,
                queries=envelope["video_queries"].get(row["native_post_id"], []),
                video_id=(
                    row["native_post_id"] if row["record_type"] == "Comment" else None
                ),
                **kw,
            )
        else:
            expected = normalize_court(
                payload,
                normalized_at=record["normalized_at"],
                geocoded_at=envelope.get("geocoded_at"),
                **kw,
            )
            if (
                expected["address"] is None
                and record["address_source"] == "court_name_fallback"
            ):
                expected.pop("address")
            # Geo enrichment is a separate permitted input, not native PBPlus
            # evidence. Native fields still match; geography stays v1-compatible.
            for name in (
                "latitude",
                "longitude",
                "address_source",
                "geo_status",
                "geocoded_at",
            ):
                expected.pop(name)
        if any(record.get(k) != v for k, v in expected.items()):
            raise ValueError("Native-to-Silver transformation lineage disagrees")


def publish_verified_social(spark, artifact, settings, *, kafka_settings):
    from dsp_collector.publishers.kafka import publish_social_batch

    batch = parse_json(Path(artifact).read_bytes())
    if batch.get("native_bronze_required"):
        return publish_social_batch(
            batch,
            settings=kafka_settings,
            bronze_verifier=lambda value: verify_silver_lineage(spark, value, settings),
        )
    return publish_social_batch(artifact, settings=kafka_settings)


def observation_references(path, rows):
    """Supply exact R01-T03 references; this is not a stage-transition engine."""
    from orchestration.local.processing_manifest import artifact_reference

    refs = []
    for row in rows:
        ref = artifact_reference(
            path,
            source_platform=row["source_platform"],
            collection_run_id=row["collection_run_id"],
            schema_version="1",
            record_count=1,
            # R01 policy_reference is a bounded version/fingerprint token, not
            # an arbitrary permission-evidence URI. Pin the complete assertion.
            policy_reference="sha256:"
            + hashlib.sha256(
                canonical_json(
                    {
                        "permission_reference": row["permission_reference"],
                        "retention_policy_id": row["retention_policy_id"],
                        "retention_expires_at": row["retention_expires_at"],
                        "origin": row["origin"],
                    }
                ).encode("utf-8")
            ).hexdigest(),
        )
        if ref["content_sha256"] != row["artifact_sha256"]:
            raise ValueError("Manifest input artifact changed")
        ref.update(
            kind="observations",
            source_record_id=row["source_record_id"],
            observation_range={
                "first_id": row["observation_id"],
                "last_id": row["observation_id"],
            },
        )
        refs.append(ref)
    return refs


def ingest_observations(spark, artifact, settings):
    from .ingest_bronze import ingest_bronze

    envelope = parse_json(Path(artifact).read_bytes())
    if CONTEXT_FIELD not in envelope:
        raise ValueError("Explicit native observation admission context required")
    return ingest_bronze(spark, artifact, settings, source=envelope["source"])


def main(argv=None, *, settings=None, ownership=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", required=True, type=Path)
    args = parser.parse_args(argv)
    source = parse_json(args.artifact.read_bytes())["source"]
    if source not in ("pbplus", "threads", "youtube"):
        raise ValueError("Unsupported native source")
    settings = settings or SparkSettings.from_env()
    with job_ownership(
        settings,
        outputs=(source + "_bronze", "record_observations", "observation_artifacts"),
        ownership=ownership,
    ):
        spark = create_spark(settings)
        try:
            print(canonical_json(ingest_observations(spark, args.artifact, settings)))
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
