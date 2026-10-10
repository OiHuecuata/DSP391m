"""Admission -> immutable raw readback -> R02A native commits -> verified v2 refs."""

from delta.tables import DeltaTable
from dsp_collector.normalizers.observation import (
    canonical_json,
    digest,
    parse_json,
    timestamp,
)
from dsp_collector.normalizers.observation_v2 import project, validate
from dsp_collector.publishers.observation import build
from dsp_collector.publishers.observation import validate as validate_event
from dsp_collector.runtime.durable import sha256
from dsp_collector.runtime.source_policy import admit, policy_hash, validate_payload
from pyspark.sql import functions as F

from ...io.delta import merge_delta, read_delta
from ...processing.clean.contracts import spark_schema
from ...processing.clean.contracts import validate as contract_validate


def _insert_exact(spark, rows, path, schema, key):
    unique = {}
    for row in rows:
        if row[key] in unique and unique[row[key]] != row:
            raise ValueError("Conflicting immutable rows")
        unique[row[key]] = row
    rows = list(unique.values())
    if DeltaTable.isDeltaTable(spark, path):
        frame = read_delta(spark, path)
        if frame.schema.simpleString() != schema.simpleString():
            raise ValueError("Native schema drift")
        existing = {
            r[key]: r.asDict(recursive=True)
            for r in frame.filter(F.col(key).isin(list(unique))).collect()
        }
        if any(existing[k] != row for k, row in unique.items() if k in existing):
            raise ValueError("Immutable Delta conflict")
        rows = [row for row in rows if row[key] not in existing]
    merge_delta(
        spark.createDataFrame(rows, schema), path, keys=(key,), initialize_empty=True
    )
    saved = read_delta(spark, path).filter(F.col(key).isin(list(unique))).collect()
    if sorted(canonical_json(r.asDict(recursive=True)) for r in saved) != sorted(
        canonical_json(r) for r in unique.values()
    ):
        raise ValueError("Delta commit/readback failure")
    return int(DeltaTable.forPath(spark, path).history(1).first().version)


def ingest(spark, *, store, reference, settings, policy, fixture=False):
    origin = admit(policy, fixture=fixture)
    raw = store.read(reference["artifact_uri"])
    if sha256(raw) != reference["content_sha256"]:
        raise ValueError("Raw exact-byte checksum mismatch")
    envelope = parse_json(raw)
    validate_payload(envelope, policy)
    if envelope.get("source_policy_sha256") != policy_hash(policy):
        raise PermissionError("Reviewed policy changed since the accepted source fetch")
    if (
        envelope["native_observation_context"]["origin"] != origin
        or envelope["native_observation_context"]["permission_reference"]
        != policy["reference"]
    ):
        raise PermissionError("Saved fetch was not admitted by this source review")
    rows = project(
        envelope, artifact_uri=reference["artifact_uri"], artifact_sha256=sha256(raw)
    )
    context = envelope["native_observation_context"]
    artifact = {
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
    }
    artifact.update(
        schema_version="2",
        artifact_id="sha256:"
        + digest(
            [
                "observation-artifact.v2",
                envelope["run_id"],
                context["query_id"],
                context["fetch_batch_id"],
            ]
        ),
        source_platform="threads",
        collection_run_id=envelope["run_id"],
        collected_at=timestamp(envelope["collected_at"]),
        artifact_uri=reference["artifact_uri"],
        artifact_sha256=sha256(raw),
        payload_json=raw.decode("utf-8"),
        native_record_count=len(rows),
    )
    artifact["retention_expires_at"] = timestamp(artifact["retention_expires_at"])
    contract_validate(artifact, "observation_artifact_v2")
    # Both preflight conflicts are checked before committing either native table.
    artifact_version = _insert_exact(
        spark,
        [artifact],
        settings.paths.observation_artifacts,
        spark_schema("observation_artifact_v2"),
        "artifact_id",
    )
    record_version = _insert_exact(
        spark,
        rows,
        settings.paths.record_observations,
        spark_schema("observation_v2"),
        "observation_id",
    )
    # Registered observation-grain projections, retaining the R02A record model.
    for name, kind in (
        ("bronze_social_posts", "Post"),
        ("bronze_social_comments", "Comment"),
        ("bronze_engagement_snapshots", None),
    ):
        selected = [row for row in rows if kind is None or row["record_type"] == kind]
        _insert_exact(
            spark,
            selected,
            settings.paths.base + "/bronze/" + name,
            spark_schema("observation_v2"),
            "observation_id",
        )
    receipt = {
        "record_path": settings.paths.record_observations,
        "record_version": record_version,
        "artifact_path": settings.paths.observation_artifacts,
        "artifact_version": artifact_version,
        "artifact_id": artifact["artifact_id"],
    }
    events = [build(row, receipt) for row in rows]
    return events


def resolve(spark, event, *, settings, store, policy, fixture=False, cache=None):
    validate_event(event)
    origin = admit(policy, fixture=fixture)
    if (
        event["record_path"] != settings.paths.record_observations
        or event["artifact_path"] != settings.paths.observation_artifacts
    ):
        raise ValueError("Event targets are outside configured native storage")

    def row_at(path, version, key, value):
        if cache is not None and (path, version, key, value) in cache:
            return cache[(path, version, key, value)]
        rows = (
            spark.read.format("delta")
            .option("versionAsOf", version)
            .load(path)
            .filter(F.col(key) == value)
            .limit(2)
            .collect()
        )
        if len(rows) != 1:
            raise ValueError("Verified Bronze evidence missing or duplicated")
        return rows[0].asDict(recursive=True)

    row = row_at(
        event["record_path"],
        event["record_version"],
        "observation_id",
        event["observation_id"],
    )
    validate(row)
    if row["origin"] != origin or row["permission_reference"] != policy["reference"]:
        raise PermissionError("Native policy lineage mismatch")
    if any(
        event[k] != row[k]
        for k in (
            "source_platform",
            "source_record_id",
            "record_type",
            "collection_run_id",
            "query_id",
            "fetch_batch_id",
            "artifact_uri",
            "artifact_sha256",
            "payload_hash",
        )
    ):
        raise ValueError("Event and native lineage disagree")
    artifact = row_at(
        event["artifact_path"],
        event["artifact_version"],
        "artifact_id",
        event["artifact_id"],
    )
    raw_key = ("raw", row["artifact_uri"])
    raw = cache.get(raw_key) if cache is not None else None
    if raw is None:
        raw = store.read(row["artifact_uri"])
        if cache is not None:
            cache[raw_key] = raw
    if (
        sha256(raw) != row["artifact_sha256"]
        or raw.decode() != artifact["payload_json"]
    ):
        raise ValueError("Original raw/artifact parity failure")
    if parse_json(raw).get("source_policy_sha256") != policy_hash(policy):
        raise PermissionError("Native acquisition policy hash mismatch")
    projection_key = ("projection", row["artifact_uri"], sha256(raw))
    expected = cache.get(projection_key) if cache is not None else None
    if expected is None:
        expected = project(
            parse_json(raw),
            artifact_uri=row["artifact_uri"],
            artifact_sha256=sha256(raw),
        )
        if cache is not None:
            cache[projection_key] = expected
    if row not in expected:
        raise ValueError("Bronze record does not project from verified original bytes")
    return row


def resolve_all(spark, events, **kwargs):
    cache, groups = {}, {}
    for event in events:
        validate_event(event)
        settings = kwargs["settings"]
        if (
            event["record_path"] != settings.paths.record_observations
            or event["artifact_path"] != settings.paths.observation_artifacts
        ):
            raise ValueError("Event outside configured Bronze")
        for prefix, key in (("record", "observation_id"), ("artifact", "artifact_id")):
            slot = (event[prefix + "_path"], event[prefix + "_version"], key)
            groups.setdefault(slot, set()).add(event[key])
    for (location, version, key), values in groups.items():
        rows = (
            spark.read.format("delta")
            .option("versionAsOf", version)
            .load(location)
            .filter(F.col(key).isin(sorted(values)))
            .collect()
        )
        if len(rows) != len(values):
            raise ValueError("Missing or duplicate pinned Bronze evidence")
        for row in rows:
            value = row.asDict(recursive=True)
            cache[(location, version, key, value[key])] = value
    return [resolve(spark, event, cache=cache, **kwargs) for event in events]
