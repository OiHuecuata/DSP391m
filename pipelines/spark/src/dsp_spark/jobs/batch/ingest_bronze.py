"""Ingest source-record datasets while preserving immutable run envelopes."""

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path

from delta.tables import DeltaTable
from dsp_collector.runtime.bronze import (
    extract_collection_status,
    extract_observations,
    load_policies,
)
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ...io.delta import merge_delta, read_delta
from ...processing.clean.bronze import persist_observations, reject_bronze
from ...processing.clean.contracts import parse_json, validate
from ...runtime.config import SparkSettings
from ...runtime.spark import create_spark

BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("schema_version", T.StringType(), False),
        T.StructField("source", T.StringType(), False),
        T.StructField("run_id", T.StringType(), False),
        T.StructField("collected_at", T.StringType(), False),
        T.StructField("ingested_at", T.TimestampType(), False),
        T.StructField("artifact_uri", T.StringType(), False),
        T.StructField("payload_json", T.StringType(), False),
    ]
)


def ingest_bronze(spark, artifact: str | Path, settings: SparkSettings, *, source: str):
    if source not in ("threads", "pbplus", "youtube"):
        raise ValueError("Bronze source must be threads, pbplus or youtube")
    path = Path(artifact).resolve()
    # Decode bytes directly so even CRLF, whitespace and JSON property order
    # survive; do not reserialize or flatten raw source fields.
    envelope = {}
    try:
        raw = path.read_bytes().decode("utf-8")
        envelope = parse_json(raw)
        if not isinstance(envelope, dict):
            raise TypeError("Bronze envelope must be an object")
        validate(envelope, source + "_bronze")
        if envelope["source"] != source:
            raise ValueError("artifact source conflicts with requested source")
        datasets, errors = extract_observations(
            envelope,
            artifact_uri=path.as_uri(),
            policies=load_policies(os.getenv("DSP_SOURCE_POLICY_REGISTRY")),
        )
    except Exception as exc:
        reject_bronze(
            spark,
            settings,
            source,
            envelope.get("run_id") if isinstance(envelope, dict) else None,
            path.as_uri(),
            [{"error": str(exc)}],
        )
        raise
    row = (
        envelope["schema_version"],
        envelope["source"],
        envelope["run_id"],
        envelope["collected_at"],
        datetime.now(UTC),
        path.as_uri(),
        raw,
    )
    frame = spark.createDataFrame([row], BRONZE_SCHEMA)
    target = getattr(settings.paths, source + "_bronze")
    if DeltaTable.isDeltaTable(spark, target):
        saved = read_delta(spark, target).filter(
            (F.col("source") == source) & (F.col("run_id") == envelope["run_id"])
        )
        old = saved.select("payload_json").first()
        if old and parse_json(old[0]) != envelope:
            # Historical run envelopes are immutable; a changed run is rejected.
            error = "conflicting payload for existing Bronze collection run"
            reject_bronze(
                spark,
                settings,
                source,
                envelope["run_id"],
                path.as_uri(),
                [{"error": error}],
            )
            raise ValueError(error)
    counts = persist_observations(spark, datasets, settings)
    reject_bronze(spark, settings, source, envelope["run_id"], path.as_uri(), errors)
    merge_delta(frame, target, keys=("source", "run_id"))
    return {
        "source": source,
        "run_id": envelope["run_id"],
        "input_count": 1,
        "datasets": counts,
        "rejected_count": len(errors),
        "status": datasets["bronze_collection_runs"][0]["execution_status"],
        "path": target,
    }


def backfill_bronze(spark, settings, *, sources=("pbplus", "threads", "youtube")):
    """Repeatable non-destructive extraction of original persisted envelopes.

    Does not require historical artifact_uri files to remain on disk. Reads the
    current Delta snapshot, preserving all archive files and transaction logs.
    """
    results = []
    policies = load_policies(os.getenv("DSP_SOURCE_POLICY_REGISTRY"))
    for source in sources:
        path = getattr(settings.paths, source + "_bronze")
        if not DeltaTable.isDeltaTable(spark, path):
            continue
        for saved in (
            read_delta(spark, path)
            .orderBy(F.to_timestamp("collected_at"), "run_id")
            .toLocalIterator()
        ):
            envelope = parse_json(saved.payload_json)
            validate(envelope, source + "_bronze")
            if envelope["run_id"] != saved.run_id or envelope["source"] != source:
                raise ValueError("archived envelope lineage conflicts with Delta row")
            datasets, errors = extract_observations(
                envelope, artifact_uri=saved.artifact_uri, policies=policies
            )
            counts = persist_observations(spark, datasets, settings)
            reject_bronze(
                spark, settings, source, saved.run_id, saved.artifact_uri, errors
            )
            results.append(
                {
                    "source": source,
                    "run_id": saved.run_id,
                    "datasets": counts,
                    "rejected_count": len(errors),
                }
            )
    return results


def ingest_collection_status(spark, artifact, settings):
    path = Path(artifact).resolve()
    status = parse_json(path.read_text())
    datasets, _errors = extract_collection_status(
        status,
        artifact_uri=path.as_uri(),
        policies=load_policies(os.getenv("DSP_SOURCE_POLICY_REGISTRY")),
    )
    return persist_observations(spark, datasets, settings)


def main(argv=None, *, spark=None, settings=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("threads", "pbplus", "youtube"))
    parser.add_argument("--artifact", type=Path)
    parser.add_argument("--backfill", action="store_true")
    parser.add_argument("--status-file", type=Path)
    args = parser.parse_args(argv)
    if (
        not args.status_file
        and not args.backfill
        and (not args.source or not args.artifact)
    ):
        parser.error(
            "--source and --artifact are required unless --backfill is selected"
        )
    settings = settings or SparkSettings.from_env()
    owned = spark is None
    spark = create_spark(settings) if owned else spark
    try:
        if args.status_file:
            print(
                json.dumps(ingest_collection_status(spark, args.status_file, settings))
            )
            return
        print(
            json.dumps(
                backfill_bronze(
                    spark,
                    settings,
                    sources=(
                        (args.source,)
                        if args.source
                        else ("pbplus", "threads", "youtube")
                    ),
                )
                if args.backfill
                else ingest_bronze(spark, args.artifact, settings, source=args.source)
            )
        )
    finally:
        if owned:
            spark.stop()


if __name__ == "__main__":
    main()
