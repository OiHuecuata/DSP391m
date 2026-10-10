"""Preserve a complete run-scoped collector envelope in Delta Bronze."""

import argparse
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import types as T

from ...io.delta import merge_delta, read_delta
from ...processing.clean.contracts import parse_json, validate
from ...runtime.config import SparkSettings
from ...runtime.ownership import job_ownership
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
    raw = path.read_bytes().decode("utf-8")
    envelope = parse_json(raw)
    validate(envelope, source + "_bronze")
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
    merge_delta(frame, target, keys=("source", "run_id"))
    # Insert-only merges keep the first payload under a run identity. Confirm
    # that it really is this artifact before callers publish/ingest Silver.
    # A byte-identical replay may come from another URI; retain the original URI.
    saved = (
        read_delta(spark, target)
        .filter((F.col("source") == source) & (F.col("run_id") == envelope["run_id"]))
        .select("schema_version", "collected_at", "payload_json")
        .limit(2)
        .collect()
    )
    if len(saved) != 1 or (
        saved[0].schema_version,
        saved[0].collected_at,
        saved[0].payload_json,
    ) != (envelope["schema_version"], envelope["collected_at"], raw):
        raise ValueError("Bronze run/envelope verification failed")
    return {
        "source": source,
        "run_id": envelope["run_id"],
        "input_count": 1,
        "path": target,
    }


def main(argv=None, *, settings=None, ownership=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source", required=True, choices=("threads", "pbplus", "youtube")
    )
    parser.add_argument("--artifact", required=True, type=Path)
    args = parser.parse_args(argv)
    settings = settings or SparkSettings.from_env()
    with job_ownership(
        settings, outputs=(args.source + "_bronze",), ownership=ownership
    ):
        spark = create_spark(settings)
        try:
            print(ingest_bronze(spark, args.artifact, settings, source=args.source))
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
