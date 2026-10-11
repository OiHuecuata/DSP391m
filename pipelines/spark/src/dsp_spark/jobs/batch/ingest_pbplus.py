"""Persist canonical Court observations without normalizing them again."""

import argparse
from datetime import UTC
from pathlib import Path

from dsp_collector.normalizers.court import normalize_court
from dsp_collector.runtime.bronze import timestamp
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ...io.delta import merge_delta, read_delta
from ...processing.clean.contracts import parse_json, spark_schema, validate
from ...runtime.config import SparkSettings
from ...runtime.spark import create_spark


def ingest_pbplus(spark, artifact: str | Path, settings: SparkSettings):
    path = Path(artifact).resolve()
    batch = parse_json(path.read_bytes().decode("utf-8"))
    if (
        not isinstance(batch, dict)
        or batch.get("source") != "pbplus"
        or batch.get("schema_version") != "1"
    ):
        raise ValueError("expected a PBPlus Silver v1 batch")
    records = batch.get("records")
    if (
        not isinstance(records, list)
        or type(batch.get("count")) is not int
        or batch["count"] != len(records)
        or not isinstance(batch.get("run_id"), str)
        or not batch["run_id"].strip()
    ):
        raise ValueError("Silver batch requires run_id, records and matching count")
    # Fail the batch before any write if a record violates its contract/lineage.
    for record in records:
        validate(record, "court")
        if record["source"] != "pbplus" or record["run_id"] != batch["run_id"]:
            raise ValueError("court lineage conflicts with Silver batch")
    # Spark owns the explode and typed projection; fields come from the contract.
    envelope_schema = T.StructType(
        [T.StructField("records", T.ArrayType(spark_schema("court")), False)]
    )
    frame = (
        spark.createDataFrame([{"records": records}], envelope_schema)
        .select(F.explode("records").alias("record"))
        .select("record.*")
        .withColumn("lakehouse_ingested_at", F.current_timestamp())
        .withColumn("source_artifact_uri", F.lit(path.as_uri()))
    )
    merge_delta(
        frame,
        settings.paths.courts,
        keys=("run_id", "record_id"),
        initialize_empty=True,
    )
    return {
        "run_id": batch["run_id"],
        "input_count": len(records),
        "path": settings.paths.courts,
    }


def ingest_court_bronze(spark, settings, *, run_id=None):
    """Adapt supported PBPlus observations from the record-level Bronze table.

    Future source platforms remain in Bronze until their Silver adapters exist.
    Uses the existing Court contract and replay key; no multi-source resolution.
    """
    import json

    from ...processing.clean.bronze import reject_bronze

    frame = read_delta(spark, settings.paths.bronze_court_observations)
    if run_id:
        frame = frame.filter(frame.collection_run_id == run_id)
    records, uris, rejected = [], {}, 0
    for row in frame.filter("source_platform = 'pbplus'").toLocalIterator():
        try:
            record = normalize_court(
                json.loads(row.raw_payload),
                run_id=row.collection_run_id,
                collected_at=timestamp(row.collected_at.replace(tzinfo=UTC)),
            )
            validate(record, "court")
            records.append(record)
            uris[(record["run_id"], record["record_id"])] = row.artifact_uri
        except (ValueError, TypeError, KeyError) as exc:
            reject_bronze(
                spark,
                settings,
                row.source_platform,
                row.collection_run_id,
                row.artifact_uri,
                [
                    {
                        "observation_id": row.observation_id,
                        "stage": "court_silver_adapter",
                        "error": str(exc),
                    }
                ],
            )
            rejected += 1
    schema = T.StructType(
        list(spark_schema("court"))
        + [T.StructField("source_artifact_uri", T.StringType())]
    )
    canonical = spark.createDataFrame(
        [
            {**r, "source_artifact_uri": uris[(r["run_id"], r["record_id"])]}
            for r in records
        ],
        schema,
    )
    canonical = canonical.withColumn(
        "lakehouse_ingested_at", F.current_timestamp()
    ).select(
        *spark_schema("court").names, "lakehouse_ingested_at", "source_artifact_uri"
    )
    merge_delta(
        canonical,
        settings.paths.courts,
        keys=("run_id", "record_id"),
        initialize_empty=True,
    )
    return {"accepted_observations": len(records), "rejected_observations": rejected}


def main(argv=None, *, spark=None, settings=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path)
    parser.add_argument("--from-bronze", action="store_true")
    args = parser.parse_args(argv)
    if not args.from_bronze and not args.artifact:
        parser.error("--artifact or --from-bronze is required")
    settings = settings or SparkSettings.from_env()
    owned = spark is None
    spark = create_spark(settings) if owned else spark
    try:
        print(
            ingest_court_bronze(spark, settings)
            if args.from_bronze
            else ingest_pbplus(spark, args.artifact, settings)
        )
    finally:
        if owned:
            spark.stop()


if __name__ == "__main__":
    main()
