"""Persist canonical Court observations without normalizing them again."""

import argparse
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import types as T

from ...io.delta import merge_delta
from ...processing.clean.contracts import parse_json, spark_schema, validate
from ...runtime.config import SparkSettings
from ...runtime.ownership import job_ownership
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


def main(argv=None, *, settings=None, ownership=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", required=True, type=Path)
    args = parser.parse_args(argv)
    settings = settings or SparkSettings.from_env()
    with job_ownership(settings, outputs=("courts",), ownership=ownership):
        spark = create_spark(settings)
        try:
            print(ingest_pbplus(spark, args.artifact, settings))
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
