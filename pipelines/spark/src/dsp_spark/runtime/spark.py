"""Local-mode Spark with compatible Delta, Kafka and optional Hadoop S3A JARs."""

import os
from importlib.metadata import version
from pathlib import Path

from pyspark.sql import SparkSession

from ..io.s3 import s3a_options
from .config import (
    DELTA_VERSION,
    HADOOP_VERSION,
    SCALA_BINARY_VERSION,
    SPARK_VERSION,
    SparkSettings,
)


def packages(settings: SparkSettings, *, kafka: bool = False) -> list[str]:
    selected = [f"io.delta:delta-spark_{SCALA_BINARY_VERSION}:{DELTA_VERSION}"]
    if kafka:
        selected.append(
            f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_BINARY_VERSION}:{SPARK_VERSION}"
        )
    if any(
        getattr(settings.paths, field).startswith("s3a://")
        for field in settings.paths.__dataclass_fields__
    ):
        selected.append(f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}")
    return selected


def create_spark(
    settings: SparkSettings | None = None, *, kafka: bool = False
) -> SparkSession:
    settings = settings or SparkSettings.from_env()
    if version("pyspark") != SPARK_VERSION or version("delta-spark") != DELTA_VERSION:
        raise RuntimeError("Install the repository's locked Spark/Delta runtime")
    if SparkSession.getActiveSession() is not None:
        raise RuntimeError(
            "A SparkSession is already active; reuse it rather than silently changing its runtime"
        )
    builder = (
        SparkSession.builder.master(settings.master)
        .appName(settings.app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.streaming.stopTimeout", "30000")
        .config("spark.ui.enabled", "false")
    )
    # Default Ivy resolution pins transitive dependencies. An explicit JAR list
    # lets air-gapped environments use a previously resolved, verified bundle.
    if jars := os.getenv("DSP_SPARK_JARS"):
        builder = builder.config("spark.jars", jars)
    else:
        builder = builder.config(
            "spark.jars.packages", ",".join(packages(settings, kafka=kafka))
        )
        cache = os.getenv(
            "DSP_SPARK_IVY_DIR", str(Path(__file__).resolve().parents[3] / ".local/ivy")
        )
        builder = builder.config("spark.jars.ivy", cache)
    if any(
        getattr(settings.paths, field).startswith("s3a://")
        for field in settings.paths.__dataclass_fields__
    ):
        for key, value in s3a_options(
            endpoint=settings.s3_endpoint,
            region=settings.s3_region,
            path_style=settings.s3_path_style,
        ).items():
            builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
