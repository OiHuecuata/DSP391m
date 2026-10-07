"""Kafka Silver events to observation-preserving Delta via foreachBatch."""

import argparse
from functools import partial

from pyspark.sql import functions as F
from pyspark.sql import types as T

from ...io.delta import merge_delta
from ...io.kafka import read_social_stream
from ...processing.clean.contracts import (
    contract_documents,
    spark_schema,
    validation_reason,
)
from ...runtime.config import SparkSettings
from ...runtime.spark import create_spark


def split_social_events(frame):
    """Validate the original JSON before Spark parsing can coerce/drop fields.

    The canonical contracts travel with the UDF; executors need no filesystem
    checkout or network schema resolver. Transport identity is also checked.
    """
    documents = contract_documents()
    reason = F.udf(
        partial(validation_reason, kind="event", documents=documents), T.StringType()
    )
    checked = (
        frame.select(
            F.col("value").cast("string").alias("raw_value"),
            F.col("value").alias("raw_value_bytes"),
            F.col("key").cast("string").alias("kafka_key"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("reason", reason("raw_value_bytes"))
        .withColumn("event", F.from_json("raw_value", spark_schema("event")))
    )
    checked = checked.withColumn(
        "reason",
        F.when(F.col("reason").isNotNull(), F.col("reason"))
        .when(F.col("event").isNull(), F.lit("Spark event parsing failed"))
        .when(
            F.col("kafka_key").isNull()
            | (F.col("kafka_key") != F.col("event.record_id")),
            F.lit("Kafka key conflicts with record_id"),
        ),
    )
    valid = (
        checked.filter(F.col("reason").isNull())
        .select(
            "event.payload.*",
            F.col("event.event_id").alias("event_id"),
            F.col("event.emitted_at").alias("emitted_at"),
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
        )
        .withColumn("lakehouse_ingested_at", F.current_timestamp())
    )
    rejected = (
        checked.filter(F.col("reason").isNotNull())
        .select(
            "raw_value",
            "raw_value_bytes",
            "kafka_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "reason",
        )
        .withColumn("rejected_at", F.current_timestamp())
    )
    return valid, rejected


def persist_social_batch(frame, batch_id: int, *, settings: SparkSettings):
    # foreachBatch can run again after a crash. Both writes therefore merge on
    # stable identity. Separate tables are not one cross-table transaction.
    frame = frame.persist()
    try:
        valid, rejected = split_social_events(frame)
        merge_delta(
            rejected,
            settings.paths.rejected_social,
            keys=("kafka_topic", "kafka_partition", "kafka_offset"),
        )
        merge_delta(
            valid,
            settings.paths.social_posts,
            keys=("event_id",),
            initialize_empty=True,
        )
    finally:
        frame.unpersist()


def start_social_stream(spark, settings: SparkSettings, *, available_now: bool = False):
    stream = read_social_stream(spark, settings)
    # An empty source may never invoke foreachBatch. Derive the empty Silver
    # schema through the same projection used by actual Kafka batches.
    empty, _ = split_social_events(spark.createDataFrame([], stream.schema))
    merge_delta(
        empty, settings.paths.social_posts, keys=("event_id",), initialize_empty=True
    )
    writer = (
        stream.writeStream.queryName("social_posts_kafka_to_delta")
        .option("checkpointLocation", settings.paths.social_checkpoint)
        .foreachBatch(partial(persist_social_batch, settings=settings))
    )
    writer = (
        writer.trigger(availableNow=True)
        if available_now
        else writer.trigger(processingTime=settings.trigger_interval)
    )
    return writer.start()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Drain currently available Kafka data, then exit.",
    )
    args = parser.parse_args(argv)
    settings = SparkSettings.from_env()
    spark = create_spark(settings, kafka=True)
    query = None
    try:
        query = start_social_stream(spark, settings, available_now=args.available_now)
        query.awaitTermination()
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
