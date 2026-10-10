"""Kafka Silver events to observation-preserving Delta via foreachBatch."""

import argparse
import os
from datetime import UTC, datetime
from functools import partial

from jsonschema import ValidationError
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

SOCIAL_ADDITIONS = ("view_count", "parent_record_id", "video_duration", "tags")


def initialize_social_silver(spark, settings):
    """Initialize an empty canonical social table without contacting Kafka."""
    schema = T.StructType(
        [
            T.StructField("key", T.BinaryType()),
            T.StructField("value", T.BinaryType()),
            T.StructField("topic", T.StringType()),
            T.StructField("partition", T.IntegerType()),
            T.StructField("offset", T.LongType()),
            T.StructField("timestamp", T.TimestampType()),
        ]
    )
    migrate_social_schema(spark, settings)
    migrate_social_comments(spark, settings)
    empty, _ = split_social_events(spark.createDataFrame([], schema))
    merge_delta(
        empty, settings.paths.social_posts, keys=("event_id",), initialize_empty=True
    )


def migrate_social_schema(spark, settings):
    """Explicit nullable additions only; preserve all existing observation files."""
    from delta.tables import DeltaTable

    from ...io.delta import read_delta

    path = settings.paths.social_posts
    if not DeltaTable.isDeltaTable(spark, path):
        return
    current = read_delta(spark, path).schema
    fields = {f.name: f for f in spark_schema("social")}
    missing = [name for name in SOCIAL_ADDITIONS if name not in current.names]
    if missing:
        transport = {
            "event_id": "string",
            "emitted_at": "string",
            "kafka_topic": "string",
            "kafka_partition": "int",
            "kafka_offset": "bigint",
            "kafka_timestamp": "timestamp",
            "lakehouse_ingested_at": "timestamp",
        }
        expected = {
            f.name: f.dataType.simpleString()
            for f in spark_schema("social")
            if f.name not in missing
        }
        expected.update(transport)
        if {f.name: f.dataType.simpleString() for f in current} != expected:
            raise ValueError("Cannot migrate an incompatible SocialPost Silver schema")
        if "`" in path:
            raise ValueError("Invalid Delta path")
        columns = ", ".join(
            f"`{name}` {fields[name].dataType.simpleString()}" for name in missing
        )
        spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMNS ({columns})")


def comment_condition(frame):
    return (F.col("source") == "youtube_comment") | F.coalesce(
        F.col("is_reply"), F.lit(False)
    )


def migrate_social_comments(spark, settings):
    """Copy historical comment observations before atomic removal from posts.

    Delta files/logs remain intact for time travel. On interruption, the copy
    merge safely replays; posts are removed only after comments are committed.
    """
    from delta.tables import DeltaTable

    from ...io.delta import read_delta

    if DeltaTable.isDeltaTable(spark, settings.paths.social_posts):
        frame = read_delta(spark, settings.paths.social_posts)
        comments = frame.filter(comment_condition(frame))
        merge_delta(
            comments,
            settings.paths.social_comments,
            keys=("event_id",),
            initialize_empty=True,
        )
        if not comments.isEmpty():
            DeltaTable.forPath(spark, settings.paths.social_posts).delete(
                "source = 'youtube_comment' OR is_reply = true"
            )


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
    # Nullable additions follow the pre-existing physical Silver columns.
    valid = valid.select(
        *(n for n in valid.columns if n not in SOCIAL_ADDITIONS), *SOCIAL_ADDITIONS
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
    from dsp_collector.publishers.kafka import validate_bronze_event
    from dsp_collector.runtime.bronze import (
        DATASETS,
        apply_source_policy,
        decode,
        encode,
        load_policies,
        timestamp,
    )

    from ...processing.clean.bronze import persist_observations

    # New raw-source events share the topic with legacy normalized events.
    event_type = F.get_json_object(F.col("value").cast("string"), "$.event_type")
    bronze = frame.filter(event_type == "bronze.observed")
    policies = load_policies(os.getenv("DSP_SOURCE_POLICY_REGISTRY"))
    datasets = {name: [] for name in DATASETS}
    failures = []
    for message in bronze.toLocalIterator():
        try:
            event = decode(bytes(message.value).decode("utf-8"))
            row = apply_source_policy(validate_bronze_event(event), policies)
            if bytes(message.key).decode("utf-8") != event["event_id"]:
                raise ValueError("Kafka key conflicts with Bronze observation identity")
            row["ingestion_metadata"] = encode(
                {
                    "kafka_topic": message.topic,
                    "kafka_partition": message.partition,
                    "kafka_offset": message.offset,
                    "kafka_timestamp": (
                        timestamp(message.timestamp.replace(tzinfo=UTC))
                        if message.timestamp
                        else None
                    ),
                    "microbatch_id": batch_id,
                }
            )
            datasets[event["dataset"]].append(row)
        except (ValueError, TypeError, KeyError, ValidationError) as exc:
            failures.append((message, str(exc)))
    if any(datasets.values()):
        persist_observations(frame.sparkSession, datasets, settings)
    if failures:
        reject_schema = split_social_events(frame.limit(0))[1].schema
        rejected_rows = [
            (
                (
                    None
                    if "credential" in reason or "policy prohibits" in reason
                    else bytes(m.value).decode("utf-8", errors="replace")
                ),
                (
                    None
                    if "credential" in reason or "policy prohibits" in reason
                    else bytes(m.value)
                ),
                bytes(m.key).decode("utf-8", errors="replace") if m.key else None,
                m.topic,
                m.partition,
                m.offset,
                m.timestamp,
                reason,
                datetime.now(UTC),
            )
            for m, reason in failures
        ]
        merge_delta(
            frame.sparkSession.createDataFrame(rejected_rows, reject_schema),
            settings.paths.rejected_social,
            keys=("kafka_topic", "kafka_partition", "kafka_offset"),
        )
    frame = frame.filter(event_type.isNull() | (event_type != "bronze.observed"))
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
            valid.filter(~comment_condition(valid)),
            settings.paths.social_posts,
            keys=("event_id",),
            initialize_empty=True,
        )
        merge_delta(
            valid.filter(comment_condition(valid)),
            settings.paths.social_comments,
            keys=("event_id",),
            initialize_empty=True,
        )
    finally:
        frame.unpersist()


def start_social_stream(spark, settings: SparkSettings, *, available_now: bool = False):
    migrate_social_schema(spark, settings)
    migrate_social_comments(spark, settings)
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
