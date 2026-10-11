"""Kafka transport configuration only; jobs validate the event contract."""

from pyspark.sql import SparkSession

from ..runtime.config import SparkSettings


def read_social_stream(spark: SparkSession, settings: SparkSettings):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.social_post_topic)
        .option("startingOffsets", settings.starting_offsets)
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
        .option("failOnDataLoss", "true")
        .option("kafka.allow.auto.create.topics", "false")
        .load()
    )
