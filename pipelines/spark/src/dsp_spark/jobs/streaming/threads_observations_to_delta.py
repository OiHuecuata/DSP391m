"""Kafka v2 references admitted against pinned native Delta versions and raw bytes."""

from dsp_collector.normalizers.observation import parse_json
from dsp_collector.runtime.durable import sha256

from ...io.delta import merge_delta
from ...processing.clean.contracts import spark_schema
from ...processing.threads.contracts import path
from ..batch.ingest_threads_observations import _insert_exact, resolve_all


def consume_batch(spark, batch, *, settings, store, policy, fixture=False):
    from pyspark.sql import types as T

    accepted, rejected, events = [], [], []
    for transport in batch.toLocalIterator():
        value = transport.asDict()
        raw = (
            bytes(value["value"])
            if not isinstance(value["value"], str)
            else value["value"].encode()
        )
        try:
            event = parse_json(raw)
            key = (
                bytes(value["key"]).decode()
                if not isinstance(value["key"], str)
                else value["key"]
            )
            if key != event["observation_id"]:
                raise ValueError("Kafka key lineage mismatch")
            if (
                event["record_path"] != settings.paths.record_observations
                or event["artifact_path"] != settings.paths.observation_artifacts
            ):
                raise ValueError("Kafka reference targets another configured lakehouse")
            events.append(event)
        except (ValueError, KeyError, TypeError, PermissionError) as error:
            rejected.append(
                {
                    "topic": value["topic"],
                    "partition": value["partition"],
                    "offset": value["offset"],
                    "value_sha256": sha256(raw),
                    "reason": type(error).__name__,
                }
            )
        # Bounded batches; Spark's offset checkpoint advances only after all writes.
        if len(events) >= 250:
            accepted = resolve_all(
                spark,
                events,
                settings=settings,
                store=store,
                policy=policy,
                fixture=fixture,
            )
            _insert_exact(
                spark,
                accepted,
                path(settings, "admitted_threads_observations"),
                spark_schema("observation_v2"),
                "observation_id",
            )
            events = []
    accepted = resolve_all(
        spark, events, settings=settings, store=store, policy=policy, fixture=fixture
    )
    _insert_exact(
        spark,
        accepted,
        path(settings, "admitted_threads_observations"),
        spark_schema("observation_v2"),
        "observation_id",
    )
    schema = T.StructType(
        [
            T.StructField("topic", T.StringType()),
            T.StructField("partition", T.LongType()),
            T.StructField("offset", T.LongType()),
            T.StructField("value_sha256", T.StringType()),
            T.StructField("reason", T.StringType()),
        ]
    )
    merge_delta(
        spark.createDataFrame(rejected, schema),
        path(settings, "rejected_threads_events"),
        keys=("topic", "partition", "offset"),
        initialize_empty=True,
    )


def drain(spark, *, settings, store, policy, fixture=False):
    from dsp_collector.publishers.observation import TOPIC

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 250)
        .option("failOnDataLoss", "true")
        .load()
    )
    query = (
        source.writeStream.foreachBatch(
            lambda batch, _batch_id: consume_batch(
                spark,
                batch,
                settings=settings,
                store=store,
                policy=policy,
                fixture=fixture,
            )
        )
        .option(
            "checkpointLocation",
            settings.paths.base + "/checkpoints/threads_observations_v2",
        )
        .trigger(availableNow=True)
        .start()
    )
    if not query.awaitTermination(300):
        query.stop()
        raise TimeoutError("Threads Kafka drain did not complete")
    if query.exception():
        raise RuntimeError("Threads Kafka processing failed")
    return query.recentProgress
