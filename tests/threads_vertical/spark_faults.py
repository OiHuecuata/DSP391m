"""Actual Kafka/Spark recovery checks with a separate declared fixture lakehouse."""

import copy
import json
from pathlib import Path

from dsp_collector.normalizers.observation import canonical_json
from dsp_collector.publishers.observation import TOPIC, publish
from dsp_collector.runtime.durable import ArtifactStore
from dsp_spark.io.delta import read_delta
from dsp_spark.jobs.batch.ingest_threads_observations import ingest, resolve_all
from dsp_spark.jobs.streaming.threads_observations_to_delta import consume_batch, drain
from dsp_spark.processing.threads.contracts import path
from dsp_spark.processing.threads.silver import build as silver
from dsp_spark.runtime.config import LakehousePaths, SparkSettings
from dsp_spark.runtime.spark import create_spark

from tests.threads_vertical.fixtures import POLICY, post
from tests.threads_vertical.test_features import envelope


def run(directory):
    from confluent_kafka import Producer
    from pyspark.sql import functions as F

    directory = Path(directory)
    store = ArtifactStore((directory / "raw").as_uri())
    settings = SparkSettings(
        paths=LakehousePaths((directory / "lakehouse").as_uri()),
        kafka_bootstrap_servers="127.0.0.1:19092",
    )
    spark = create_spark(settings, kafka=True)
    checks = []
    try:
        events = []
        for revision in (0, 1):
            value = envelope(
                [post(1, revision=revision), post(5, revision=revision)],
                run=f"fixture-fault-{revision}",
                observed=f"2026-10-10T0{revision}:00:00.000000Z",
            )
            reference = store.put(
                f"revision-{revision}.json", canonical_json(value).encode()
            )
            events.extend(
                ingest(
                    spark,
                    store=store,
                    reference=reference,
                    settings=settings,
                    policy=POLICY,
                    fixture=True,
                )
            )
        assert len(events) == 8
        missing = copy.deepcopy(events[0])
        missing["observation_id"] = missing["event_id"] = "sha256:" + "0" * 64
        try:
            resolve_all(
                spark,
                [missing],
                settings=settings,
                store=store,
                policy=POLICY,
                fixture=True,
            )
        except ValueError:
            checks.append("Missing Bronze evidence rejected")
        else:
            raise AssertionError("Missing native evidence was accepted")

        class LostAcknowledgements:
            def __init__(self, config):
                self.producer = Producer(config)

            def produce(self, topic, **kwargs):
                kwargs.pop("on_delivery")
                self.producer.produce(topic, **kwargs)

            def poll(self, timeout):
                return self.producer.poll(timeout)

            def flush(self, timeout):
                return self.producer.flush(timeout)

        try:
            publish(
                events,
                bootstrap=settings.kafka_bootstrap_servers,
                producer_factory=LostAcknowledgements,
            )
        except RuntimeError:
            checks.append(
                "Real Kafka deliveries with lost client acknowledgement require retry"
            )
        else:
            raise AssertionError("Unknown acknowledgement marked success")
        publish(list(reversed(events)), bootstrap=settings.kafka_bootstrap_servers)
        publish(events[:2], bootstrap=settings.kafka_bootstrap_servers)
        # Replay the exact same unique observations separately; transport duplication
        # and ordering cannot add source identities or cumulative metric snapshots.
        publish(events, bootstrap=settings.kafka_bootstrap_servers)
        source = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "earliest")
            .load()
        )
        failed_once = directory / "failed-once.json"

        def fail_after_commit(batch, _id):
            consume_batch(
                spark,
                batch,
                settings=settings,
                store=store,
                policy=POLICY,
                fixture=True,
            )
            failed_once.write_text(
                '{"fixture_fault":"after Delta commit before offset acknowledgement"}'
            )
            raise RuntimeError("Injected fixture crash after Delta commit")

        query = (
            source.writeStream.foreachBatch(fail_after_commit)
            .option(
                "checkpointLocation",
                settings.paths.base + "/checkpoints/threads_observations_v2",
            )
            .trigger(availableNow=True)
            .start()
        )
        try:
            query.awaitTermination(180)
        except Exception:
            pass
        assert failed_once.is_file(), "Fault injection did not run"
        drain(spark, settings=settings, store=store, policy=POLICY, fixture=True)
        admitted = read_delta(spark, path(settings, "admitted_threads_observations"))
        assert admitted.count() == 8
        drain(spark, settings=settings, store=store, policy=POLICY, fixture=True)
        assert (
            read_delta(spark, path(settings, "admitted_threads_observations")).count()
            == 8
        )
        checks.extend(
            [
                "Out-of-order and duplicate Kafka delivery deduplicated",
                "Consumer restarted after committed Delta / uncommitted offsets",
                "Repeated checkpoint drain is idempotent",
            ]
        )
        counts = silver(spark, settings)
        assert (
            counts["silver_social_posts"] == 2
            and counts["silver_social_comments"] == 2
            and counts["silver_social_engagement_snapshots"] == 8
        )
        snapshots = read_delta(
            spark, path(settings, "silver_social_engagement_snapshots")
        )
        native_key = next(
            e["observation_id"]
            for e in events
            if e["source_record_id"] == "1"
            and e["collection_run_id"] == "fixture-fault-1"
        )
        selected = (
            snapshots.filter(F.col("observation_id") == native_key).first().asDict()
        )
        assert (
            selected["likes"] == 0
            and selected["likes_delta"] == -1
            and selected["is_latest_eligible"]
        )
        assert snapshots.filter("is_latest_eligible").count() == 4
        checks.extend(
            [
                "Signed native metric decrease preserved",
                "Latest eligible selection uses one snapshot per content",
                "Posts and nested replies remain independent",
            ]
        )
        result = {
            "fixture": True,
            "verification_level": "LOCAL_INTEGRATION_VERIFIED",
            "native_observations": 8,
            "unique_posts": 2,
            "replies": 2,
            "checks": checks,
            "silver_counts": counts,
        }
        (directory / "fault-verification.json").write_text(canonical_json(result))
        print(json.dumps(result, indent=2), flush=True)
        return result
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys

    run(Path(sys.argv[1]))
