"""Offline event, producer acknowledgement, configuration and Silver replay tests."""

from __future__ import annotations

import copy
import json
import os
import sys
import tempfile
import unittest
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dsp_collector.normalizers.social_post import normalize_social_post
from dsp_collector.publishers import kafka
from dsp_collector.runtime.config import KAFKA_SOCIAL_POST_TOPIC, KafkaSettings


class FakeProducer:
    def __init__(
        self, *, outcomes=(), remaining=0, flush_error=None, enqueue_error=None
    ):
        self.messages = []
        self.outcomes = list(outcomes)
        self.pending = []
        self.remaining = remaining
        self.flush_error = flush_error
        self.enqueue_error = enqueue_error
        self.flush_timeouts = []

    def produce(self, topic, *, key, value, on_delivery):
        if self.enqueue_error:
            raise self.enqueue_error
        index = len(self.messages)
        self.messages.append((topic, key, value))
        error = self.outcomes[index] if index < len(self.outcomes) else None
        self.pending.append((on_delivery, error))

    def flush(self, timeout):
        self.flush_timeouts.append(timeout)
        if self.flush_error:
            raise self.flush_error
        if not self.remaining:
            for callback, error in self.pending:
                callback(error, None)
            self.pending.clear()
        return self.remaining


class KafkaPublisherTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.record = normalize_social_post(
            {"id": "123", "text": "Tìm sân pickleball Hà Nội 🇻🇳"},
            run_id="kafka-run",
            collected_at="2026-10-05T04:00:00+00:00",
        )
        self.batch = {
            "schema_version": "1",
            "source": "threads",
            "run_id": "kafka-run",
            "count": 2,
            "records": [
                self.record,
                {**self.record, "record_id": "threads:456", "source_record_id": "456"},
            ],
        }
        self.fake = FakeProducer()
        self.settings = KafkaSettings()
        native = patch.object(
            kafka,
            "_producer",
            side_effect=AssertionError("native Kafka producer used in unit test"),
        )
        native.start()
        self.addCleanup(native.stop)
        network = patch(
            "socket.socket.connect", side_effect=AssertionError("network attempted")
        )
        network.start()
        self.addCleanup(network.stop)

    def publisher(self, settings=None):
        return kafka.KafkaPublisher(
            settings or self.settings, producer_factory=lambda config: self.fake
        )

    def publish(self, batch=None):
        return kafka.publish_social_batch(
            batch if batch is not None else self.batch,
            settings=self.settings,
            publisher_factory=self.publisher,
        )

    def test_event_validates_against_event_and_canonical_silver_contract(self):
        event = kafka.build_social_post_event(self.record)
        kafka.validate_social_post_event(event)
        self.assertEqual(event["payload"], self.record)
        self.assertEqual(event["schema_version"], "1")
        self.assertEqual(event["event_type"], "social_post.normalized")
        self.assertEqual(event["source"], "threads")
        self.assertEqual(event["run_id"], self.record["run_id"])
        self.assertEqual(event["record_id"], self.record["record_id"])
        self.assertNotIn("posts", event)

    def test_event_schema_references_silver_without_duplicating_payload(self):
        path = (
            Path(__file__).resolve().parents[3]
            / "contracts/kafka/social-post-event.v1.schema.json"
        )
        schema = json.loads(path.read_text())
        self.assertEqual(
            schema["$schema"], "https://json-schema.org/draft/2020-12/schema"
        )
        self.assertEqual(
            schema["properties"]["payload"],
            {"$ref": "../data/silver/social-post.v1.schema.json"},
        )

    def test_emitted_at_is_utc_and_independent_from_collection(self):
        when = datetime(2026, 10, 6, 12, tzinfo=timezone(timedelta(hours=7)))
        event = kafka.build_social_post_event(self.record, emitted_at=when)
        self.assertEqual(event["emitted_at"], "2026-10-06T05:00:00.000000Z")
        self.assertEqual(
            datetime.fromisoformat(event["emitted_at"]).utcoffset(), timedelta(0)
        )
        self.assertNotEqual(event["emitted_at"], self.record["collected_at"])
        with self.assertRaises(ValueError):
            kafka.build_social_post_event(
                self.record,
                emitted_at=datetime(2026, 1, 1, tzinfo=UTC).replace(tzinfo=None),
            )

    def test_event_timestamp_requires_rfc3339_utc(self):
        event = kafka.build_social_post_event(self.record)
        for value in (
            "2026-10-05 10:00:00Z",
            "2026-10-05T10:00Z",
            "2026-10-05T10:00:00+07:00",
            "2026-10-05T25:00:00Z",
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                kafka.validate_social_post_event({**event, "emitted_at": value})

    def test_event_id_stable_across_replay_with_different_emission_time(self):
        first = kafka.build_social_post_event(
            self.record, emitted_at=datetime(2026, 1, 1, tzinfo=UTC)
        )
        second = kafka.build_social_post_event(
            self.record, emitted_at=datetime(2026, 1, 2, tzinfo=UTC)
        )
        self.assertEqual(first["event_id"], second["event_id"])
        self.assertNotEqual(first["emitted_at"], second["emitted_at"])
        different = kafka.build_social_post_event({**self.record, "run_id": "another"})
        self.assertNotEqual(first["event_id"], different["event_id"])

    def test_identity_framing_avoids_delimiter_collision(self):
        self.assertNotEqual(kafka._event_id("a:b", "c"), kafka._event_id("a", "b:c"))

    def test_event_builder_does_not_mutate_or_alias_silver(self):
        before = copy.deepcopy(self.record)
        event = kafka.build_social_post_event(self.record)
        event["payload"]["categories"].append("changed")
        self.assertEqual(self.record, before)

    def test_record_key_topic_and_unicode_utf8(self):
        settings = KafkaSettings(social_post_topic="custom.social.v1")
        publisher = self.publisher(settings)
        event = kafka.build_social_post_event(self.record)
        receipt = publisher.publish_social_post(event)
        self.assertEqual(receipt.status, "pending")
        publisher.close()
        topic, key, value = self.fake.messages[0]
        self.assertEqual(topic, settings.social_post_topic)
        self.assertEqual(key, self.record["record_id"].encode("utf-8"))
        self.assertIn(self.record["text"].encode("utf-8"), value)
        self.assertEqual(json.loads(value), event)
        self.assertEqual(receipt.status, "delivered")

    def test_producer_reliability_configuration(self):
        factory = Mock(return_value=self.fake)
        settings = KafkaSettings(
            bootstrap_servers="broker:29092",
            client_id="test",
            delivery_timeout_ms=2000,
            request_timeout_ms=1000,
            retries=3,
        )
        publisher = kafka.KafkaPublisher(settings, producer_factory=factory)
        config = factory.call_args.args[0]
        self.assertEqual(config["acks"], "all")
        self.assertTrue(config["enable.idempotence"])
        self.assertEqual(config["bootstrap.servers"], settings.bootstrap_servers)
        self.assertEqual(config["client.id"], "test")
        self.assertEqual(config["retries"], 3)
        self.assertEqual(config["delivery.timeout.ms"], 2000)
        self.assertEqual(config["request.timeout.ms"], 1000)
        self.assertLessEqual(config["max.in.flight.requests.per.connection"], 5)
        publisher.close()
        self.assertEqual(self.fake.flush_timeouts, [2.0])

    def test_delivery_error_surfaces_from_flush(self):
        self.fake = FakeProducer(outcomes=["broker rejected message"])
        publisher = self.publisher()
        receipt = publisher.publish_social_post(
            kafka.build_social_post_event(self.record)
        )
        with self.assertRaises(kafka.PublicationError):
            publisher.flush()
        self.assertEqual(receipt.status, "failed")
        self.assertEqual(receipt.error, "broker rejected message")
        with self.assertRaises(kafka.PublicationError):
            publisher.close()
        publisher.close()

    def test_flush_timeout_marks_delivery_unconfirmed(self):
        self.fake = FakeProducer(remaining=1)
        publisher = self.publisher()
        receipt = publisher.publish_social_post(
            kafka.build_social_post_event(self.record)
        )
        with self.assertRaises(kafka.PublicationError):
            publisher.close()
        self.assertEqual(receipt.status, "failed")
        self.assertIn("acknowledgement", receipt.error)

    def test_native_flush_exception_surfaces(self):
        self.fake = FakeProducer(flush_error=RuntimeError("flush failed"))
        publisher = self.publisher()
        receipt = publisher.publish_social_post(
            kafka.build_social_post_event(self.record)
        )
        with self.assertRaisesRegex(kafka.PublicationError, "flush failed"):
            publisher.close()
        self.assertIn("unconfirmed", receipt.error)

    def test_missing_callback_cannot_be_reported_as_success(self):
        publisher = self.publisher()
        publisher.publish_social_post(kafka.build_social_post_event(self.record))
        self.fake.flush = Mock(return_value=0)
        with self.assertRaisesRegex(kafka.PublicationError, "unconfirmed"):
            publisher.close()

    def test_close_is_idempotent_and_rejects_subsequent_publish(self):
        publisher = self.publisher()
        publisher.close()
        publisher.close()
        self.assertEqual(len(self.fake.flush_timeouts), 1)
        with self.assertRaisesRegex(kafka.PublicationError, "closed"):
            publisher.publish_social_post(kafka.build_social_post_event(self.record))

    def test_enqueue_failure_is_explicit(self):
        self.fake = FakeProducer(enqueue_error=BufferError("queue full"))
        publisher = self.publisher()
        with self.assertRaisesRegex(kafka.PublicationError, "queue full"):
            publisher.publish_social_post(kafka.build_social_post_event(self.record))
        publisher.close()

    def test_producer_creation_failure_is_explicit(self):
        with self.assertRaisesRegex(kafka.PublicationError, "initialization failed"):
            kafka.KafkaPublisher(
                self.settings,
                producer_factory=Mock(side_effect=RuntimeError("bad config")),
            )

    def test_invalid_silver_record_cannot_enqueue(self):
        publisher = self.publisher()
        for field, value in (
            ("text", 123),
            ("schema_version", "2"),
            ("collected_at", "2026-13-01T00:00:00.000000Z"),
        ):
            with self.subTest(field=field), self.assertRaises(ValueError):
                kafka.build_social_post_event({**self.record, field: value})
        event = kafka.build_social_post_event(self.record)
        event["payload"]["raw_payload"] = {"source": "raw"}
        with self.assertRaises(ValueError):
            publisher.publish_social_post(event)
        self.assertFalse(self.fake.messages)
        publisher.close()

    def test_pbplus_is_not_accepted(self):
        with self.assertRaisesRegex(ValueError, "Unsupported SocialPost source"):
            kafka.build_social_post_event({**self.record, "source": "pbplus"})
        with self.assertRaises(ValueError):
            self.publish({**self.batch, "source": "pbplus"})

    def test_event_lineage_and_identity_must_match_payload(self):
        for field, value in (
            ("run_id", "other"),
            ("record_id", "threads:other"),
            ("event_id", "sha256:" + "0" * 64),
            ("source", "pbplus"),
            ("event_type", "raw"),
        ):
            event = kafka.build_social_post_event(self.record)
            event[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                kafka.validate_social_post_event(event)

    def test_saved_silver_batch_replays_without_mutation_or_recrawl(self):
        path = self.root / "silver.json"
        path.write_text(json.dumps(self.batch, ensure_ascii=False), encoding="utf-8")
        before = path.read_bytes()
        result = self.publish(path)
        first_ids = [json.loads(m[2])["event_id"] for m in self.fake.messages]
        self.fake = FakeProducer()
        second = self.publish(path)
        self.assertEqual(
            first_ids, [json.loads(m[2])["event_id"] for m in self.fake.messages]
        )
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual((result.status, second.status), ("success", "success"))
        self.assertEqual(result.published_count, 2)

    def test_empty_batch_succeeds_without_producer(self):
        factory = Mock(side_effect=AssertionError("empty batch created a producer"))
        result = kafka.publish_social_batch(
            {**self.batch, "count": 0, "records": []},
            settings=self.settings,
            publisher_factory=factory,
        )
        factory.assert_not_called()
        self.assertEqual(
            (result.status, result.published_count, result.failed_count),
            ("success", 0, 0),
        )

    def test_partial_publication_has_acknowledged_and_failed_counts(self):
        self.fake = FakeProducer(outcomes=[None, "broker error"])
        result = self.publish()
        self.assertEqual(
            (result.status, result.published_count, result.failed_count),
            ("partial", 1, 1),
        )
        self.assertEqual(result.errors[0]["record_index"], 1)
        self.assertIn("broker error", result.errors[0]["error"])
        self.assertIsNotNone(result.flush_error)

    def test_all_delivery_failures_fail_batch(self):
        self.fake = FakeProducer(outcomes=["one", "two"])
        result = self.publish()
        self.assertEqual(
            (result.status, result.published_count, result.failed_count),
            ("failed", 0, 2),
        )

    def test_invalid_record_is_reported_and_valid_record_can_publish(self):
        self.batch["records"][0]["text"] = 123
        result = self.publish()
        self.assertEqual(
            (result.status, result.published_count, result.failed_count),
            ("partial", 1, 1),
        )
        self.assertEqual(len(self.fake.messages), 1)
        self.assertIn("contract violation", result.errors[0]["error"])

    def test_record_batch_lineage_conflict_is_not_published(self):
        self.batch["records"][0]["run_id"] = "unrelated"
        result = self.publish()
        self.assertEqual(result.status, "partial")
        self.assertEqual(len(self.fake.messages), 1)

    def test_all_invalid_records_fail_without_producer(self):
        self.batch["records"] = [{}, {}]
        result = self.publish()
        self.assertEqual((result.status, result.failed_count), ("failed", 2))
        self.assertFalse(self.fake.messages)

    def test_bad_batch_count_and_bronze_are_rejected(self):
        with self.assertRaises(ValueError):
            self.publish({**self.batch, "count": 3})
        with self.assertRaises(ValueError):
            self.publish(
                {
                    "source": "threads",
                    "schema_version": "1",
                    "run_id": "raw",
                    "posts": [],
                }
            )

    def test_defaults_are_disabled_and_topic_is_canonical(self):
        with patch.dict(os.environ, {}, clear=True):
            settings = KafkaSettings.from_env()
        self.assertFalse(settings.enabled)
        self.assertEqual(settings.social_post_topic, KAFKA_SOCIAL_POST_TOPIC)

    def test_env_configuration_is_used_and_validated(self):
        with patch.dict(
            os.environ,
            {
                "KAFKA_ENABLED": "true",
                "KAFKA_BOOTSTRAP_SERVERS": "broker:29092",
                "KAFKA_SOCIAL_POST_TOPIC": "another.v1",
                "KAFKA_RETRIES": "4",
            },
            clear=True,
        ):
            settings = KafkaSettings.from_env()
        self.assertTrue(settings.enabled)
        self.assertEqual(settings.bootstrap_servers, "broker:29092")
        self.assertEqual(settings.social_post_topic, "another.v1")
        self.assertEqual(settings.retries, 4)
        for options in (
            {"retries": 0},
            {"social_post_topic": "bad topic"},
            {"request_timeout_ms": 0},
            {"delivery_timeout_ms": 10},
            {"bootstrap_servers": ""},
        ):
            with self.subTest(options=options), self.assertRaises(ValueError):
                KafkaSettings(**options)
        with (
            patch.dict(os.environ, {"KAFKA_ENABLED": "invalid"}),
            self.assertRaises(ValueError),
        ):
            KafkaSettings.from_env()


if __name__ == "__main__":
    unittest.main()
