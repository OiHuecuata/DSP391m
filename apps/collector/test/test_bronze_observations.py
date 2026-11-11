"""Deterministic source-record contracts, lifecycle evidence and Kafka replay."""

import copy
import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock, patch

from dsp_collector.publishers.kafka import (
    DeliveryReceipt,
    publish_bronze_batch,
    validate_bronze_event,
)
from dsp_collector.runtime.bronze import (
    extract_observations,
    load_policies,
    validate_observation,
)

ROOT = Path(__file__).resolve().parents[3]
AT = "2026-10-10T00:00:00Z"


def envelope(source="threads", run="run1"):
    return {
        "schema_version": "1",
        "source": source,
        "run_id": run,
        "collected_at": AT,
        "count": 1,
        "posts": [{"id": "123", "text": "pickleball", "like_count": 0}],
        "query_runs": [],
    }


def youtube():
    return {
        "schema_version": "1",
        "source": "youtube",
        "run_id": "yt",
        "collected_at": AT,
        "query_runs": [],
        "responses": [],
        "videos": [
            {
                "id": "v1",
                "snippet": {"title": "pickleball", "publishedAt": AT},
                "statistics": {"viewCount": "0"},
            }
        ],
        "comments": [
            {
                "video_id": "v1",
                "payload": {
                    "id": "c1",
                    "snippet": {"textOriginal": "comment", "likeCount": 0},
                },
            },
            {
                "video_id": "v1",
                "payload": {
                    "id": "c2",
                    "snippet": {"parentId": "c1", "textOriginal": "reply"},
                },
            },
        ],
        "video_queries": {"v1": ["pickleball"]},
        "requests": {},
        "errors": [],
    }


class BronzeObservationTests(unittest.TestCase):
    def test_pbplus_record_count_and_unknown_versus_zero(self):
        raw = json.loads((ROOT / "tests/fixtures/pipeline/pbplus.json").read_text())
        raw["posts"].append({"id": "venue-six", "number_of_courts": 6})
        rows, errors = extract_observations(raw)
        self.assertFalse(errors)
        venues = rows["bronze_court_observations"]
        self.assertEqual([r["court_count_raw"] for r in venues], [None, "0", "6"])
        self.assertEqual(len(rows["bronze_collection_runs"]), 1)
        self.assertEqual(len(venues), 3)

    def test_negative_court_count_is_rejected_without_defaulting_unknown_to_one(self):
        raw = {
            **envelope("pbplus"),
            "posts": [
                {"id": "venue-bad", "number_of_courts": -1},
                {"id": "venue-unknown", "number_of_courts": None},
            ],
        }
        rows, errors = extract_observations(raw)
        self.assertEqual(len(errors), 1)
        venue = rows["bronze_court_observations"][0]
        self.assertIsNone(venue["court_count_raw"])
        self.assertIsNone(venue["source_url"])
        self.assertEqual(
            rows["bronze_collection_runs"][0]["execution_status"], "partial"
        )

    def test_threads_independent_post_and_null_metrics(self):
        rows, errors = extract_observations(envelope())
        self.assertFalse(errors)
        self.assertEqual(len(rows["bronze_social_posts"]), 1)
        metric = rows["bronze_engagement_snapshots"][0]
        self.assertEqual(metric["like_count"], 0)
        self.assertIsNone(metric["view_count"])
        self.assertEqual(json.loads(metric["metric_availability"])["like_count"], True)
        self.assertEqual(json.loads(metric["metric_availability"])["view_count"], False)

    def test_video_comments_and_nested_replies_are_separate(self):
        rows, errors = extract_observations(youtube())
        self.assertFalse(errors)
        self.assertEqual(len(rows["bronze_social_posts"]), 1)
        comments = rows["bronze_social_comments"]
        self.assertEqual(len(comments), 2)
        self.assertEqual([r["native_post_id"] for r in comments], ["v1", "v1"])
        self.assertEqual(comments[1]["parent_comment_id"], "c1")
        self.assertTrue(comments[1]["is_reply"])
        self.assertEqual(len(rows["bronze_engagement_snapshots"]), 3)

    def test_threads_visible_replies_require_actual_payload(self):
        raw = envelope()
        raw["posts"][0]["detail"] = {
            "visible_replies": [
                {
                    "id": "456",
                    "text": "hi",
                    "reply_to_post_id": "123",
                    "is_reply": True,
                }
            ]
        }
        rows, errors = extract_observations(raw)
        self.assertFalse(errors)
        self.assertEqual(rows["bronze_social_comments"][0]["native_post_id"], "123")

    def test_stable_replay_and_legitimate_multiple_observations(self):
        first, _ = extract_observations(envelope())
        replay, _ = extract_observations(envelope())
        second, _ = extract_observations(envelope(run="run2"))
        self.assertEqual(first, replay)
        self.assertNotEqual(
            first["bronze_social_posts"][0]["observation_id"],
            second["bronze_social_posts"][0]["observation_id"],
        )

    def test_payload_hash_excludes_queries_and_derived_enrichment(self):
        a = envelope()
        b = copy.deepcopy(a)
        b["posts"][0].update(
            matched_queries=["new query"],
            detected_locations=["Hanoi"],
            categories=["business"],
        )
        ar, _ = extract_observations(a)
        br, _ = extract_observations(b)
        self.assertEqual(
            ar["bronze_social_posts"][0]["payload_hash"],
            br["bronze_social_posts"][0]["payload_hash"],
        )

    def test_partial_failure_preserves_good_records_and_run_errors(self):
        a = envelope()
        a["posts"].append({"text": "missing id"})
        a["posts"][0]["detail_status"] = "error"
        rows, errors = extract_observations(a)
        self.assertEqual(len(errors), 1)
        self.assertEqual(len(rows["bronze_social_posts"]), 1)
        self.assertEqual(
            rows["bronze_collection_runs"][0]["execution_status"], "partial"
        )
        self.assertTrue(json.loads(rows["bronze_collection_runs"][0]["errors_json"]))

    def test_explicit_deletion_requires_source_evidence(self):
        a = envelope()
        a["source_changes"] = [
            {
                "source_record_id": "123",
                "record_type": "Post",
                "change_type": "deleted",
                "evidence": {"verified_endpoint": "source", "http_status": 410},
            }
        ]
        rows, errors = extract_observations(a)
        self.assertFalse(errors)
        self.assertEqual(rows["bronze_source_changes"][0]["change_type"], "deleted")
        del a["source_changes"][0]["evidence"]
        rows, errors = extract_observations(a)
        self.assertTrue(errors)
        self.assertFalse(rows["bronze_source_changes"])

    def test_empty_search_and_failed_run_do_not_imply_deletion(self):
        a = envelope()
        a.update(posts=[], count=0, errors=[{"reason": "failed"}])
        rows, _ = extract_observations(a)
        self.assertFalse(rows["bronze_source_changes"])
        self.assertEqual(
            rows["bronze_collection_runs"][0]["execution_status"], "failed"
        )

    def test_invalid_timestamp_identity_and_metrics_are_observable(self):
        a = envelope()
        a["posts"] = [
            {"id": "a", "taken_at": "naive-time"},
            {"id": "b", "like_count": -1},
            {"id": " ", "text": "x"},
        ]
        rows, errors = extract_observations(a)
        self.assertEqual(len(errors), 3)
        self.assertFalse(rows["bronze_social_posts"])

    def test_conflicting_duplicate_source_payload_rejected(self):
        a = envelope()
        a["posts"].append({"id": "123", "text": "changed"})
        rows, errors = extract_observations(a)
        self.assertEqual(len(rows["bronze_social_posts"]), 1)
        self.assertIn("conflicting duplicate", errors[0]["error"])

    def test_secrets_are_rejected_before_raw_persistence(self):
        for key in ("access_token", "accessToken", "X-Goog-Api-Key", "Cookie"):
            with self.subTest(key=key):
                raw = envelope()
                raw["posts"][0]["nested"] = {key: "secret"}
                with self.assertRaisesRegex(ValueError, "credential"):
                    extract_observations(raw)

    def test_policy_denial_and_source_specific_expiration(self):
        with self.assertRaisesRegex(ValueError, "prohibits"):
            extract_observations(
                envelope(), policies={"threads": {"storage_permission": False}}
            )

        # This fixture tests the computed deadline while it is still eligible.
        # Freeze its clock so the assertion remains valid after 17 October.
        class FixtureClock(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 10, 11, tzinfo=UTC).astimezone(tz)

        with patch("dsp_collector.runtime.bronze.datetime", FixtureClock):
            rows, _ = extract_observations(
                envelope(),
                policies={
                    "threads": {
                        "retention_policy_id": "reviewed-policy",
                        "retention_days": 7,
                    }
                },
            )
        self.assertEqual(
            rows["bronze_social_posts"][0]["retention_expires_at"],
            "2026-10-17T00:00:00.000000Z",
        )
        rows, _ = extract_observations(envelope())
        self.assertIsNone(rows["bronze_social_posts"][0]["retention_expires_at"])

    def test_policy_registry_rejects_unsupported_field_permissions(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "policy.json"
            p.write_text(
                json.dumps(
                    {
                        "threads": {
                            "retention_policy_id": "p",
                            "storage_permission": True,
                            "allowed_fields": ["text"],
                        }
                    }
                )
            )
            with self.assertRaisesRegex(ValueError, "reviewed adapter"):
                load_policies(p)

    def test_future_osm_venue_contract_accepts_source_namespace(self):
        rows, _ = extract_observations(
            {**envelope("pbplus"), "posts": [{"id": "node/123", "court_name": "name"}]}
        )
        record = rows["bronze_court_observations"][0]
        from dsp_collector.runtime.bronze import digest

        record["source_platform"] = "osm"
        record["observation_id"] = digest(
            [
                "bronze_court_observations",
                "osm",
                "node/123",
                "run1",
                record["collected_at"],
            ]
        )
        validate_observation("bronze_court_observations", record)

    def test_terminal_status_projects_a_run_without_fabricated_source_records(self):
        from dsp_collector.runtime.bronze import extract_collection_status

        status = {
            "source": "youtube",
            "run_id": "fatal",
            "status": "FAILED",
            "started_at": AT,
            "completed_at": AT,
            "stage": "collecting",
            "error": "API unavailable",
            "metrics": {},
        }
        datasets, errors = extract_collection_status(status)
        self.assertFalse(errors)
        self.assertEqual(sum(len(v) for v in datasets.values()), 1)
        run = datasets["bronze_collection_runs"][0]
        self.assertEqual(run["execution_status"], "failed")
        self.assertIsNone(run["records_returned"])
        publisher = Mock()
        publisher.publish_bronze_observation.side_effect = lambda e: DeliveryReceipt(
            e["event_id"], e["event_id"], "delivered"
        )
        receipt = publish_bronze_batch(status, publisher_factory=lambda _: publisher)
        self.assertEqual(receipt.published_count, 1)

    def test_query_metadata_supports_real_query_run_objects(self):
        raw = envelope()
        raw["query_runs"] = [{"query": "pickleball Hanoi", "status": "error"}]
        rows, errors = extract_observations(raw)
        self.assertFalse(errors)
        run = rows["bronze_collection_runs"][0]
        self.assertIsNotNone(run["query_id"])
        self.assertEqual(run["execution_status"], "partial")

    def test_kafka_payload_cannot_bypass_secret_or_duplicate_json_validation(self):
        from dsp_collector.runtime.bronze import decode, digest, encode

        raw = envelope()
        rows, _ = extract_observations(raw)
        row = rows["bronze_social_posts"][0]
        payload = {"id": "123", "cookie": "secret"}
        row.update(raw_payload=encode(payload), payload_hash=digest(payload))
        with self.assertRaisesRegex(ValueError, "credential"):
            validate_observation("bronze_social_posts", row)
        with self.assertRaisesRegex(ValueError, "duplicate JSON"):
            decode('{"field":1,"field":2}')

    def test_expired_source_policy_requires_refresh(self):
        from dsp_collector.runtime.bronze import apply_source_policy

        raw = envelope()
        raw["collected_at"] = "2000-01-01T00:00:00Z"
        rows, _ = extract_observations(raw)
        with self.assertRaisesRegex(ValueError, "refresh required"):
            apply_source_policy(
                rows["bronze_social_posts"][0],
                {
                    "threads": {
                        "retention_policy_id": "p",
                        "storage_permission": True,
                        "retention_days": 1,
                    }
                },
            )

    def test_mismatched_detail_payload_cannot_replace_source_post(self):
        raw = envelope()
        raw["posts"][0]["detail"] = {
            "raw_payload": {"pk": "999", "caption": {"text": "different"}}
        }
        rows, errors = extract_observations(raw)
        self.assertTrue(errors)
        self.assertEqual(rows["bronze_social_posts"][0]["text_raw"], "pickleball")

    def test_kafka_record_events_preserve_raw_records_and_ack_counts(self):
        publisher = Mock()
        events = []

        def published(event):
            validate_bronze_event(event)
            events.append(event)
            return DeliveryReceipt(event["event_id"], event["event_id"], "delivered")

        publisher.publish_bronze_observation.side_effect = published
        result = publish_bronze_batch(youtube(), publisher_factory=lambda _: publisher)
        self.assertEqual(result.status, "success")
        self.assertEqual(result.published_count, 7)
        self.assertEqual(len({e["event_id"] for e in events}), 7)
        publisher.close.assert_called_once()
