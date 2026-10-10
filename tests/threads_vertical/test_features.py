import copy
import unittest
from datetime import UTC, datetime, timedelta

from dsp_collector.normalizers.observation import canonical_json
from dsp_collector.normalizers.observation_v2 import project, validate
from dsp_collector.runtime.durable import sha256
from dsp_spark.processing.threads.features import (
    content,
    geo_mentions,
    intent_evidence,
    snapshot,
)

from tests.threads_vertical.fixtures import POLICY, post


def envelope(posts, *, run="fixture-features", observed="2026-10-10T01:00:00.000000Z"):
    from dsp_collector.runtime.source_policy import policy_hash

    return {
        "source": "threads",
        "run_id": run,
        "count": len(posts),
        "posts": posts,
        "collected_at": observed,
        "source_policy_sha256": policy_hash(POLICY),
        "native_observation_context": {
            "origin": "SYNTHETIC",
            "permission_reference": POLICY["reference"],
            "retention_policy_id": "isolated-fixture",
            "retention_expires_at": (datetime.now(UTC) + timedelta(days=7)).isoformat(),
            "provenance_scope": "FETCH_BATCH",
            "fetch_batch_id": "fixture-fetch",
            "query_id": "fixture-query",
            "observed_at": observed,
        },
    }


def native(value):
    return project(
        value,
        artifact_uri="file:///tmp/declared-fixture.json",
        artifact_sha256=sha256(canonical_json(value).encode()),
    )


class FeatureTests(unittest.TestCase):
    def test_vietnam_publication_cohort_and_native_update(self):
        value = native(envelope([post(5, revision=1)]))[0]
        self.assertEqual(content(value)["published_date_vn"], "2026-10-10")
        self.assertEqual(content(value)["publication_hour_vn"], 6)
        self.assertEqual(value["updated_at"], "2026-10-09T23:31:40.000000Z")
        self.assertAlmostEqual(snapshot(value)["content_age_hours"], 1.5)

    def test_reply_has_own_location_topic_and_metrics(self):
        rows = native(envelope([post(1)]))
        direct = next(row for row in rows if row["source_record_id"] == "10001")
        nested = next(row for row in rows if row["source_record_id"] == "10002")
        self.assertEqual(geo_mentions(content(direct)), [])
        self.assertEqual(content(direct)["intent_labels"], ["IRRELEVANT"])
        mentions = geo_mentions(content(nested))
        self.assertEqual([row["location_text_raw"] for row in mentions], ["Huế"])
        self.assertTrue(all(row["admin_unit_id"] is None for row in mentions))
        self.assertEqual(nested["like_count"], 0)
        self.assertIsNone(nested["view_count"])

    def test_query_location_is_not_a_content_mention(self):
        value = envelope([post(8)])
        value["query_geo_name"] = "Hà Nội"
        value["posts"][0]["detected_locations"] = ["Hà Nội"]
        self.assertEqual(geo_mentions(content(native(value)[0])), [])

    def test_unevaluated_intent_has_no_calibrated_confidence(self):
        value = content(native(envelope([post(5)]))[0])
        evidence = intent_evidence(value)
        self.assertIn("SEEK_COURT", [row["intent_type"] for row in evidence])
        self.assertTrue(
            all(
                row["confidence"] is None
                and row["classification_status"] == "EXPLORATORY_UNEVALUATED"
                for row in evidence
            )
        )
        self.assertIsNone(value["sentiment"])

    def test_native_language_does_not_use_derived_hint(self):
        value = post(8)
        value["raw_payload"].pop("language")
        value["relevance"] = {"language_hint": "vi"}
        self.assertIsNone(native(envelope([value]))[0]["language_raw"])

    def test_lineage_identity_and_metric_tampering_are_rejected(self):
        row = native(envelope([post(8)]))[0]
        for key, new_value in (
            ("observation_id", "sha256:" + "0" * 64),
            ("like_count", 999),
            ("payload_hash", "0" * 64),
        ):
            changed = copy.deepcopy(row)
            changed[key] = new_value
            with self.assertRaises(ValueError):
                validate(changed)

    def test_fetch_identity_changes_observation_not_content_key(self):
        first = envelope([post(8)])
        second = copy.deepcopy(first)
        second["native_observation_context"]["fetch_batch_id"] = "second-fetch"
        a, b = native(first)[0], native(second)[0]
        self.assertEqual(a["native_record_key"], b["native_record_key"])
        self.assertNotEqual(a["observation_id"], b["observation_id"])

    def test_unknown_counter_prevents_unconfirmed_engagement_sum(self):
        row = native(envelope([post(7)]))[0]
        value = snapshot(row)
        self.assertIsNone(value["likes"])
        self.assertIsNone(value["engagement_total"])
        self.assertIsNone(value["engagement_rate"])


if __name__ == "__main__":
    unittest.main()
