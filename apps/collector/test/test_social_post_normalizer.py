"""Offline contract checks using unittest; no new test dependencies required.

The recursive checks cover the structural constraints used by this schema,
not the complete JSON Schema standard. A future runtime validator should use
a Draft 2020-12 JSON Schema library with format checking enabled.
"""

from __future__ import annotations

import copy
import json
import re
import sys
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

COLLECTOR_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = COLLECTOR_ROOT.parents[1]
sys.path.insert(0, str(COLLECTOR_ROOT / "src"))

from dsp_collector.normalizers.social_post import normalize_social_post

SCHEMA_PATH = REPOSITORY_ROOT / "contracts/data/silver/social-post.v1.schema.json"
SAMPLE_PATH = COLLECTOR_ROOT / ".local/threads/pickleball_sample.json"
COLLECTED_AT = "2026-10-01T23:02:10.383825+07:00"
NORMALIZED_AT = "2026-10-05T12:00:00+07:00"


class SocialPostNormalizerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))

    def setUp(self):
        self.post = {
            "id": "1234567890123456789",
            "code": "SampleCode",
            "url": "https://www.threads.com/@player/post/SampleCode",
            "text": "Tìm sân pickleball Hà Nội",
            "taken_at": 0,
            "like_count": 47,
            "reply_count": 21,
            "repost_count": 0,
            "quote_count": 2,
            "is_reply": True,
            "media_type": 19,
            "has_media": False,
            "user": {
                "pk": "100",
                "username": "player",
                "full_name": "Search Player",
                "is_verified": True,
            },
            "matched_queries": ["pickleball Hà Nội", "pickleball Hanoi"],
            "query_contexts": [
                {
                    "query": "pickleball Hà Nội",
                    "kind": "city",
                    "geo_key": "hanoi",
                    "geo_name": "Hà Nội",
                },
                {
                    "query": "pickleball",
                    "kind": "fallback",
                    "geo_key": None,
                    "geo_name": None,
                },
            ],
            "categories": ["court", "demand"],
            "detected_locations": ["Hà Nội"],
            "relevance": {
                "vietnam_score": 8,
                "topic_score": 5,
                "language_hint": "vi",
                "signals": ["geo:text:Hà Nội", "geo:query:Hà Nội"],
            },
        }

    def normalize(self, post=None, **metadata):
        options = {
            "run_id": "test-run",
            "collected_at": COLLECTED_AT,
            "normalized_at": NORMALIZED_AT,
        }
        options.update(metadata)
        result = normalize_social_post(self.post if post is None else post, **options)
        self.assert_contract(result)
        return result

    def add_detail(self, **changes):
        detail = {
            "id": self.post["id"],
            "code": self.post["code"],
            "url": "https://www.threads.com/@player/post/SampleCode?detail=1",
            "text": "Expanded detail text",
            "taken_at": 1,
            "author": {
                "id": "100",
                "username": "player",
                "full_name": "Detail Player",
                "is_verified": False,
            },
            "engagement": {
                "like_count": 0,
                "reply_count": 30,
                "repost_count": 4,
                "quote_count": 0,
            },
            "is_reply": False,
            "media_type": 2,
            "media": [{"type": "video", "url": "https://example.org/video.mp4"}],
            "raw_payload": {"source_only": "excluded"},
            "visible_replies": [{"text": "also excluded"}],
        }
        detail.update(changes)
        self.post.update(detail=detail, detail_status="ok")
        return detail

    def assert_contract(self, value, node=None):
        """Assert this contract's types, shape, bounds, patterns and formats."""
        node = self.schema if node is None else node
        if "$ref" in node:
            node = self.schema["$defs"][node["$ref"].removeprefix("#/$defs/")]
        kinds = node["type"]
        kinds = kinds if isinstance(kinds, list) else [kinds]
        type_matches = {
            "null": value is None,
            "string": isinstance(value, str),
            "integer": type(value) is int,
            "boolean": type(value) is bool,
            "array": isinstance(value, list),
            "object": isinstance(value, dict),
        }
        self.assertTrue(any(type_matches[kind] for kind in kinds), (value, kinds))
        if "const" in node:
            self.assertEqual(value, node["const"])
        if "enum" in node:
            self.assertIn(value, node["enum"])
        if value is None:
            return
        if isinstance(value, dict):
            self.assertTrue(set(node.get("required", [])).issubset(value))
            if node.get("additionalProperties") is False:
                self.assertTrue(set(value).issubset(node["properties"]))
            for key, item in value.items():
                self.assert_contract(item, node["properties"][key])
        elif isinstance(value, list):
            for item in value:
                self.assert_contract(item, node["items"])
        elif isinstance(value, str):
            self.assertGreaterEqual(len(value), node.get("minLength", 0))
            if "pattern" in node:
                self.assertIsNotNone(re.search(node["pattern"], value))
            if node.get("format") == "date-time":
                self.assertEqual(
                    datetime.fromisoformat(value).utcoffset().total_seconds(), 0
                )
        elif type(value) is int and "minimum" in node:
            self.assertGreaterEqual(value, node["minimum"])

    def test_search_only_record(self):
        result = self.normalize()
        self.assertEqual(result["schema_version"], "1")
        self.assertEqual(result["source"], "threads")
        self.assertEqual(result["text"], self.post["text"])
        self.assertIsNone(result["detail_status"])
        self.assertIsNone(result["detail_error"])

    def test_failed_detail_remains_valid(self):
        self.post.update(
            detail=None, detail_status="error", detail_error="SSR unavailable"
        )
        result = self.normalize()
        self.assertEqual(result["text"], self.post["text"])
        self.assertEqual(result["detail_status"], "error")
        self.assertEqual(result["detail_error"], "SSR unavailable")

    def test_outer_id_is_canonical(self):
        self.add_detail(id="999")
        result = self.normalize()
        self.assertEqual(result["source_record_id"], self.post["id"])
        self.assertEqual(result["record_id"], f"threads:{self.post['id']}")

    def test_integer_id_preserves_large_identity(self):
        self.post["id"] = 1234567890123456789
        self.add_detail(id="1234567890123456789")
        self.assertEqual(self.normalize()["source_record_id"], "1234567890123456789")

    def test_identity_is_deterministic(self):
        self.assertEqual(self.normalize(), self.normalize())

    def test_run_id_does_not_change_identity(self):
        first = self.normalize(run_id="first")
        second = self.normalize(run_id="second", normalized_at="2026-10-06T00:00:00Z")
        self.assertEqual(first["record_id"], second["record_id"])
        self.assertNotEqual(first["run_id"], second["run_id"])

    def test_unix_publication_time(self):
        self.assertEqual(
            self.normalize()["published_at"], "1970-01-01T00:00:00.000000Z"
        )
        self.post["taken_at"] = 1780208417
        self.assertEqual(
            self.normalize()["published_at"], "2026-05-31T06:20:17.000000Z"
        )

    def test_missing_publication_time_stays_null(self):
        self.post.pop("taken_at")
        self.assertIsNone(self.normalize()["published_at"])

    def test_invalid_publication_time_stays_null(self):
        for value in (None, True, "invalid", {}, float("nan"), float("inf"), 10**100):
            with self.subTest(value=value):
                self.post["taken_at"] = value
                self.assertIsNone(self.normalize()["published_at"])

    def test_observation_times_are_separate_and_utc(self):
        result = self.normalize()
        self.assertEqual(result["collected_at"], "2026-10-01T16:02:10.383825Z")
        self.assertEqual(result["normalized_at"], "2026-10-05T05:00:00.000000Z")
        self.assertNotEqual(result["collected_at"], result["published_at"])

    def test_aware_datetime_metadata(self):
        result = self.normalize(collected_at=datetime(2026, 10, 1, tzinfo=UTC))
        self.assertEqual(result["collected_at"], "2026-10-01T00:00:00.000000Z")

    def test_default_normalization_time_is_generated(self):
        before = datetime.now(UTC)
        result = normalize_social_post(
            self.post, run_id="run", collected_at=COLLECTED_AT
        )
        after = datetime.now(UTC)
        self.assert_contract(result)
        self.assertLessEqual(before, datetime.fromisoformat(result["normalized_at"]))
        self.assertLessEqual(datetime.fromisoformat(result["normalized_at"]), after)

    def test_search_author_mapping(self):
        result = self.normalize()
        self.assertEqual(
            [
                result[field]
                for field in (
                    "author_id",
                    "author_username",
                    "author_full_name",
                    "author_is_verified",
                )
            ],
            ["100", "player", "Search Player", True],
        )

    def test_search_engagement_mapping(self):
        result = self.normalize()
        # Search zeros may already represent missing source counters.
        for field in ("like_count", "reply_count", "repost_count", "quote_count"):
            self.assertEqual(result[field], self.post[field])

    def test_successful_detail_author_precedence(self):
        self.add_detail()
        result = self.normalize()
        self.assertEqual(result["author_full_name"], "Detail Player")
        self.assertIs(result["author_is_verified"], False)
        self.assertEqual(result["author_id"], "100")

    def test_successful_detail_engagement_precedence(self):
        detail = self.add_detail()
        result = self.normalize()
        for field, value in detail["engagement"].items():
            self.assertEqual(result[field], value)

    def test_detail_text_precedence(self):
        self.add_detail()
        self.assertEqual(self.normalize()["text"], "Expanded detail text")
        self.post["detail"]["text"] = ""
        self.assertEqual(self.normalize()["text"], "")

    def test_detail_url_time_and_characteristics(self):
        detail = self.add_detail()
        result = self.normalize()
        self.assertEqual(result["source_url"], detail["url"])
        self.assertEqual(result["source_code"], detail["code"])
        self.assertEqual(result["published_at"], "1970-01-01T00:00:01.000000Z")
        self.assertIs(result["is_reply"], False)
        self.assertEqual(result["media_type"], 2)
        self.assertIs(result["has_media"], True)

    def test_null_detail_fields_fall_back_independently(self):
        baseline = self.normalize()
        self.add_detail(
            code=None,
            url=None,
            text=None,
            taken_at=None,
            is_reply=None,
            media_type=None,
            media=None,
            author={
                "id": None,
                "username": None,
                "full_name": None,
                "is_verified": None,
            },
            engagement={
                "like_count": None,
                "reply_count": None,
                "repost_count": None,
                "quote_count": None,
            },
        )
        expected = {**baseline, "detail_status": "ok"}
        self.assertEqual(self.normalize(), expected)

    def test_partial_detail_does_not_hide_search_author_or_counts(self):
        self.add_detail(
            author={"full_name": "Detail Name"}, engagement={"like_count": 0}
        )
        result = self.normalize()
        self.assertEqual(result["author_username"], "player")
        self.assertEqual(result["author_full_name"], "Detail Name")
        self.assertEqual(result["like_count"], 0)
        self.assertEqual(result["reply_count"], 21)

    def test_malformed_detail_values_fall_back(self):
        self.add_detail(
            url="bad URL",
            text={},
            taken_at="invalid",
            is_reply="false",
            media_type=True,
            author={"full_name": []},
            engagement={"like_count": True, "reply_count": -1, "repost_count": "4"},
        )
        result = self.normalize()
        self.assertEqual(result["source_url"], self.post["url"])
        self.assertEqual(result["text"], self.post["text"])
        self.assertEqual(result["published_at"], "1970-01-01T00:00:00.000000Z")
        self.assertEqual(result["author_full_name"], "Search Player")
        self.assertEqual(result["like_count"], 47)
        self.assertEqual(result["reply_count"], 21)
        self.assertEqual(result["repost_count"], 0)
        self.assertIs(result["is_reply"], True)
        self.assertEqual(result["media_type"], 19)

    def test_unsafe_detail_identity_ignores_all_enrichment(self):
        for identity in ("999", None, {}, [], True, 1.5, "bad", ""):
            with self.subTest(identity=identity):
                self.add_detail(id=identity)
                result = self.normalize()
                self.assertEqual(result["text"], self.post["text"])
                self.assertEqual(result["like_count"], 47)
                self.assertEqual(result["source_url"], self.post["url"])
                self.assertEqual(result["source_record_id"], self.post["id"])
                # Retain the crawler's report, not a fabricated normalizer state.
                self.assertEqual(result["detail_status"], "ok")

    def test_mismatched_detail_code_ignores_enrichment(self):
        self.add_detail(code="OtherCode")
        self.assertEqual(self.normalize()["text"], self.post["text"])

    def test_mismatched_detail_username_ignores_enrichment(self):
        self.add_detail(author={"username": "another_player"})
        self.assertEqual(self.normalize()["author_full_name"], "Search Player")

    def test_malformed_detail_object_falls_back(self):
        for detail in (None, [], "invalid", 123):
            with self.subTest(detail=detail):
                self.post.update(detail=detail, detail_status="ok")
                self.assertEqual(self.normalize()["text"], self.post["text"])

    def test_non_ok_detail_is_ignored(self):
        self.add_detail()
        for status in (None, "error"):
            with self.subTest(status=status):
                self.post["detail_status"] = status
                self.assertEqual(self.normalize()["text"], self.post["text"])

    def test_empty_or_malformed_detail_media_preserves_search(self):
        self.post["has_media"] = True
        for media in ([], None, {}, [None], [{"type": "video", "url": "invalid"}]):
            with self.subTest(media=media):
                self.add_detail(media=media)
                self.assertIs(self.normalize()["has_media"], True)

    def test_matched_queries_preserved(self):
        self.assertEqual(
            self.normalize()["matched_queries"], self.post["matched_queries"]
        )

    def test_query_contexts_preserved(self):
        self.assertEqual(
            self.normalize()["query_contexts"], self.post["query_contexts"]
        )

    def test_categories_preserved(self):
        self.assertEqual(self.normalize()["categories"], self.post["categories"])

    def test_detected_text_locations_preserved(self):
        self.assertEqual(
            self.normalize()["detected_locations"], self.post["detected_locations"]
        )

    def test_relevance_preserved_from_search_after_enrichment(self):
        self.add_detail(text="Unrelated enriched text")
        result = self.normalize()
        for field in ("vietnam_score", "topic_score", "language_hint"):
            self.assertEqual(result[field], self.post["relevance"][field])
        self.assertEqual(result["relevance_signals"], self.post["relevance"]["signals"])
        self.assertEqual(result["categories"], self.post["categories"])

    def test_successful_detail_state(self):
        self.add_detail()
        result = self.normalize()
        self.assertEqual(result["detail_status"], "ok")
        self.assertIsNone(result["detail_error"])

    def test_input_is_not_mutated_and_output_is_not_aliased(self):
        self.add_detail()
        before = copy.deepcopy(self.post)
        result = self.normalize()
        self.assertEqual(self.post, before)
        result["matched_queries"].append("new query")
        result["query_contexts"][0]["query"] = "changed"
        result["categories"].clear()
        result["detected_locations"].clear()
        result["relevance_signals"].clear()
        self.assertEqual(self.post, before)

    def test_unknown_fields_and_raw_payload_are_excluded(self):
        self.post["unknown"] = {"nested": object()}
        self.post["query_contexts"][0]["unknown"] = "not a contract field"
        self.add_detail()
        result = self.normalize()
        for field in ("unknown", "raw_payload", "detail", "user", "visible_replies"):
            self.assertNotIn(field, result)
        self.assertNotIn("unknown", result["query_contexts"][0])
        json.dumps(result, allow_nan=False)

    def test_minimal_record_has_nullable_fields_and_empty_context(self):
        result = self.normalize({"id": "1"})
        for field in (
            "published_at",
            "text",
            "author_id",
            "like_count",
            "has_media",
            "vietnam_score",
        ):
            self.assertIsNone(result[field])
        for field in (
            "matched_queries",
            "query_contexts",
            "categories",
            "detected_locations",
            "relevance_signals",
        ):
            self.assertEqual(result[field], [])

    def test_invalid_outer_identity_is_rejected_even_with_detail(self):
        self.add_detail()
        for identity in (None, "", True, 0, -1, 1.5, {}, "bad"):
            with self.subTest(identity=identity):
                self.post["id"] = identity
                with self.assertRaisesRegex(ValueError, "post.id"):
                    self.normalize()

    def test_missing_or_naive_observation_metadata_is_rejected(self):
        # A naive datetime intentionally exercises the timezone requirement.
        naive = datetime(2026, 10, 1)  # noqa: DTZ001
        for field, values in {
            "run_id": (None, "", "   ", 123),
            "collected_at": (
                None,
                "invalid",
                "2026-10-01",
                naive,
            ),
            "normalized_at": (
                "invalid",
                "2026-10-05T00:00:00",
                naive,
            ),
        }.items():
            for value in values:
                with (
                    self.subTest(field=field, value=value),
                    self.assertRaises(ValueError),
                ):
                    self.normalize(**{field: value})

    def test_malformed_context_is_rejected_without_silent_loss(self):
        for field, value in (
            ("matched_queries", "not a list"),
            ("categories", [123]),
            ("query_contexts", {}),
            ("query_contexts", [{}]),
            ("query_contexts", [{"query": "q", "kind": "city", "geo_name": 123}]),
            ("detected_locations", {}),
        ):
            with self.subTest(field=field):
                post = {**self.post, field: value}
                with self.assertRaises((ValueError, TypeError)):
                    self.normalize(post)

    def test_no_filesystem_reads_during_normalization(self):
        with patch("builtins.open", side_effect=AssertionError("unexpected file read")):
            self.normalize()

    def test_contract_rejects_extra_fields_and_invalid_types(self):
        result = self.normalize()
        for changes in (
            {"extra": "raw"},
            {"like_count": -1},
            {"reply_count": True},
            {"normalized_at": "2026-10-05"},
        ):
            with self.subTest(changes=changes), self.assertRaises(AssertionError):
                self.assert_contract({**result, **changes})

    def test_saved_sample_all_posts_normalize(self):
        if not SAMPLE_PATH.exists():
            self.skipTest("read-only local Threads sample is not available")
        sample = json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))
        self.assertTrue(sample["posts"])
        self.assertEqual(len(sample["posts"]), sample["count"])
        for post in sample["posts"]:
            with self.subTest(source_id=post["id"]):
                before = copy.deepcopy(post)
                result = self.normalize(
                    post, run_id="sample-test-run", collected_at=sample["collected_at"]
                )
                self.assertEqual(result["source_record_id"], str(post["id"]))
                self.assertEqual(result["detail_status"], post.get("detail_status"))
                self.assertEqual(result["detail_error"], post.get("detail_error"))
                self.assertEqual(result["text"], post["text"])
                self.assertEqual(post, before)
                json.dumps(result, allow_nan=False)


if __name__ == "__main__":
    unittest.main()
