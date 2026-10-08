"""Official API bounds, replay, source identities and credential containment."""

import copy
import json
import tempfile
import unittest
from pathlib import Path

import httpx
from dsp_collector.jobs.collect_youtube import process_youtube_envelope
from dsp_collector.normalizers.youtube import normalize_youtube
from dsp_collector.publishers.kafka import (
    DeliveryReceipt,
    build_social_post_event,
    publish_social_batch,
)
from dsp_collector.sources.api.youtube import APIStop, YouTubeConfig, collect_youtube


def video(identity="video_00001"):
    return {
        "id": identity,
        "snippet": {
            "title": "Sân pickleball Hà Nội",
            "description": "Chơi ở Đà Nẵng",
            "publishedAt": "2026-10-01T01:02:03Z",
            "channelId": "channel",
            "channelTitle": "Club",
            "tags": ["pickleball"],
        },
        "statistics": {"viewCount": "123", "likeCount": "5", "commentCount": "2"},
        "contentDetails": {"duration": "PT2M"},
    }


def comment(identity="comment_1"):
    return {
        "snippet": {
            "topLevelComment": {
                "id": identity,
                "snippet": {
                    "textOriginal": "Tìm sân ở Hà Nội",
                    "publishedAt": "2026-10-02T00:00:00Z",
                    "likeCount": 3,
                    "authorDisplayName": "Viewer",
                    "authorChannelId": {"value": "viewer"},
                },
            }
        }
    }


class YouTubeTests(unittest.TestCase):
    def test_youtube_mismatched_identity_is_rejected(self):
        r = normalize_youtube(
            video(), run_id="run", collected_at="2026-10-08T00:00:00Z"
        )
        r["source_record_id"] = "different"
        with self.assertRaisesRegex(ValueError, "identity conflicts"):
            build_social_post_event(r)

    def test_comment_contract_requires_parent_and_excludes_views(self):
        r = normalize_youtube(
            comment()["snippet"]["topLevelComment"],
            run_id="run",
            collected_at="2026-10-08T00:00:00Z",
            video_id="video_00001",
        )
        for change in (
            {"parent_record_id": None},
            {"view_count": 1},
            {"tags": ["invalid"]},
        ):
            with self.subTest(change=change), self.assertRaises(ValueError):
                build_social_post_event({**r, **change})

    def test_native_zero_counters_are_preserved(self):
        value = video()
        value["statistics"] = {"viewCount": "0", "likeCount": "0", "commentCount": "0"}
        r = normalize_youtube(value, run_id="run", collected_at="2026-10-08T00:00:00Z")
        self.assertEqual(
            (r["view_count"], r["like_count"], r["reply_count"]), (0, 0, 0)
        )

    def config(self, **kw):
        return YouTubeConfig(
            **{
                "keywords": ["pickleball Hà Nội"],
                "include_geography": False,
                "max_queries": 1,
                "max_pages": 2,
                "page_size": 2,
                "max_videos": 100,
                "max_comment_requests": 2,
                "max_comment_pages": 2,
                "max_comments_per_video": 3,
                "request_delay": 0,
                "retries": 0,
                **kw,
            }
        )

    def run_api(self, handler, **kw):
        requests = []

        def respond(request):
            requests.append(request)
            value = handler(request, len(requests))
            return (
                httpx.Response(value[0], json=value[1])
                if isinstance(value, tuple)
                else httpx.Response(200, json=value)
            )

        with httpx.Client(transport=httpx.MockTransport(respond)) as client:
            envelope = collect_youtube(
                "fixture-key-never-log",
                self.config(**kw),
                client=client,
                sleep=lambda _: None,
            )
        return envelope, requests

    def handler(self, request, n):
        if request.url.path.endswith("/search"):
            return {"items": [{"id": {"videoId": "video_00001"}}]}
        if request.url.path.endswith("/videos"):
            return {"items": [video()]}
        return {"items": [comment()]}

    def test_query_generation_reuses_vietnam_geography(self):
        queries = YouTubeConfig(max_queries=30).queries()
        self.assertIn("pickleball Hà Nội", queries)
        self.assertIn("pickleball Đà Nẵng", queries)
        self.assertEqual(len(queries), len(set(queries)))

    def test_search_pagination_global_deduplication(self):
        def handler(r, n):
            if r.url.path.endswith("/search"):
                return {
                    "items": [{"id": {"videoId": "video_00001"}}],
                    **(
                        {"nextPageToken": "next"}
                        if not r.url.params.get("pageToken")
                        else {}
                    ),
                }
            return self.handler(r, n)

        e, requests = self.run_api(handler)
        self.assertEqual(e["search_exposures"], 2)
        self.assertEqual(len(e["videos"]), 1)
        self.assertEqual(requests[1].url.params["pageToken"], "next")

    def test_repeated_token_stops_without_loop(self):
        e, _r = self.run_api(
            lambda r, n: (
                {
                    "items": [{"id": {"videoId": "video_00001"}}],
                    "nextPageToken": "repeat",
                }
                if r.url.path.endswith("/search")
                else self.handler(r, n)
            ),
            max_pages=20,
        )
        self.assertEqual(e["stopping_reason"], "repeated_page_token")
        self.assertEqual(e["requests"]["by_endpoint"]["search"], 2)

    def test_metadata_batches_at_most_fifty(self):
        def handler(r, n):
            if r.url.path.endswith("/search"):
                offset = 50 if r.url.params.get("pageToken") else 0
                return {
                    "items": [
                        {"id": {"videoId": f"video_{i:05}"}}
                        for i in range(offset, offset + 50)
                    ],
                    "nextPageToken": "next",
                }
            return (
                {"items": [video(v) for v in r.url.params.get("id", "").split(",")]}
                if r.url.path.endswith("/videos")
                else {"items": []}
            )

        e, r = self.run_api(handler, page_size=50, max_comment_requests=0)
        batches = [
            r.url.params["id"].split(",") for r in r if r.url.path.endswith("/videos")
        ]
        self.assertEqual(list(map(len, batches)), [50, 50])
        self.assertEqual(len(e["videos"]), 100)

    def test_comments_are_bounded_and_deduplicated(self):
        def handler(r, n):
            if r.url.path.endswith("/commentThreads"):
                return {
                    "items": [comment(f"c_{i}") for i in range(4)],
                    "nextPageToken": "next",
                }
            return self.handler(r, n)

        e, _ = self.run_api(handler)
        self.assertEqual(len(e["comments"]), 3)
        self.assertEqual(e["requests"]["by_endpoint"]["commentThreads"], 1)

    def test_comments_disabled_keeps_video_success(self):
        e, _ = self.run_api(
            lambda r, n: (
                (403, {"error": {"errors": [{"reason": "commentsDisabled"}]}})
                if r.url.path.endswith("/commentThreads")
                else self.handler(r, n)
            )
        )
        with tempfile.TemporaryDirectory() as d:
            result = process_youtube_envelope(e, run_id="disabled", artifact_root=d)
        self.assertEqual(result.status, "success")
        self.assertEqual(result.count, 1)

    def test_empty_comments_are_clean(self):
        e, _ = self.run_api(
            lambda r, n: (
                {"items": []}
                if r.url.path.endswith("/commentThreads")
                else self.handler(r, n)
            )
        )
        self.assertEqual(e["comments"], [])
        self.assertEqual(e["errors"], [])

    def test_request_budget_includes_all_requests(self):
        e, r = self.run_api(self.handler, max_requests=2)
        self.assertEqual(len(r), 2)
        self.assertEqual(e["stopping_reason"], "request_budget")
        self.assertIsNone(e["requests"]["remaining_quota"])

    def test_quota_stops_without_retrying(self):
        e, r = self.run_api(
            lambda r, n: (
                403,
                {
                    "error": {
                        "errors": [{"reason": "quotaExceeded"}],
                        "message": "fixture-key-never-log",
                    }
                },
            ),
            retries=3,
        )
        self.assertEqual(len(r), 1)
        self.assertEqual(e["stopping_reason"], "quotaExceeded")
        self.assertNotIn("fixture-key-never-log", json.dumps(e))

    def test_rate_limit_retries_are_budgeted(self):
        e, _r = self.run_api(
            lambda r, n: (429, {"error": {}}) if n == 1 else self.handler(r, n),
            retries=1,
        )
        self.assertEqual(e["requests"]["by_endpoint"]["search"], 2)
        self.assertTrue(e["videos"])

    def test_unavailable_api_has_safe_reason(self):
        def handler(r, n):
            raise httpx.ConnectError("fixture-key-never-log", request=r)

        e, r = self.run_api(handler, retries=1)
        self.assertEqual(len(r), 2)
        self.assertEqual(e["stopping_reason"], "api_unavailable")
        self.assertNotIn("fixture-key-never-log", json.dumps(e))

    def test_key_not_in_url_or_bronze(self):
        e, r = self.run_api(self.handler)
        self.assertTrue(all("key" not in str(r.url) for r in r))
        self.assertEqual(r[0].headers["X-Goog-Api-Key"], "fixture-key-never-log")
        self.assertNotIn("fixture-key-never-log", json.dumps(e))

    def test_publication_window_is_forwarded(self):
        _e, r = self.run_api(
            self.handler,
            published_after="2026-01-01T00:00:00Z",
            published_before="2026-10-01T00:00:00Z",
        )
        self.assertEqual(r[0].url.params["publishedAfter"], "2026-01-01T00:00:00Z")

    def test_config_validation(self):
        for kw in (
            {"page_size": 51},
            {"max_requests": 0},
            {"keywords": []},
            {"retries": 4},
            {"order": "invalid"},
            {"published_after": "2026-01-01"},
        ):
            with self.subTest(kw=kw), self.assertRaises(ValueError):
                self.config(**kw)

    def test_not_configured(self):
        with self.assertRaisesRegex(APIStop, "youtube_not_configured"):
            collect_youtube("")

    def test_identity_native_metrics_and_geography(self):
        t = "2026-10-08T00:00:00.000000Z"
        v = normalize_youtube(video(), run_id="run", collected_at=t)
        c = normalize_youtube(
            comment()["snippet"]["topLevelComment"],
            run_id="run",
            collected_at=t,
            video_id="video_00001",
        )
        self.assertEqual(v["record_id"], "youtube:video_00001")
        self.assertEqual(c["record_id"], "youtube-comment:comment_1")
        self.assertEqual(c["parent_record_id"], v["record_id"])
        self.assertEqual(v["view_count"], 123)
        self.assertIsNone(c["view_count"])
        self.assertIsNone(c["reply_count"])
        self.assertEqual(c["detected_locations"], ["Hà Nội"])
        self.assertIn("Đà Nẵng", v["detected_locations"])
        build_social_post_event(v)
        build_social_post_event(c)

    def test_missing_metrics_are_null(self):
        v = video()
        v.pop("statistics")
        row = normalize_youtube(
            v, run_id="run", collected_at="2026-10-08T00:00:00.000000Z"
        )
        self.assertIsNone(row["view_count"])
        self.assertIsNone(row["like_count"])

    def test_replay_preserves_complete_observation_and_event_id(self):
        e, _ = self.run_api(self.handler)
        with tempfile.TemporaryDirectory() as d:
            a = process_youtube_envelope(e, run_id="replay", artifact_root=d)
            raw = Path(a.bronze_path).read_text()
            silver = Path(a.silver_path).read_text()
            b = process_youtube_envelope(json.loads(raw), artifact_root=d)
            self.assertEqual(a, b)
            self.assertEqual(Path(b.silver_path).read_text(), silver)
            records = json.loads(silver)["records"]
            self.assertEqual(
                build_social_post_event(records[0])["event_id"],
                build_social_post_event(copy.deepcopy(records[0]))["event_id"],
            )

    def test_kafka_publication_uses_shared_topic_and_identity(self):
        e, _ = self.run_api(self.handler)
        events = []

        class Publisher:
            def __init__(self, settings):
                pass

            def publish_social_post(self, event):
                events.append(event)
                return DeliveryReceipt(
                    event["event_id"], event["record_id"], "delivered"
                )

            def close(self):
                pass

        with tempfile.TemporaryDirectory() as d:
            result = process_youtube_envelope(e, run_id="publish", artifact_root=d)
            published = publish_social_batch(
                result.silver_path, publisher_factory=Publisher
            )
        self.assertEqual(published.published_count, 2)
        self.assertEqual({e["source"] for e in events}, {"youtube", "youtube_comment"})

    def test_api_error_partial_preserves_collected_metadata(self):
        e, _ = self.run_api(
            lambda r, n: (
                (403, {"error": {"errors": [{"reason": "quotaExceeded"}]}})
                if r.url.path.endswith("/commentThreads")
                else self.handler(r, n)
            )
        )
        with tempfile.TemporaryDirectory() as d:
            result = process_youtube_envelope(e, run_id="partial", artifact_root=d)
        self.assertEqual(result.status, "partial")
        self.assertEqual(result.count, 1)
