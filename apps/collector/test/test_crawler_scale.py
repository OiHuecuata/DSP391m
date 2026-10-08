"""Coverage, pacing boundaries, global deduplication and access-block handling."""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "apps/collector/src"))
from bs4 import BeautifulSoup
from dsp_collector.jobs.collect_social import process_threads_envelope
from dsp_collector.sources.crawler.pbplus import crawler as pbplus
from dsp_collector.sources.crawler.threads import crawler as threads
from dsp_collector.sources.crawler.threads.vietnam_terms import QuerySpec


class CrawlerScaleTests(unittest.TestCase):
    def test_query_seeds_use_canonical_plan_and_deduplicate(self):
        plan = threads.expanded_query_plan(["pickleball", " Pickleball "])
        self.assertEqual(len(plan), 17)
        self.assertIn("pickleball Hà Nội", [q.query for q in plan])

    def test_search_retains_native_payload(self):
        native = {
            "pk": "123",
            "caption": {"text": "pickleball"},
            "user": {},
            "extra_native": {"kept": True},
        }
        result = threads._post_from_edge(
            {"node": {"thread": {"thread_items": [{"post": native}]}}}
        )
        self.assertEqual(result["raw_payload"], native)

    def test_rate_limit_is_not_reloaded_or_bypassed(self):
        page = Mock()
        page.goto.return_value = SimpleNamespace(status=429)
        with self.assertRaises(threads.ThreadsAccessBlocked):
            threads._fetch_search_html(
                page, query="pickleball", sort="recent", timeout_ms=1000
            )
        page.reload.assert_not_called()

    def test_global_identity_dedup_and_detail_budget_remove_sample_cap(self):
        page = Mock()
        browser = Mock()
        browser.new_page.return_value = page
        manager = Mock()
        manager.__enter__ = Mock(
            return_value=SimpleNamespace(
                chromium=SimpleNamespace(launch=Mock(return_value=browser))
            )
        )
        manager.__exit__ = Mock(return_value=False)
        plan = [
            QuerySpec("pickleball Hà Nội", "pickleball", "hanoi", "Hà Nội", 1, "city"),
            QuerySpec("pickleball Hanoi", "pickleball", "hanoi", "Hà Nội", 2, "city"),
        ]
        posts = lambda start: [
            {"id": str(i), "text": "Tìm sân pickleball Hà Nội", "taken_at": 1791158400}
            for i in range(start, start + 10)
        ]
        with (
            patch.object(threads, "sync_playwright", return_value=manager),
            patch.object(threads, "expanded_query_plan", return_value=plan),
            patch.object(
                threads,
                "_collect_for_query",
                side_effect=[
                    (
                        posts(1),
                        {"has_next_page": False},
                        "https://www.threads.com/search/",
                    ),
                    (
                        posts(6),
                        {"has_next_page": False},
                        "https://www.threads.com/search/",
                    ),
                ],
            ),
            patch.object(threads, "_fetch_post_detail") as detail,
        ):
            result = threads.crawl_threads_posts(
                limit=1000, per_query_cap=200, detail_limit=0, query_delay=0
            )
        self.assertEqual(result["count"], 15)
        self.assertEqual(result["discovered_unique_count"], 15)
        self.assertEqual(
            sum(q["duplicate_discovered_count"] for q in result["query_runs"]), 5
        )
        self.assertEqual(
            sum(q["detail_skipped_count"] for q in result["query_runs"]), 15
        )
        detail.assert_not_called()
        self.assertEqual(result["collection_stopping_reason"], "query_plan_exhausted")

    def test_pbplus_access_block_is_not_retried(self):
        with (
            patch.object(
                pbplus.requests, "get", return_value=SimpleNamespace(status_code=403)
            ) as request,
            self.assertRaises(pbplus.PBPlusAccessBlocked),
        ):
            pbplus.get_page("https://pickleballplus.vn", retries=3)
        self.assertEqual(request.call_count, 1)

    def test_pbplus_block_stops_remaining_details_and_preserves_partial_data(self):
        soup = BeautifulSoup(
            '<a href="/san-tap/1">A</a><a href="/san-tap/1">A again</a><a href="/san-tap/2">B</a><a href="/san-tap/3">C</a>',
            "html.parser",
        )
        with (
            patch.object(pbplus, "get_page", return_value=soup),
            patch.object(
                pbplus,
                "scrape_court",
                side_effect=[
                    {"id": "https://pickleballplus.vn/san-tap/1"},
                    pbplus.PBPlusAccessBlocked("blocked"),
                ],
            ) as detail,
        ):
            result = pbplus.crawl_pbplus_courts(delay=0)
        self.assertEqual(detail.call_count, 2)
        self.assertEqual(result["count"], 1)
        self.assertTrue(result["query_runs"][0]["blocked"])
        self.assertEqual(result["query_runs"][0]["duplicate_links"], 1)
        self.assertEqual(result["query_runs"][0]["status"], "partial")

    def test_search_errors_are_not_reported_as_full_collection_success(self):
        envelope = {
            "source": "threads",
            "seed_query": "pickleball",
            "sort": "recent",
            "collected_at": "2026-10-08T00:00:00Z",
            "count": 1,
            "posts": [{"id": "123"}],
            "query_runs": [
                {
                    "query": "pickleball",
                    "kind": "country",
                    "geo_name": "Vietnam",
                    "status": "error",
                }
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            result = process_threads_envelope(envelope, artifact_root=directory)
            self.assertEqual(result.status, "partial")
            self.assertEqual(
                len(json.loads(result.silver_path.read_text())["collection_errors"]), 1
            )

    def test_detail_access_block_preserves_search_record_and_stops_queries(self):
        page = Mock()
        browser = Mock()
        browser.new_page.return_value = page
        manager = Mock()
        manager.__enter__ = Mock(
            return_value=SimpleNamespace(
                chromium=SimpleNamespace(launch=Mock(return_value=browser))
            )
        )
        manager.__exit__ = Mock(return_value=False)
        plan = [
            QuerySpec("pickleball Hà Nội", "pickleball", "hanoi", "Hà Nội", 1, "city"),
            QuerySpec("pickleball Hanoi", "pickleball", "hanoi", "Hà Nội", 2, "city"),
        ]
        post = {
            "id": "123",
            "text": "Tìm sân pickleball Hà Nội",
            "taken_at": 1791158400,
        }
        with (
            patch.object(threads, "sync_playwright", return_value=manager),
            patch.object(threads, "expanded_query_plan", return_value=plan),
            patch.object(
                threads,
                "_collect_for_query",
                return_value=([post], {}, "https://www.threads.com/search/"),
            ) as search,
            patch.object(
                threads,
                "_fetch_post_detail",
                side_effect=threads.ThreadsAccessBlocked("HTTP 403"),
            ),
        ):
            result = threads.crawl_threads_posts(query_delay=0)
        self.assertEqual(result["count"], 1)
        self.assertEqual(search.call_count, 1)
        self.assertEqual(result["collection_stopping_reason"], "access_blocked")
        self.assertTrue(result["query_runs"][0]["blocked"])


if __name__ == "__main__":
    unittest.main()
