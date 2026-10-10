"""Public-page fixtures: planner coverage, accounting, budgets and crash recovery."""

import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from dsp_collector.jobs.collect_social import process_threads_envelope
from dsp_collector.runtime.bronze import extract_observations
from dsp_collector.sources.crawler.threads import crawler
from dsp_collector.sources.crawler.threads.discovery import (
    BudgetExhausted,
    PublicPage,
    RequestBudget,
    acknowledge_checkpoint,
    build_discovery_plan,
    normalized,
    public_next_url,
    query_id,
)
from dsp_collector.sources.crawler.threads.vietnam_terms import (
    QuerySpec,
    build_query_plan,
)

AT = "2026-10-10T00:00:00Z"
EPOCH = datetime.fromisoformat(AT).timestamp()
PLAN = [
    QuerySpec("pickleball Hà Nội", "pickleball", "hanoi", "Hà Nội", 0, "city"),
    QuerySpec("pickleball Đà Nẵng", "pickleball", "da_nang", "Đà Nẵng", 1, "city"),
]


def post(identity, text="Tìm sân pickleball Hà Nội", when=EPOCH):
    return {
        "id": str(identity),
        "text": text,
        "taken_at": when,
        "like_count": 0,
        "raw_payload": {
            "pk": str(identity),
            "caption": {"text": text},
            "taken_at": when,
            "like_count": 0,
        },
    }


class ThreadsDiscoveryTests(unittest.TestCase):
    def test_legacy_plan_exact_17_is_geographic_vocabulary_not_query_budget(self):
        legacy = build_query_plan("pickleball")
        self.assertEqual(len(legacy), 17)
        self.assertEqual(sum(s.kind == "country" for s in legacy), 2)
        self.assertEqual(sum(s.kind == "city" for s in legacy), 14)
        self.assertEqual(sum(s.kind == "fallback" for s in legacy), 1)

    def test_registry_geography_vietnamese_intents_and_subject_variants(self):
        plan = build_discovery_plan(["pickleball"], include_local=True)
        values = {s.query for s in plan}
        self.assertIn("giá thuê sân pickleball Hà Nội", values)
        self.assertIn("ghép kèo pickleball", values)
        self.assertIn("huấn luyện pickleball Đà Nẵng", values)
        self.assertIn("pickle ball Hà Nội", values)
        self.assertIn("pickleball Cầu Giấy", values)
        subject = plan[:12]
        self.assertEqual(len({s.geo_name for s in subject if s.kind == "city"}), 10)
        self.assertEqual(
            {s.query_category for s in plan},
            {"subject", "venues", "pricing", "players", "training"},
        )

    def test_planner_deduplicates_unicode_case_and_seed_variants(self):
        one = build_discovery_plan(["pickleball"])
        repeated = build_discovery_plan([" Pickleball ", "pickleball"])
        self.assertEqual(
            [normalized(q.query) for q in one], [normalized(q.query) for q in repeated]
        )
        self.assertEqual(len(one), len({query_id(q.query) for q in one}))
        self.assertEqual(
            query_id(__import__("unicodedata").normalize("NFD", "pickleball Hà Nội")),
            query_id("pickleball Hà Nội"),
        )

    def test_configurable_categories_geographic_scope_and_aliases(self):
        plan = build_discovery_plan(
            ["pickleball"],
            categories=[],
            geo_keys=["vn-hanoi"],
            max_geo_aliases=1,
            expand_subject_variants=False,
        )
        self.assertEqual(len(plan), 4)  # unqualified, two country spellings, Hanoi
        with self.assertRaises(ValueError):
            build_discovery_plan(["pickleball"], geo_keys=["invented-province"])
        with self.assertRaises(ValueError):
            build_discovery_plan(["pickleball"], categories=["invented-intent"])

    def fixture(self, pages):
        page = Mock()
        page.url = crawler.build_search_url(PLAN[0].query)
        page.goto.side_effect = lambda url, **kw: (
            setattr(page, "url", url) or SimpleNamespace(status=200)
        )
        page.evaluate.return_value = None
        browser = Mock()
        browser.new_page.return_value = page
        manager = Mock()
        manager.__enter__ = Mock(
            return_value=SimpleNamespace(
                chromium=SimpleNamespace(launch=Mock(return_value=browser))
            )
        )
        manager.__exit__ = Mock(return_value=False)
        queue = iter(pages)

        def search(target, **kwargs):
            target.goto(crawler.build_search_url(kwargs["query_spec"].query))
            value = next(queue)
            if isinstance(value, Exception):
                raise value
            records, info = value
            return records, info, target.url

        return manager, page, browser, search

    def crawl(self, pages, **options):
        manager, _page, _browser, search = self.fixture(pages)
        with patch.object(
            crawler, "sync_playwright", return_value=manager
        ), patch.object(
            crawler, "expanded_query_plan", return_value=PLAN
        ), patch.object(
            crawler, "_collect_for_query", side_effect=search
        ):
            return crawler.crawl_threads_posts(detail_limit=0, query_delay=0, **options)

    def test_rejection_reasons_dedup_and_exact_accounting(self):
        result = self.crawl(
            [
                (
                    [
                        post(1),
                        post(2, "Tennis court Hà Nội"),
                        post(3, "pickleball news Hà Nội"),
                        post(4, "pickleball court in London"),
                    ],
                    {},
                ),
                ([post(1), post(5)], {}),
            ]
        )
        stats = result["discovery"]
        self.assertEqual(
            (
                stats["source_exposure_count"],
                stats["accepted_count"],
                stats["accepted_duplicates_count"],
                stats["rejected_count"],
            ),
            (6, 2, 1, 3),
        )
        reasons = {
            k: v
            for q in result["query_runs"]
            for k, v in q["rejection_reasons"].items()
        }
        self.assertEqual(
            reasons,
            {
                "missing_pickleball_subject": 1,
                "missing_market_topic": 1,
                "missing_vietnam_evidence": 1,
            },
        )
        self.assertEqual(sum(len(e["decisions"]) for e in result["search_evidence"]), 6)
        self.assertEqual(result["count"], 2)

    def test_result_and_per_query_limits_record_unevaluated_results(self):
        result = self.crawl(
            [([post(1), post(2), post(3)], {}), ([post(4), post(5)], {})],
            limit=2,
            per_query_cap=1,
        )
        self.assertEqual(result["count"], 2)
        self.assertEqual(result["discovery"]["not_evaluated_count"], 3)
        self.assertEqual(sum(q["accepted_count"] for q in result["query_runs"]), 2)

    def test_time_window_is_enforced_locally_without_invented_source_parameters(self):
        result = self.crawl(
            [
                (
                    [
                        post(1, when=EPOCH - 1),
                        post(2),
                        post(3, when=EPOCH + 3600),
                        post(4, when=None),
                    ],
                    {},
                )
            ],
            max_queries=1,
            published_after=AT,
            published_before="2026-10-10T01:00:00Z",
        )
        self.assertEqual(result["count"], 1)
        self.assertEqual(
            result["query_runs"][0]["rejection_reasons"],
            {"outside_time_window": 2, "missing_publication_time": 1},
        )
        self.assertEqual(
            result["discovery"]["window_enforcement"], "local_source_timestamp_filter"
        )

    def test_query_and_request_budgets_are_separate(self):
        result = self.crawl([([post(1)], {})], max_queries=1, max_requests=10)
        self.assertEqual(result["discovery"]["attempted_queries"], 1)
        self.assertEqual(result["collection_stopping_reason"], "query_budget")
        result = self.crawl([([post(1)], {})], max_requests=1)
        self.assertEqual(result["discovery"]["requests_this_invocation"], 1)
        self.assertEqual(result["collection_stopping_reason"], "request_budget")
        budget = RequestBudget(1, None)
        page = Mock()
        wrapped = PublicPage(page, budget)
        wrapped.goto("https://www.threads.com")
        with self.assertRaises(BudgetExhausted):
            wrapped.reload()
        page.reload.assert_not_called()

    def test_public_link_pagination_and_page_budget(self):
        manager, _page, _browser, search = self.fixture(
            [([post(1)], {"has_next_page": True})]
        )
        target = crawler.build_search_url(PLAN[0].query) + "&page=2"

        def following(p, **kw):
            p.goto(kw["url"])
            return [post(2)], {"has_next_page": True}, p.url

        with patch.object(
            crawler, "sync_playwright", return_value=manager
        ), patch.object(
            crawler, "expanded_query_plan", return_value=PLAN[:1]
        ), patch.object(
            crawler, "_collect_for_query", side_effect=search
        ), patch(
            "dsp_collector.sources.crawler.threads.discovery.public_next_url",
            return_value=target,
        ), patch.object(
            crawler, "_collect_public_page", side_effect=following
        ):
            result = crawler.crawl_threads_posts(
                detail_limit=0, query_delay=0, max_pages_per_query=2
            )
        self.assertEqual(result["count"], 2)
        self.assertEqual(len(result["query_runs"][0]["pages"]), 2)
        self.assertEqual(result["query_runs"][0]["pagination_stop"], "page_budget")

    def test_opaque_cursor_without_public_link_is_not_fabricated(self):
        result = self.crawl(
            [([post(1)], {"has_next_page": True, "end_cursor": "opaque"})],
            max_queries=1,
            max_pages_per_query=3,
        )
        self.assertEqual(
            result["query_runs"][0]["pagination_stop"],
            "cursor_without_supported_public_navigation",
        )
        self.assertEqual(len(result["search_evidence"]), 1)

    def test_public_links_cannot_change_source_query_or_bypass_access_controls(self):
        page = Mock()
        page.url = crawler.build_search_url(PLAN[0].query)
        for href in [
            "https://attacker.example/search/?q=pickleball",
            "https://www.threads.com/login",
            "https://www.threads.com/search/?q=other",
        ]:
            page.evaluate.return_value = href
            self.assertIsNone(public_next_url(page, PLAN[0].query))

    def test_checkpoint_continues_queries_and_does_not_republish_old_records(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "checkpoint.json"
            first = self.crawl(
                [([post(1)], {})], max_queries=1, checkpoint_path=path, run_id="first"
            )
            acknowledge_checkpoint(path, "first")
            second = self.crawl(
                [([post(1), post(2)], {})],
                max_queries=1,
                checkpoint_path=path,
                run_id="second",
            )
            self.assertEqual(first["count"], 1)
            self.assertEqual([p["id"] for p in second["posts"]], ["2"])
            self.assertEqual(second["query_runs"][0]["query"], PLAN[1].query)
            self.assertEqual(second["discovery"]["accepted_duplicates_count"], 1)
            self.assertEqual(
                first["discovery"]["survey_id"], second["discovery"]["survey_id"]
            )
            self.assertTrue(json.loads(path.read_text())["exported"])

    def test_checkpoint_recovery_preserves_successes_after_process_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "checkpoint.json"
            with self.assertRaisesRegex(RuntimeError, "process stopped"):
                self.crawl(
                    [([post(1)], {}), RuntimeError("process stopped")],
                    checkpoint_path=path,
                    run_id="pending",
                )
            checkpoint = json.loads(path.read_text())
            self.assertFalse(checkpoint["exported"])
            captured = checkpoint["posts"]["1"]["_observed_at"]
            resumed = self.crawl(
                [([post(2)], {})], checkpoint_path=path, run_id="pending"
            )
            self.assertEqual(resumed["count"], 2)
            self.assertEqual(resumed["posts"][0]["_observed_at"], captured)
            rows, errors = extract_observations({**resumed, "schema_version": "1"})
            self.assertFalse(errors)
            self.assertEqual(
                rows["bronze_social_posts"][0]["collected_at"],
                datetime.fromisoformat(captured)
                .astimezone(UTC)
                .isoformat(timespec="microseconds")
                .replace("+00:00", "Z"),
            )

    def test_source_access_block_stops_without_retry_and_keeps_evidence(self):
        result = self.crawl(
            [([post(1)], {}), crawler.ThreadsAccessBlocked("HTTP 429")], retries=3
        )
        self.assertEqual(result["count"], 1)
        self.assertEqual(result["discovery"]["failed_queries"], 1)
        self.assertEqual(result["collection_stopping_reason"], "access_blocked")
        with tempfile.TemporaryDirectory() as directory:
            processed = process_threads_envelope(result, artifact_root=directory)
            self.assertEqual(processed.status, "partial")

    def test_parser_preserves_full_search_evidence_and_reports_selection_limit(self):
        natives = [post(1), post(1), post(2), post(3)]
        edges = [
            {"node": {"thread": {"thread_items": [{"post": p["raw_payload"]}]}}}
            for p in natives
        ]
        source = {
            "inform_module": {},
            "edges": edges,
            "page_info": {"has_next_page": False},
        }
        html = '"searchResults":' + json.dumps(source)
        evidence = {}
        records, _ = crawler.parse_posts(html, 2, evidence=evidence)
        self.assertEqual(len(records), 2)
        self.assertEqual(evidence["source_exposure_count"], 4)
        self.assertEqual(evidence["within_page_duplicates_count"], 1)
        self.assertEqual(evidence["parser_deferred_count"], 1)
        self.assertEqual(len(evidence["search_results"]["edges"]), 4)

    def test_pagination_checkpoint_resumes_the_actual_public_link(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pagination.json"
            manager, _page, _browser, search = self.fixture(
                [([post(1)], {"has_next_page": True})]
            )
            target = crawler.build_search_url(PLAN[0].query) + "&page=2"
            with patch.object(
                crawler, "sync_playwright", return_value=manager
            ), patch.object(
                crawler, "expanded_query_plan", return_value=PLAN[:1]
            ), patch.object(
                crawler, "_collect_for_query", side_effect=search
            ), patch(
                "dsp_collector.sources.crawler.threads.discovery.public_next_url",
                return_value=target,
            ):
                first = crawler.crawl_threads_posts(
                    detail_limit=0,
                    query_delay=0,
                    max_requests=1,
                    max_pages_per_query=3,
                    checkpoint_path=path,
                    run_id="page-one",
                )
            self.assertEqual(first["collection_stopping_reason"], "request_budget")
            self.assertEqual(
                json.loads(path.read_text())["query_progress"]["next_url"], target
            )

            acknowledge_checkpoint(path, "page-one")

            def next_page(p, **kw):
                self.assertEqual(kw["url"], target)
                p.goto(kw["url"])
                return [post(2)], {"has_next_page": False}, p.url

            with patch.object(
                crawler, "sync_playwright", return_value=manager
            ), patch.object(
                crawler, "expanded_query_plan", return_value=PLAN[:1]
            ), patch.object(
                crawler,
                "_collect_for_query",
                side_effect=AssertionError("restarted at page one"),
            ), patch.object(
                crawler, "_collect_public_page", side_effect=next_page
            ):
                second = crawler.crawl_threads_posts(
                    detail_limit=0,
                    query_delay=0,
                    max_requests=2,
                    max_pages_per_query=3,
                    checkpoint_path=path,
                    run_id="page-two",
                )
            self.assertEqual([p["id"] for p in second["posts"]], ["2"])
            self.assertEqual(second["query_runs"][0]["pages"][0]["page"], 2)

    def test_duration_budget_can_stop_before_the_first_navigation(self):
        with patch(
            "dsp_collector.sources.crawler.threads.discovery.time.monotonic",
            side_effect=[0, 10],
        ):
            budget = RequestBudget(10, 1)
            with self.assertRaisesRegex(BudgetExhausted, "duration_budget"):
                budget.check()

    def test_cli_discovery_config_overrides_are_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "threads": {
                            "query": "pickleball",
                            "limit": 100,
                            "max_requests": 9,
                            "max_pages_per_query": 3,
                        }
                    }
                )
            )
            args = SimpleNamespace(
                discovery_config=path,
                max_queries=2,
                max_requests=None,
                max_pages_per_query=None,
                published_after=AT,
                published_before=None,
                lookback_days=None,
                checkpoint=Path(directory) / "state.json",
                fresh=True,
            )
            options = crawler.discovery_options(args)
            self.assertEqual(options["max_requests"], 9)
            self.assertEqual(options["max_queries"], 2)
            self.assertEqual(options["max_pages_per_query"], 3)
            self.assertFalse(options["resume"])
            self.assertNotIn("limit", options)

    def test_prepared_checkpoint_is_replayed_until_source_artifact_acknowledgement(
        self,
    ):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "checkpoint.json"
            first = self.crawl(
                [([post(1)], {})],
                max_queries=1,
                checkpoint_path=path,
                run_id="prepared",
            )
            with patch.object(
                crawler,
                "sync_playwright",
                side_effect=AssertionError("recrawled uncommitted source"),
            ), patch.object(crawler, "expanded_query_plan", return_value=PLAN):
                replay = crawler.crawl_threads_posts(
                    detail_limit=0,
                    query_delay=0,
                    max_queries=1,
                    checkpoint_path=path,
                    run_id="prepared",
                )
            self.assertEqual(first, replay)
            with self.assertRaisesRegex(ValueError, "not been acknowledged"):
                self.crawl([], max_queries=1, checkpoint_path=path, run_id="other")
            acknowledge_checkpoint(path, "prepared")
            next_run = self.crawl(
                [([post(2)], {})], max_queries=1, checkpoint_path=path, run_id="next"
            )
            self.assertEqual(next_run["query_runs"][0]["query"], PLAN[1].query)

    def test_invalid_checkpoint_configuration_fails_before_requests(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "checkpoint.json"
            self.crawl([([post(1)], {})], checkpoint_path=path, max_queries=1)
            with self.assertRaisesRegex(ValueError, "configuration changed"):
                self.crawl([], checkpoint_path=path, max_pages_per_query=2)
