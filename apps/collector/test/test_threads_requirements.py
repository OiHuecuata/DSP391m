"""Requirements-backed regression gate, using public-page fixtures only."""

import copy
import json
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

import test_threads_discovery as fixtures
from dsp_collector.jobs.collect_social import process_threads_envelope
from dsp_collector.runtime.bronze import extract_observations, validate_observation
from dsp_collector.sources.crawler.threads import crawler
from dsp_collector.sources.crawler.threads.discovery import (
    acknowledge_checkpoint,
    build_discovery_plan,
    fingerprint,
    national_geography,
    query_id,
    rejection_reasons,
    semantic_vocabulary,
)
from dsp_collector.sources.crawler.threads.post_parser import (
    ThreadsPostParserError,
    _engagement,
    _visible_replies,
)
from dsp_collector.sources.crawler.threads.survey import coverage, migrate, select_query

AT, PLAN, post = fixtures.AT, fixtures.PLAN, fixtures.post

ROOT = Path(__file__).resolve().parents[3]


class ThreadsRequirementsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.plan = build_discovery_plan(["pickleball"], include_local=True)

    def crawl(self, pages, **kwargs):
        return fixtures.ThreadsDiscoveryTests().crawl(pages, **kwargs)

    def test_all_province_versions_and_query_provenance(self):
        references = national_geography()
        provinces = [r for r in references if r["level"] == "province"]
        counts = Counter(r["admin_version"] for r in provinces)
        self.assertEqual(counts, {"pre_2025_07": 63, "2025_07": 34})
        reachable = {p["geo_id"] for q in self.plan for p in q.geo_provenance}
        self.assertTrue({r["geo_id"] for r in provinces} <= reachable)
        hanoi = next(q for q in self.plan if q.query == "pickleball Hà Nội")
        self.assertEqual(
            {p["admin_version"] for p in hanoi.geo_provenance},
            {"pre_2025_07", "2025_07"},
        )
        self.assertTrue(
            any(q.query == "pickleball Sài Gòn" for q in self.plan),
            "Sài Gòn must be retained",
        )
        self.assertTrue(all(r["source_url"] for r in references))

    def test_unlimited_aliases_validated_localities_and_dedup(self):
        one = build_discovery_plan(
            ["pickleball"], categories=[], max_geo_aliases=1, include_local=True
        )
        unlimited = build_discovery_plan(
            ["pickleball"], categories=[], max_geo_aliases=None, include_local=True
        )
        self.assertEqual([q.query for q in one], [q.query for q in unlimited])
        self.assertIn("pickleball Nghĩa Đô", {q.query for q in one})
        self.assertEqual(len(self.plan), len({query_id(q.query) for q in self.plan}))
        self.assertGreater(len(self.plan), 702)

    def test_semantic_variants_and_contextual_relevance(self):
        values = {q.query for q in self.plan}
        for phrase in [
            "chơi pickleball",
            "tìm kèo pickleball Hà Nội",
            "học pickleball Cần Thơ",
            "giải pickleball Hải Phòng",
            "thuê sân pickleball Đà Nẵng",
            "lớp pickleball Huế",
            "tim keo pickleball Hà Nội",
            "sân full pickleball",
        ]:
            self.assertTrue(phrase in values, phrase)
        self.assertEqual(
            set(semantic_vocabulary()),
            {
                "playing",
                "players",
                "training",
                "competitions",
                "venues",
                "booking",
                "pricing",
                "availability",
                "supply",
                "feedback",
            },
        )
        self.assertIn(
            "missing_pickleball_subject",
            rejection_reasons(post(1, "Tìm kèo Hà Nội"), PLAN[0]),
        )
        self.assertFalse(rejection_reasons(post(1, "Giải pickleball Hà Nội"), PLAN[0]))

    def test_fair_first_round_and_stable_expansion(self):
        first = self.plan[:32]
        self.assertGreaterEqual(len({q.query_category for q in first}), 10)
        self.assertGreaterEqual(len({q.geo_name for q in first}), 10)
        state = migrate({"query_index": 0}, self.plan, fingerprint([]))
        chosen = []
        for _ in range(2):
            attempted = set()
            for _ in range(16):
                _spec, qid = select_query(state, self.plan, attempted)
                attempted.add(qid)
                chosen.append(qid)
                state["ledger"][qid] = {"status": "exhausted", "attempts": 1}
        self.assertEqual(len(set(chosen)), 32)
        migrated = migrate(state, list(reversed(self.plan)), fingerprint([]))
        self.assertEqual(coverage(migrated, self.plan)["completed_queries"], 32)

    def test_actual_legacy_cursor_migration_uses_saved_plan_not_new_indices(self):
        original = json.loads(
            (Path(__file__).parent / "fixtures/threads_checkpoint_v1.json").read_text()
        )
        if original.get("coverage_version") == "2":
            self.skipTest("active fixture is no longer a legacy checkpoint")
        state = migrate(copy.deepcopy(original), self.plan, fingerprint([]))
        ids = {
            query_id(q["query"])
            for q in original["envelope"]["query_plan"][: original["query_index"]]
        }
        self.assertTrue(ids <= set(state["ledger"]))
        self.assertEqual(state["migration"]["old_cursor"], 92)
        self.assertEqual(state["survey_id"], original["survey_id"])
        self.assertEqual(state["envelope"], original["envelope"])

    def test_zero_results_budget_and_bronze_status_agree(self):
        for limited in [False, True]:
            result = self.crawl(
                [([], {}), ([], {})], max_queries=1 if limited else None
            )
            with tempfile.TemporaryDirectory() as td:
                batch = process_threads_envelope(result, artifact_root=td)
                rows, _ = extract_observations(result)
                self.assertEqual(batch.status, "partial" if limited else "success")
                self.assertEqual(
                    rows["bronze_collection_runs"][0]["execution_status"], batch.status
                )
                self.assertFalse(
                    json.loads(Path(batch.silver_path).read_text())["collection_errors"]
                )

    def test_transient_failure_is_retry_pending_without_coverage_loss(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "state.json"
            result = self.crawl(
                [crawler.ThreadsCrawlerError("timeout"), ([], {})],
                retries=0,
                retry_backoff_seconds=0,
                checkpoint_path=path,
                run_id="first",
            )
            self.assertEqual(result["discovery"]["retry_pending_queries"], 1)
            self.assertEqual(result["discovery"]["exhausted_queries"], 1)
            acknowledge_checkpoint(path, "first")
            resumed = self.crawl(
                [([post(1)], {})],
                checkpoint_path=path,
                run_id="next",
                retry_backoff_seconds=0,
            )
            self.assertEqual(resumed["query_runs"][0]["query"], PLAN[0].query)
            self.assertEqual(resumed["count"], 1)

    def test_result_limit_retains_unprocessed_source_evidence(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "state.json"
            first = self.crawl(
                [([post(1), post(2)], {})],
                limit=1,
                max_queries=1,
                checkpoint_path=path,
                run_id="first",
            )
            self.assertEqual(first["discovery"]["deferred_queries"], 1)
            acknowledge_checkpoint(path, "first")
            # The next invocation can process the retained page without refetching.
            state = json.loads(path.read_text())
            state["schedule_cursor"] = 0
            path.write_text(json.dumps(state))
            second = self.crawl([], max_queries=1, checkpoint_path=path, run_id="next")
            self.assertEqual(second["count"], 1)
            self.assertEqual(second["posts"][0]["id"], "2")
            self.assertEqual(
                second["posts"][0]["_observed_at"], first["posts"][0]["_observed_at"]
            )

    def test_optional_detail_crash_cannot_erase_accepted_search(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "state.json"
            case = fixtures.ThreadsDiscoveryTests()
            manager, _, _, search = case.fixture([([post(1)], {})])
            with (
                patch.object(crawler, "sync_playwright", return_value=manager),
                patch.object(crawler, "expanded_query_plan", return_value=PLAN[:1]),
                patch.object(crawler, "_collect_for_query", side_effect=search),
                patch.object(
                    crawler, "_fetch_post_detail", side_effect=RuntimeError("crash")
                ),
                self.assertRaisesRegex(RuntimeError, "crash"),
            ):
                crawler.crawl_threads_posts(
                    checkpoint_path=path, run_id="pending", detail_limit=1
                )
            state = json.loads(path.read_text())
            self.assertEqual(state["posts"]["1"]["text"], post(1)["text"])
            self.assertTrue(
                next(iter(state["ledger"].values()))["staged_page"]["records"]
            )

    def test_nullable_counters_and_malformed_counter_failure(self):
        missing = _engagement({})
        self.assertTrue(all(v is None for v in missing.values()))
        self.assertEqual(_engagement({"like_count": 0})["like_count"], 0)
        self.assertEqual(_engagement({"like_count": "12"})["like_count"], 12)
        for value in [-1, True, "unknown"]:
            with self.assertRaises(ThreadsPostParserError):
                _engagement({"like_count": value})

    def test_nested_reply_links_and_capture_times_in_bronze(self):
        raw = {
            "pk": "1",
            "caption": {"text": "Sân pickleball Hà Nội"},
            "taken_at": 1,
            "like_count": None,
        }
        child = {
            "pk": "2",
            "caption": {"text": "Cho mình hỏi giá"},
            "like_count": 0,
            "text_post_app_info": {"is_reply": True, "reply_to_post_id": "1"},
        }
        nested = {
            "pk": "3",
            "caption": {"text": "Giá sân"},
            "text_post_app_info": {"is_reply": True, "reply_to_post_id": "2"},
        }
        unrelated = {"pk": "4", "text_post_app_info": {"is_reply": True}}
        replies = _visible_replies([raw, child, nested, unrelated], main_post=raw)
        self.assertEqual([r["id"] for r in replies], ["2", "3"])
        replies[0]["_observed_at"] = "2026-10-10T01:00:00Z"
        env = {
            "source": "threads",
            "run_id": "reply-test",
            "seed_query": "pickleball",
            "sort": "recent",
            "collected_at": AT,
            "count": 1,
            "query_runs": [],
            "posts": [
                {
                    **post(1),
                    "raw_payload": raw,
                    "_observed_at": AT,
                    "detail": {
                        "raw_payload": raw,
                        "visible_replies": replies,
                        "_observed_at": "2026-10-10T01:00:00Z",
                    },
                }
            ],
        }
        rows, errors = extract_observations(env)
        self.assertFalse(errors)
        self.assertEqual(len(rows["bronze_social_posts"]), 1)
        self.assertEqual(len(rows["bronze_social_comments"]), 2)
        self.assertEqual(rows["bronze_social_comments"][1]["parent_comment_id"], "2")
        self.assertTrue(
            rows["bronze_social_comments"][0]["collected_at"].startswith(
                "2026-10-10T01:"
            )
        )
        self.assertIsNone(rows["bronze_engagement_snapshots"][0]["like_count"])
        for dataset, records in rows.items():
            for record in records:
                validate_observation(dataset, record)
        self.assertEqual(
            extract_observations(env), extract_observations(copy.deepcopy(env))
        )

    def test_field_matrix_covers_every_contract_field(self):
        matrix = json.loads(
            (
                ROOT / "contracts/data/bronze/threads-requirements-matrix.v1.json"
            ).read_text()
        )["fields"]
        for name in [
            "social-posts",
            "social-comments",
            "engagement-snapshots",
            "collection-runs",
            "source-changes",
            "court-observations",
        ]:
            schema = json.loads(
                (ROOT / f"contracts/data/bronze/{name}.v1.schema.json").read_text()
            )
            fields = [
                r for r in matrix if r["dataset"] == "bronze_" + name.replace("-", "_")
            ]
            self.assertEqual({r["field"] for r in fields}, set(schema["properties"]))
            self.assertTrue(
                all(
                    r["value_source"] and r["validation"] and r["test_evidence"]
                    for r in fields
                )
            )

    def test_empty_diagnostics_retain_all_candidates_and_malformed_edges(self):
        candidates = [
            {"inform_module": None, "edges": [], "page_info": {"has_next_page": False}},
            {
                "inform_module": None,
                "edges": [{"unsupported": "edge"}],
                "page_info": {},
            },
        ]
        html = " ".join('"searchResults":' + json.dumps(c) for c in candidates)
        evidence = {}
        records, _ = crawler.parse_posts(html, 25, evidence=evidence)
        self.assertFalse(records)
        self.assertEqual(evidence["ssr_candidate_count"], 2)
        self.assertEqual(len(evidence["ssr_candidates"]), 2)
        self.assertEqual(
            evidence["empty_classification"], "empty_selected_ssr_unverified"
        )

    def test_legacy_checkpoint_only_recovery_proves_plan_and_preserves_capture(self):
        original = json.loads(
            (Path(__file__).parent / "fixtures/threads_checkpoint_v1.json").read_text()
        )
        captured = original["started_at"]
        original.update(
            exported=False,
            artifact_committed=False,
            posts={"1": {**post(1), "_observed_at": captured}},
            query_index=1,
            query_runs=[],
            search_evidence=[],
            query_progress=None,
        )
        original.pop("envelope")
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "legacy.json"
            path.write_text(json.dumps(original))
            recovered = self.crawl(
                [([post(2)], {})],
                run_id=original["run_id"],
                checkpoint_path=path,
                max_queries=1,
                include_local=True,
                per_query_cap=25,
                max_results_per_query=25,
                max_pages_per_query=2,
            )
            self.assertEqual(recovered["run_id"], original["run_id"])
            self.assertEqual(recovered["posts"][0]["_observed_at"], captured)
            self.assertEqual(
                recovered["discovery"]["checkpoint_migration"]["old_cursor"], 1
            )

    def test_search_and_detail_are_two_evidenced_metric_captures(self):
        first = post(1)
        native = copy.deepcopy(first["raw_payload"])
        native["like_count"] = 3
        first.update(
            _observed_at=AT,
            _metrics_observed_at="2026-10-10T00:01:00Z",
            detail_status="ok",
            detail={
                "id": "1",
                "raw_payload": native,
                "_observed_at": "2026-10-10T00:01:00Z",
            },
        )
        env = {
            "source": "threads",
            "run_id": "two-captures",
            "seed_query": "pickleball",
            "sort": "recent",
            "collected_at": "2026-10-10T00:02:00Z",
            "count": 1,
            "query_runs": [],
            "posts": [first],
        }
        rows, errors = extract_observations(env)
        self.assertFalse(errors)
        self.assertEqual(len(rows["bronze_social_posts"]), 1)
        self.assertEqual(len(rows["bronze_engagement_snapshots"]), 2)
        self.assertEqual(
            {r["like_count"] for r in rows["bronze_engagement_snapshots"]}, {0, 3}
        )
        self.assertTrue(
            rows["bronze_social_posts"][0]["collected_at"].startswith(
                "2026-10-10T00:01:"
            )
        )
        with tempfile.TemporaryDirectory() as td:
            result = process_threads_envelope(env, artifact_root=td)
            row = json.loads(result.silver_path.read_text())["records"][0]
            self.assertEqual(
                row["collected_at"], rows["bronze_social_posts"][0]["collected_at"]
            )

    def test_compressed_bronze_transport_is_lossless_and_bounded(self):
        import os

        from dsp_collector.publishers.kafka import (
            build_bronze_event,
            validate_bronze_event,
        )

        env = {
            "source": "threads",
            "run_id": "codec",
            "seed_query": "pickleball",
            "sort": "recent",
            "collected_at": AT,
            "count": 1,
            "query_runs": [],
            "posts": [post(1)],
        }
        rows, _ = extract_observations(env)
        row = rows["bronze_social_posts"][0]
        plain = build_bronze_event("bronze_social_posts", row)
        with patch.dict(os.environ, {"DSP_BRONZE_COMPRESSION_THRESHOLD": "1"}):
            compressed = build_bronze_event("bronze_social_posts", row)
        self.assertEqual(
            validate_bronze_event(compressed), validate_bronze_event(plain)
        )
        self.assertEqual(compressed["event_id"], plain["event_id"])
        with (
            patch.dict(os.environ, {"DSP_BRONZE_MAX_DECODE_BYTES": "10"}),
            self.assertRaises(ValueError),
        ):
            validate_bronze_event(compressed)
        compressed["record_json"] = "invalid-base64"
        with self.assertRaises(ValueError):
            validate_bronze_event(compressed)

    def test_pagination_timeout_has_explicit_failure_classification(self):
        from unittest.mock import Mock

        from playwright.sync_api import TimeoutError

        page = Mock()
        page.goto.side_effect = TimeoutError("deadline")
        with self.assertRaises(crawler.ThreadsCrawlerError) as caught:
            crawler._collect_public_page(
                page,
                url="https://www.threads.com/search?q=pickleball",
                query_spec=PLAN[0],
                sort="recent",
                timeout_ms=1,
                raw_limit=1,
            )
        self.assertEqual(caught.exception.classification, "pagination_timeout")

    def test_bad_optional_counter_keeps_text_and_reports_rejection(self):
        env = {
            "source": "threads",
            "run_id": "bad-counter",
            "seed_query": "pickleball",
            "sort": "recent",
            "collected_at": AT,
            "query_runs": [],
            "count": 1,
            "posts": [post(1)],
        }
        env["posts"][0]["raw_payload"]["like_count"] = "unreadable"
        rows, errors = extract_observations(env)
        self.assertEqual(len(rows["bronze_social_posts"]), 1)
        self.assertEqual(errors[0]["field"], "like_count")
        self.assertIsNone(rows["bronze_engagement_snapshots"][0]["like_count"])

    def test_unknown_reply_root_is_retained_without_fabrication(self):
        child = post(2)
        child["raw_payload"]["text_post_app_info"] = {
            "is_reply": True,
            "reply_to_post_id": "999",
        }
        child["is_reply"] = True
        env = {
            "source": "threads",
            "run_id": "unknown-root",
            "seed_query": "pickleball",
            "sort": "recent",
            "collected_at": AT,
            "count": 1,
            "query_runs": [],
            "posts": [child],
        }
        rows, errors = extract_observations(env)
        self.assertFalse(rows["bronze_social_posts"])
        self.assertFalse(rows["bronze_social_comments"])
        self.assertTrue(errors)
        self.assertEqual(
            env["posts"][0]["raw_payload"]["text_post_app_info"]["reply_to_post_id"],
            "999",
        )
