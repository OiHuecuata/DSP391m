"""Offline collection-boundary, lineage, artifact, and failure tests."""

from __future__ import annotations

import copy
import io
import json
import re
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

COLLECTOR_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = COLLECTOR_ROOT.parents[1]
sys.path.insert(0, str(COLLECTOR_ROOT / "src"))

from dsp_collector.jobs import collect_social as job
from dsp_collector.normalizers.social_post import normalize_social_post
from dsp_collector.runtime import artifacts
from dsp_collector.runtime.config import default_artifact_root
from dsp_collector.runtime.status import CrawlerStatusTracker

SAMPLE_PATH = COLLECTOR_ROOT / ".local/threads/pickleball_sample.json"
COLLECTED_AT = "2026-10-01T16:02:10.383825+00:00"


class CollectSocialTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bronze_schema = json.loads(
            (
                REPOSITORY_ROOT / "contracts/data/bronze/threads-crawl.v1.schema.json"
            ).read_text(encoding="utf-8")
        )
        cls.silver_schema = json.loads(
            (
                REPOSITORY_ROOT / "contracts/data/silver/social-post.v1.schema.json"
            ).read_text(encoding="utf-8")
        )

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        # Fail the entire suite if a tested path attempts network access.
        network = patch(
            "socket.socket", side_effect=AssertionError("unexpected network call")
        )
        network.start()
        self.addCleanup(network.stop)
        self.envelope = {
            "source": "threads",
            "seed_query": "pickleball",
            "sort": "recent",
            "collected_at": COLLECTED_AT,
            "count": 2,
            "query_runs": [],
            "posts": [
                {
                    "id": "123",
                    "code": "TestCode",
                    "url": "https://www.threads.com/@player/post/TestCode",
                    "text": "Tìm sân pickleball Hà Nội 🇻🇳",
                    "taken_at": 1780208417,
                    "like_count": 3,
                    "reply_count": 1,
                    "repost_count": 0,
                    "quote_count": 0,
                    "is_reply": False,
                    "has_media": False,
                    "media_type": 19,
                    "user": {
                        "pk": "100",
                        "username": "player",
                        "full_name": "Người chơi",
                        "is_verified": False,
                    },
                    "matched_queries": ["pickleball Hà Nội"],
                    "query_contexts": [
                        {
                            "query": "pickleball Hà Nội",
                            "kind": "city",
                            "geo_key": "hanoi",
                            "geo_name": "Hà Nội",
                        }
                    ],
                    "categories": ["court"],
                    "detected_locations": ["Hà Nội"],
                    "relevance": {
                        "vietnam_score": 8,
                        "topic_score": 3,
                        "language_hint": "vi",
                        "signals": ["geo:text:Hà Nội"],
                    },
                    "detail": None,
                    "detail_status": "error",
                    "detail_error": "Detail unavailable",
                },
                {"id": "456", "text": "Tìm người chơi pickleball"},
            ],
        }

    def process(self, **kwargs):
        options = {"run_id": "test-run", "artifact_root": self.root}
        options.update(kwargs)
        return job.process_threads_envelope(self.envelope, **options)

    def read_artifacts(self, result):
        return (
            json.loads(result.bronze_path.read_text(encoding="utf-8")),
            json.loads(result.silver_path.read_text(encoding="utf-8")),
        )

    def assert_contract(self, value, schema, node=None):
        """Check the JSON Schema constraints used here without new dependencies."""
        node = schema if node is None else node
        if "$ref" in node:
            node = schema["$defs"][node["$ref"].removeprefix("#/$defs/")]
        kinds = node["type"]
        kinds = kinds if isinstance(kinds, list) else [kinds]
        matches = {
            "null": value is None,
            "string": isinstance(value, str),
            "integer": type(value) is int,
            "number": type(value) in (int, float),
            "boolean": type(value) is bool,
            "array": isinstance(value, list),
            "object": isinstance(value, dict),
        }
        self.assertTrue(any(matches[kind] for kind in kinds), (value, kinds))
        if "const" in node:
            self.assertEqual(value, node["const"])
        if "enum" in node:
            self.assertIn(value, node["enum"])
        if value is None:
            return
        if isinstance(value, dict):
            self.assertTrue(set(node.get("required", [])).issubset(value))
            properties = node.get("properties", {})
            if node.get("additionalProperties") is False:
                self.assertTrue(set(value).issubset(properties))
            for key, child in value.items():
                if key in properties:
                    self.assert_contract(child, schema, properties[key])
        elif isinstance(value, list):
            for child in value:
                self.assert_contract(child, schema, node["items"])
        elif isinstance(value, str):
            self.assertGreaterEqual(len(value), node.get("minLength", 0))
            if "pattern" in node:
                self.assertIsNotNone(re.search(node["pattern"], value))
            if node.get("format") == "date-time":
                self.assertIsNotNone(datetime.fromisoformat(value).utcoffset())
        elif type(value) in (int, float) and "minimum" in node:
            self.assertGreaterEqual(value, node["minimum"])

    def tracked_crawler(self, **kwargs):
        kwargs["status_tracker"].start(
            seed_query=kwargs["query"],
            sort=kwargs["sort"],
            target_count=kwargs["limit"],
            query_plan_count=0,
        )
        return copy.deepcopy(self.envelope)

    def test_one_bronze_artifact_per_run(self):
        result = self.process()
        self.assertEqual(
            list((self.root / "bronze/threads").glob("*.json")), [result.bronze_path]
        )

    def test_one_silver_artifact_per_run(self):
        result = self.process()
        self.assertEqual(
            list((self.root / "silver/social_posts").glob("*.json")),
            [result.silver_path],
        )

    def test_bronze_preserves_complete_crawler_result(self):
        self.envelope["unknown_envelope"] = {"preserved": True}
        self.envelope["posts"][0]["detail"] = {
            "raw_payload": {"nested": [1, 2]},
            "visible_replies": [],
        }
        bronze, _ = self.read_artifacts(self.process())
        for key, value in self.envelope.items():
            self.assertEqual(bronze[key], value)
        self.assertEqual(bronze["schema_version"], "1")
        self.assert_contract(bronze, self.bronze_schema)

    def test_run_id_connects_bronze_silver_and_every_record(self):
        result = self.process()
        bronze, silver = self.read_artifacts(result)
        self.assertEqual(bronze["run_id"], result.run_id)
        self.assertEqual(silver["run_id"], result.run_id)
        self.assertTrue(
            all(record["run_id"] == result.run_id for record in silver["records"])
        )

    def test_collected_at_propagated_without_new_collection_times(self):
        bronze, silver = self.read_artifacts(self.process())
        self.assertEqual(bronze["collected_at"], COLLECTED_AT)
        self.assertEqual(silver["collected_at"], COLLECTED_AT)
        for record in silver["records"]:
            self.assertEqual(
                datetime.fromisoformat(record["collected_at"]),
                datetime.fromisoformat(COLLECTED_AT),
            )

    def test_existing_normalizer_is_called_for_each_post(self):
        with patch.object(
            job, "normalize_social_post", wraps=normalize_social_post
        ) as normalizer:
            result = self.process()
        self.assertEqual(normalizer.call_count, self.envelope["count"])
        for call, post in zip(normalizer.call_args_list, self.envelope["posts"]):
            self.assertEqual(call.args, (post,))
            self.assertEqual(
                call.kwargs, {"run_id": result.run_id, "collected_at": COLLECTED_AT}
            )
        _, silver = self.read_artifacts(result)
        for record in silver["records"]:
            self.assert_contract(record, self.silver_schema)

    def test_saved_sample_replays_all_ten_posts_offline(self):
        if not SAMPLE_PATH.exists():
            self.skipTest("saved Threads sample unavailable")
        original = SAMPLE_PATH.read_bytes()
        envelope = json.loads(original)
        with patch.object(
            job, "_crawl_threads", side_effect=AssertionError("unexpected crawler call")
        ):
            result = job.process_threads_envelope(
                envelope, run_id="sample-replay", artifact_root=self.root
            )
        bronze, silver = self.read_artifacts(result)
        self.assertEqual(result.count, 10)
        self.assertEqual(result.rejected_count, 0)
        self.assertEqual(result.status, "success")
        self.assertEqual(bronze["posts"], envelope["posts"])
        self.assertTrue(
            all(record["detail_status"] == "error" for record in silver["records"])
        )
        self.assert_contract(bronze, self.bronze_schema)
        for record in silver["records"]:
            self.assert_contract(record, self.silver_schema)
        self.assertEqual(SAMPLE_PATH.read_bytes(), original)

    def test_failed_detail_does_not_block_normalization(self):
        _, silver = self.read_artifacts(self.process())
        self.assertEqual(silver["records"][0]["detail_status"], "error")
        self.assertEqual(silver["records"][0]["detail_error"], "Detail unavailable")
        self.assertEqual(silver["count"], 2)

    def test_output_paths_are_run_scoped(self):
        result = self.process(run_id="observation-123")
        self.assertEqual(
            result.bronze_path, self.root / "bronze/threads/observation-123.json"
        )
        self.assertEqual(
            result.silver_path, self.root / "silver/social_posts/observation-123.json"
        )

    def test_two_runs_do_not_overwrite_artifacts(self):
        first = self.process(run_id="first")
        first_bytes = (first.bronze_path.read_bytes(), first.silver_path.read_bytes())
        second = self.process(run_id="second")
        self.assertNotEqual(first.bronze_path, second.bronze_path)
        self.assertEqual(
            first_bytes,
            (first.bronze_path.read_bytes(), first.silver_path.read_bytes()),
        )
        self.assertEqual(len(list(self.root.rglob("*.json"))), 4)

    def test_utf8_json_and_vietnamese_text_preserved(self):
        result = self.process()
        text = self.envelope["posts"][0]["text"]
        for path in (result.bronze_path, result.silver_path):
            encoded = path.read_bytes()
            self.assertIn(text.encode("utf-8"), encoded)
            json.loads(encoded.decode("utf-8"))

    def test_input_envelope_not_mutated(self):
        before = copy.deepcopy(self.envelope)
        self.process()
        self.assertEqual(self.envelope, before)
        self.assertNotIn("run_id", self.envelope)

    def test_bronze_write_failure_is_visible(self):
        with (
            patch.object(
                job, "_write_json_atomic", side_effect=OSError("Bronze write denied")
            ),
            self.assertRaisesRegex(OSError, "Bronze write denied"),
        ):
            self.process()
        self.assertFalse(list(self.root.rglob("*.json")))

    def test_silver_write_failure_keeps_bronze(self):
        writer = job._write_json_atomic

        def failing_writer(path, payload):
            if "social_posts" in path.parts:
                raise OSError("Silver write denied")
            writer(path, payload)

        with (
            patch.object(job, "_write_json_atomic", side_effect=failing_writer),
            self.assertRaisesRegex(OSError, "Silver write denied"),
        ):
            self.process()
        bronze = self.root / "bronze/threads/test-run.json"
        self.assertEqual(
            json.loads(bronze.read_text())["posts"], self.envelope["posts"]
        )
        self.assertFalse((self.root / "silver/social_posts/test-run.json").exists())

    def test_invalid_record_produces_explicit_partial_batch(self):
        self.envelope["posts"][0]["id"] = None
        result = self.process()
        bronze, silver = self.read_artifacts(result)
        self.assertEqual(bronze["posts"], self.envelope["posts"])
        self.assertEqual(result.status, "partial")
        self.assertEqual(result.count, 1)
        self.assertEqual(result.rejected_count, 1)
        self.assertEqual(silver["input_count"], 2)
        self.assertEqual(silver["normalization_errors"][0]["post_index"], 0)
        self.assertEqual(silver["normalization_errors"][0]["error_type"], "ValueError")
        self.assertIn("post.id", silver["normalization_errors"][0]["error"])
        self.assertEqual(silver["records"][0]["source_record_id"], "456")
        self.assert_contract(bronze, self.bronze_schema)

    def test_all_invalid_records_produce_failed_batch(self):
        for post in self.envelope["posts"]:
            post["id"] = None
        result = self.process()
        _, silver = self.read_artifacts(result)
        self.assertEqual(result.status, "failed")
        self.assertEqual(silver["records"], [])
        self.assertEqual(silver["rejected_count"], 2)

    def test_unexpected_normalizer_exception_is_visible_and_keeps_bronze(self):
        with (
            patch.object(
                job, "normalize_social_post", side_effect=RuntimeError("normalizer bug")
            ),
            self.assertRaisesRegex(RuntimeError, "normalizer bug"),
        ):
            self.process()
        self.assertTrue((self.root / "bronze/threads/test-run.json").exists())
        self.assertFalse((self.root / "silver/social_posts/test-run.json").exists())

    def test_tracker_run_id_reused_and_success_after_silver_commit(self):
        tracker = CrawlerStatusTracker("threads", self.root / "status.json")
        with patch.object(job, "uuid4", side_effect=AssertionError("second run ID")):
            result = job.collect_threads(
                status_tracker=tracker,
                crawler=self.tracked_crawler,
                artifact_root=self.root,
            )
        bronze, silver = self.read_artifacts(result)
        self.assertEqual(result.run_id, tracker.status.run_id)
        self.assertEqual(bronze["run_id"], tracker.status.run_id)
        self.assertEqual(silver["run_id"], tracker.status.run_id)
        self.assertEqual(tracker.status.status, "success")
        self.assertEqual(tracker.status.output_path, str(result.silver_path.resolve()))

    def test_without_tracker_run_id_generated_once(self):
        crawler = Mock(return_value=self.envelope)
        with patch.object(
            job, "uuid4", return_value=Mock(hex="generated-run")
        ) as generator:
            result = job.collect_threads(crawler=crawler, artifact_root=self.root)
        generator.assert_called_once_with()
        self.assertEqual(result.run_id, "generated-run")
        self.assertIsNone(crawler.call_args.kwargs["status_tracker"])

    def test_crawler_receives_only_existing_collection_arguments(self):
        crawler = Mock(return_value=self.envelope)
        job.collect_threads(
            query="pickleball Hà Nội",
            limit=5,
            sort="top",
            headless=False,
            timeout_ms=1234,
            per_query_cap=2,
            crawler=crawler,
            artifact_root=self.root,
        )
        crawler.assert_called_once_with(
            query="pickleball Hà Nội",
            limit=5,
            sort="top",
            headless=False,
            timeout_ms=1234,
            per_query_cap=2,
            status_tracker=None,
        )

    def test_crawler_failure_marks_status_failed_without_artifacts(self):
        tracker = CrawlerStatusTracker("threads", self.root / "status.json")

        def failed_crawler(**kwargs):
            self.tracked_crawler(**kwargs)
            raise RuntimeError("crawler failed")

        with self.assertRaisesRegex(RuntimeError, "crawler failed"):
            job.collect_threads(
                status_tracker=tracker, crawler=failed_crawler, artifact_root=self.root
            )
        self.assertEqual(tracker.status.status, "failed")
        self.assertIn("crawler failed", tracker.status.error)
        self.assertFalse((self.root / "bronze").exists())
        self.assertFalse((self.root / "silver").exists())

    def test_tracker_start_failure_cannot_fall_back_to_another_id(self):
        tracker = CrawlerStatusTracker("threads", self.root / "status.json")
        with self.assertRaisesRegex(RuntimeError, "new tracked run"):
            job.collect_threads(
                status_tracker=tracker,
                crawler=Mock(return_value=self.envelope),
                artifact_root=self.root,
            )
        self.assertEqual(tracker.status.status, "failed")
        self.assertFalse((self.root / "bronze").exists())

    def test_partial_job_marks_tracker_failed_with_artifacts_preserved(self):
        self.envelope["posts"][0]["id"] = None
        tracker = CrawlerStatusTracker("threads", self.root / "status.json")
        result = job.collect_threads(
            status_tracker=tracker,
            crawler=self.tracked_crawler,
            artifact_root=self.root,
        )
        self.assertEqual(result.status, "partial")
        self.assertEqual(tracker.status.status, "failed")
        self.assertIn("1 records rejected", tracker.status.error)
        self.assertTrue(result.bronze_path.exists())
        self.assertTrue(result.silver_path.exists())

    def test_write_failure_marks_tracker_failed(self):
        tracker = CrawlerStatusTracker("threads", self.root / "status.json")
        with (
            patch.object(
                job, "_write_json_atomic", side_effect=OSError("write denied")
            ),
            self.assertRaises(OSError),
        ):
            job.collect_threads(
                status_tracker=tracker,
                crawler=self.tracked_crawler,
                artifact_root=self.root,
            )
        self.assertEqual(tracker.status.status, "failed")
        self.assertIn("write denied", tracker.status.error)

    def test_replay_recovers_existing_bronze_after_silver_write_failure(self):
        writer = job._write_json_atomic

        def failing_writer(path, payload):
            if "social_posts" in path.parts:
                raise OSError("Silver failed")
            writer(path, payload)

        with (
            patch.object(job, "_write_json_atomic", side_effect=failing_writer),
            self.assertRaises(OSError),
        ):
            self.process()
        path = self.root / "bronze/threads/test-run.json"
        original_bytes = path.read_bytes()
        bronze = json.loads(original_bytes)
        with patch.object(
            job, "uuid4", side_effect=AssertionError("replay generated a new ID")
        ):
            result = job.process_threads_envelope(bronze, artifact_root=self.root)
        self.assertEqual(result.run_id, "test-run")
        self.assertEqual(path.read_bytes(), original_bytes)
        self.assertEqual(result.count, 2)

    def test_saved_bronze_run_cannot_be_relabelled(self):
        self.envelope["run_id"] = "original"
        with self.assertRaisesRegex(ValueError, "conflicts"):
            self.process(run_id="different")
        self.assertFalse(list(self.root.rglob("*.json")))

    def test_existing_silver_run_is_not_overwritten(self):
        first = self.process()
        original = first.silver_path.read_bytes()
        with self.assertRaises(FileExistsError):
            self.process()
        self.assertEqual(first.silver_path.read_bytes(), original)

    def test_different_existing_bronze_is_not_overwritten(self):
        path = self.root / "bronze/threads/test-run.json"
        job._write_json_atomic(path, {"different": True})
        original = path.read_bytes()
        with self.assertRaisesRegex(FileExistsError, "different Bronze"):
            self.process()
        self.assertEqual(path.read_bytes(), original)

    def test_path_traversal_run_ids_rejected_before_writes(self):
        for value in ("../escape", "/absolute", "a/b", "", "..", "a" * 129):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.process(run_id=value)
        self.assertFalse(list(self.root.rglob("*.json")))

    def test_invalid_envelope_fails_before_artifacts(self):
        for field, value in (
            ("count", 3),
            ("count", True),
            ("source", "pbplus"),
            ("collected_at", "2026-10-01"),
            ("collected_at", None),
            ("query_runs", {}),
            ("posts", [None]),
            ("schema_version", "2"),
        ):
            with self.subTest(field=field), self.assertRaises(ValueError):
                job.process_threads_envelope(
                    {**self.envelope, field: value},
                    run_id="invalid",
                    artifact_root=self.root,
                )
        self.assertFalse(list(self.root.rglob("*.json")))

    def test_atomic_writer_flushes_fsyncs_then_replaces(self):
        path = self.root / "atomic.json"
        real_replace = artifacts.os.replace
        with patch.object(artifacts.os, "fsync", wraps=artifacts.os.fsync) as fsync:

            def checked_replace(source, destination):
                fsync.assert_called_once()
                self.assertFalse(Path(destination).exists())
                self.assertEqual(
                    json.loads(Path(source).read_text(encoding="utf-8")),
                    {"text": "Hà Nội"},
                )
                real_replace(source, destination)

            with patch.object(artifacts.os, "replace", side_effect=checked_replace):
                job._write_json_atomic(path, {"text": "Hà Nội"})
        self.assertEqual(
            json.loads(path.read_text(encoding="utf-8")), {"text": "Hà Nội"}
        )
        self.assertFalse(list(self.root.glob("*.tmp")))

    def test_failed_atomic_replace_leaves_no_destination_or_temporary_file(self):
        path = self.root / "atomic.json"
        with (
            patch.object(
                artifacts.os, "replace", side_effect=OSError("replace failed")
            ),
            self.assertRaisesRegex(OSError, "replace failed"),
        ):
            job._write_json_atomic(path, {"value": 1})
        self.assertFalse(path.exists())
        self.assertEqual(list(self.root.iterdir()), [])

    def test_replay_imports_no_crawler_or_kafka_transport(self):
        original_import = __import__

        def guarded_import(name, *args, **kwargs):
            if any(word in name for word in ("playwright", "publishers", "kafka")):
                raise AssertionError(f"unexpected import: {name}")
            return original_import(name, *args, **kwargs)

        with (
            patch("builtins.__import__", side_effect=guarded_import),
            patch.object(
                job,
                "_crawl_threads",
                side_effect=AssertionError("live crawler invoked"),
            ),
        ):
            self.process()

    def test_default_artifact_root_matches_collector_layout(self):
        self.assertEqual(default_artifact_root(), COLLECTOR_ROOT / ".local")

    def test_cli_offline_replay_success_and_partial_exit_codes(self):
        source = self.root / "input.json"
        for index, expected_exit in enumerate((0, 2, 1)):
            if index >= 1:
                self.envelope["posts"][index - 1]["id"] = None
            source.write_text(
                json.dumps(self.envelope, ensure_ascii=False), encoding="utf-8"
            )
            output = io.StringIO()
            with (
                redirect_stdout(output),
                patch.object(
                    job,
                    "_crawl_threads",
                    side_effect=AssertionError("live crawler invoked"),
                ),
            ):
                actual_exit = job.main(
                    [
                        "--replay",
                        str(source),
                        "--run-id",
                        f"cli-{index}",
                        "--artifact-root",
                        str(self.root),
                    ]
                )
            self.assertEqual(actual_exit, expected_exit)
            self.assertEqual(json.loads(output.getvalue())["rejected_count"], index)

    def test_cli_failure_reports_error_and_returns_nonzero(self):
        with self.assertLogs(job.LOGGER, level="ERROR") as logs:
            code = job.main(["--replay", str(self.root / "missing.json")])
        self.assertEqual(code, 1)
        self.assertIn("failed", logs.output[0])


if __name__ == "__main__":
    unittest.main()
