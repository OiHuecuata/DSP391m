"""Cross-process writer exclusion and safe backend-owned terminal reporting."""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from orchestration.local import collection_worker as worker
from orchestration.local import pipeline


class CollectionWorkerTests(unittest.TestCase):
    def test_failed_acquisition_is_failed_even_when_refresh_succeeds(self):
        self.assertEqual(worker.cycle_status("failed", 1), "FAILED")
        self.assertEqual(worker.cycle_status("failed", 0), "FAILED")
        self.assertEqual(worker.cycle_status("partial", 1), "PARTIAL")
        self.assertEqual(worker.cycle_status("success", 1), "PARTIAL")
        self.assertEqual(worker.cycle_status("success", 0), "SUCCESS")

    def test_bronze_only_recovery_normalizes_offline_and_reuses_saved_silver(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_id = "replay-bronze"
            raw = root / "bronze/threads" / (run_id + ".json")
            raw.parent.mkdir(parents=True)
            raw.write_text(
                json.dumps(
                    {
                        "schema_version": "1",
                        "source": "threads",
                        "run_id": run_id,
                        "seed_query": "pickleball",
                        "sort": "recent",
                        "collected_at": "2026-10-08T00:00:00Z",
                        "count": 1,
                        "posts": [{"id": "123", "text": "Tìm sân Hà Nội"}],
                        "query_runs": [],
                    }
                )
            )
            original = raw.read_bytes()
            checkpoint = root / "discovery.json"
            checkpoint.write_text(
                json.dumps(
                    {
                        "run_id": run_id,
                        "exported": True,
                        "artifact_committed": False,
                    }
                )
            )
            config = SimpleNamespace(
                spark=SimpleNamespace(
                    paths=SimpleNamespace(base=(root / "lake").as_uri())
                )
            )

            def replay():
                report = {"stages": {}, "metrics": {}}
                with (
                    patch.object(pipeline, "resolve_config", return_value=config),
                    patch(
                        "dsp_collector.sources.crawler.threads.crawler.crawl_threads_posts",
                        side_effect=AssertionError("must not crawl"),
                    ),
                    patch(
                        "dsp_spark.runtime.spark.create_spark",
                        side_effect=RuntimeError("stop after normalization"),
                    ),
                    self.assertRaisesRegex(RuntimeError, "stop after normalization"),
                ):
                    worker._execute_locked(
                        "threads",
                        run_id,
                        root,
                        {"threads": {"checkpoint_path": str(checkpoint)}},
                        report,
                        lambda value: None,
                    )
                self.assertTrue(report["collection"]["reused_collection"])
                self.assertEqual(report["collection"]["count"], 1)
                self.assertTrue(json.loads(checkpoint.read_text())["artifact_committed"])

            replay()
            normalized = root / "silver/social_posts" / (run_id + ".json")
            saved = normalized.read_bytes()
            (root / "collection.json").unlink()
            replay()
            self.assertEqual(raw.read_bytes(), original)
            self.assertEqual(normalized.read_bytes(), saved)

    def test_shared_writer_lock_rejects_overlap_and_releases_after_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            config = SimpleNamespace(
                spark=SimpleNamespace(
                    paths=SimpleNamespace(base=Path(directory).as_uri())
                )
            )
            with (
                self.assertRaisesRegex(RuntimeError, "injected failure"),
                pipeline.lakehouse_lock(config),
            ):
                with (
                    self.assertRaisesRegex(
                        RuntimeError, "Another local pipeline writer"
                    ),
                    pipeline.lakehouse_lock(config),
                ):
                    self.fail("second writer entered")
                raise RuntimeError("injected failure")
            with pipeline.lakehouse_lock(config):
                self.assertTrue((Path(directory) / ".pipeline-writer.lock").exists())

    def test_disk_guard_reports_failure_before_acquisition(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            policy = state / "policy.json"
            policy.write_text('{"minimum_free_bytes": 999999999999999999}')
            with (
                patch.object(worker, "load_environment"),
                patch.object(worker, "execute") as execute,
            ):
                result = worker.main(
                    [
                        "--state",
                        str(state),
                        "--source",
                        "threads",
                        "--run-id",
                        "disk-guard",
                        "--config",
                        str(policy),
                    ]
                )
            execute.assert_not_called()
            report = json.loads((state / "runs/disk-guard/summary.json").read_text())
            self.assertEqual(result, 1)
            self.assertEqual(report["status"], "FAILED")
            self.assertIn("completed_at", report)

    def test_dependency_errors_remain_server_side_and_redacted(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            policy = state / "policy.json"
            policy.write_text('{"minimum_free_bytes": 0}')
            for stage in ("kafka", "spark_ingestion", "clickhouse", "bronze"):

                def fail(
                    source,
                    identity,
                    directory,
                    policy,
                    report,
                    update,
                    failure_stage=stage,
                ):
                    report["stage"] = failure_stage
                    raise RuntimeError("dependency failed with fixture-private-key")

                with (
                    patch.object(worker, "load_environment"),
                    patch.object(worker, "execute", side_effect=fail),
                    patch.dict(os.environ, {"YOUTUBE_API_KEY": "fixture-private-key"}),
                ):
                    result = worker.main(
                        [
                            "--state",
                            str(state),
                            "--source",
                            "youtube",
                            "--run-id",
                            stage,
                            "--config",
                            str(policy),
                        ]
                    )
                root = state / "runs" / stage
                summary = (root / "summary.json").read_text()
                self.assertEqual(result, 1)
                self.assertNotIn("fixture-private-key", summary)
                self.assertNotIn(
                    "fixture-private-key", (root / "error.txt").read_text()
                )
                self.assertIn(stage, json.loads(summary)["error"])
                self.assertEqual(
                    json.loads((root / "progress.json").read_text())["status"], "FAILED"
                )

    def test_worker_environment_propagates_module_path(self):
        with tempfile.TemporaryDirectory() as directory:
            env = Path(directory) / ".env"
            env.write_text("YOUTUBE_API_KEY=fixture-server-only\n")
            with patch.dict(os.environ, {"PIPELINE_ENV_FILE": str(env)}, clear=True):
                worker.load_environment()
                self.assertIn("pipelines/spark/src", os.environ["PYTHONPATH"])
                self.assertIn("apps/collector/src", os.environ["PYTHONPATH"])
                self.assertEqual(os.environ["YOUTUBE_API_KEY"], "fixture-server-only")
                self.assertNotIn("NEXT_PUBLIC_YOUTUBE_API_KEY", os.environ)
