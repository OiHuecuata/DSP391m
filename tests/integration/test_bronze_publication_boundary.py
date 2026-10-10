"""Finite handoff failure tests: no broker, collector or analytical writes."""

import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from dsp_collector.publishers.kafka import PublicationResult

from orchestration.local import pipeline


class BronzePublicationBoundaryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="dsp-bronze-boundary-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.config = SimpleNamespace(spark=object(), kafka=object())
        self.collections = {
            source: {
                "run_id": source + "-synthetic-run",
                "status": "success",
                "count": 1,
                "bronze_path": self.root / (source + "-raw.json"),
                "silver_path": self.root / (source + "-normalized.json"),
            }
            for source in ("threads", "pbplus")
        }
        self.events = []
        self.spark = MagicMock()
        self.summary = {}
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(
            patch.object(pipeline, "ensure_infrastructure", return_value={})
        )
        self.stack.enter_context(
            patch.object(
                pipeline,
                "collect",
                side_effect=lambda _config, source, _dir: self.collections[source],
            )
        )
        self.runtime = self.stack.enter_context(
            patch("dsp_spark.runtime.spark.create_spark", return_value=self.spark)
        )
        self.archive = self.stack.enter_context(
            patch(
                "dsp_spark.jobs.batch.ingest_bronze.ingest_bronze",
                side_effect=self.record_archive,
            )
        )
        self.publish = self.stack.enter_context(
            patch(
                "dsp_collector.publishers.kafka.publish_social_batch",
                side_effect=self.record_publication,
            )
        )
        self.courts = self.stack.enter_context(
            patch("dsp_spark.jobs.batch.ingest_pbplus.ingest_pbplus")
        )
        self.stream = self.stack.enter_context(
            patch(
                "dsp_spark.jobs.streaming.social_kafka_to_delta.start_social_stream",
                side_effect=RuntimeError("stop_after_handoff"),
            )
        )

    def record_archive(self, _spark, artifact, _settings, *, source):
        self.events.append("bronze:" + source)
        self.assertEqual(artifact, self.collections[source]["bronze_path"])

    def record_publication(self, artifact, *, settings):
        self.events.append("kafka")
        self.assertEqual(artifact, self.collections["threads"]["silver_path"])
        self.assertIs(settings, self.config.kafka)
        return PublicationResult(
            "threads-synthetic-run", "synthetic.topic", "success", 1, 1, 0, []
        )

    def execute(self):
        # Existing ownership wrapper is tested separately; focus on the handoff.
        return pipeline._execute_locked(self.config, self.summary, self.root)

    def test_spark_startup_failure_cannot_publish_unarchived_events(self):
        self.runtime.side_effect = OSError("injected_runtime_failure")
        with self.assertRaises(OSError):
            self.execute()
        self.publish.assert_not_called()
        self.archive.assert_not_called()
        self.stream.assert_not_called()

    def test_bronze_commit_or_readback_failure_cannot_publish_events(self):
        self.archive.side_effect = ValueError("Bronze run/envelope verification failed")
        with self.assertRaises(ValueError):
            self.execute()
        self.publish.assert_not_called()
        self.stream.assert_not_called()
        self.courts.assert_not_called()
        self.spark.stop.assert_called_once()

    def test_all_bronze_inputs_precede_publication_without_changing_handoff(self):
        with self.assertRaisesRegex(RuntimeError, "stop_after_handoff"):
            self.execute()
        self.assertEqual(self.events, ["bronze:threads", "bronze:pbplus", "kafka"])
        self.publish.assert_called_once()
        self.assertEqual(self.summary["publication"]["published_count"], 1)
        self.spark.stop.assert_called_once()

    def test_publication_failure_preserves_bronze_and_stops_runtime(self):
        self.publish.side_effect = OSError("injected_publish_failure")
        with self.assertRaises(OSError):
            self.execute()
        self.assertEqual(self.events, ["bronze:threads", "bronze:pbplus"])
        self.stream.assert_not_called()
        self.spark.stop.assert_called_once()


if __name__ == "__main__":
    unittest.main()
