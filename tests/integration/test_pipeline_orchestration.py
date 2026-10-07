"""Configuration, collection handoff and failure policy without service mocks."""

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "local_pipeline", ROOT / "orchestration/local/pipeline.py"
)
pipeline = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = pipeline
spec.loader.exec_module(pipeline)
FIXTURES = ROOT / "tests/fixtures/pipeline"


class PipelineOrchestrationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="dsp-pipeline-control-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        environment = patch.dict(
            os.environ,
            {
                "PIPELINE_MODE": "replay",
                "THREADS_REPLAY_PATH": str(FIXTURES / "threads.json"),
                "PBPLUS_REPLAY_PATH": str(FIXTURES / "pbplus.json"),
                "PIPELINE_STATE_DIR": str(self.root),
            },
            clear=True,
        )
        environment.start()
        self.addCleanup(environment.stop)

    def test_one_namespace_resolves_both_producer_and_consumer(self):
        os.environ["DSP_KAFKA_SOCIAL_POST_TOPIC"] = "test.shared.v1"
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "127.0.0.1:9092"
        config = pipeline.resolve_config()
        self.assertEqual(config.kafka.social_post_topic, config.spark.social_post_topic)
        self.assertEqual(
            config.kafka.bootstrap_servers, config.spark.kafka_bootstrap_servers
        )
        self.assertEqual(os.environ["KAFKA_SOCIAL_POST_TOPIC"], "test.shared.v1")
        self.assertEqual(os.environ["PYSPARK_PYTHON"], sys.executable)

    def test_conflicting_topic_fails_before_collection(self):
        os.environ.update(
            KAFKA_SOCIAL_POST_TOPIC="producer.v1",
            DSP_KAFKA_SOCIAL_POST_TOPIC="consumer.v1",
        )
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            pipeline.resolve_config()

    def test_conflicting_bootstrap_fails_before_collection(self):
        os.environ.update(
            KAFKA_BOOTSTRAP_SERVERS="localhost:9092",
            DSP_KAFKA_BOOTSTRAP_SERVERS="kafka:29092",
        )
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            pipeline.resolve_config()

    def test_container_hostname_is_rejected_for_host_execution(self):
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "kafka:29092"
        with self.assertRaisesRegex(ValueError, "Host-side"):
            pipeline.resolve_config()

    def test_cloud_paths_are_rejected_for_local_workflow(self):
        os.environ["DSP_LAKEHOUSE_BASE_URI"] = "s3a://fixture/lake"
        with self.assertRaisesRegex(ValueError, "local file"):
            pipeline.resolve_config()

    def test_replay_inputs_are_required_and_live_cannot_use_them(self):
        del os.environ["THREADS_REPLAY_PATH"]
        with self.assertRaisesRegex(ValueError, "THREADS_REPLAY_PATH"):
            pipeline.resolve_config()
        os.environ["PIPELINE_MODE"] = "live"
        with self.assertRaisesRegex(ValueError, "Live mode"):
            pipeline.resolve_config()

    def test_partial_policy_and_exit_codes(self):
        pipeline.collection_status({"status": "success", "count": 0}, 0)
        pipeline.collection_status({"status": "partial", "count": 1}, 2)
        for result, code in (
            ({"status": "partial", "count": 0}, 2),
            ({"status": "failed", "count": 0}, 1),
            ({"status": "success", "count": 1}, 2),
        ):
            with self.assertRaises(ValueError):
                pipeline.collection_status(result, code)

    def collection(self, source="threads"):
        config = pipeline.resolve_config()
        result = pipeline.collect(config, source, self.root / "run")
        directory = self.root / "replay" / source / config.replay[source]["sha256"]
        manifest = json.loads((directory / "manifest.json").read_text())
        return config, result, directory, manifest

    def test_actual_replay_cli_and_cached_artifacts_are_immutable(self):
        config, result, _, _ = self.collection()
        original = Path(result["silver_path"]).read_bytes()
        second = pipeline.collect(config, "threads", self.root / "another-run")
        self.assertEqual(second, result)
        self.assertEqual(Path(second["silver_path"]).read_bytes(), original)

    def test_actual_partial_collection_preserves_usable_records(self):
        payload = json.loads((FIXTURES / "threads.json").read_text())
        payload["posts"].append({"id": None})
        payload["count"] += 1
        path = self.root / "partial.json"
        path.write_text(json.dumps(payload))
        os.environ["THREADS_REPLAY_PATH"] = str(path)
        _, result, _, _ = self.collection()
        self.assertEqual(
            (result["status"], result["count"], result["rejected_count"]),
            ("partial", 2, 1),
        )

    def test_actual_valid_empty_collection_succeeds(self):
        os.environ["THREADS_REPLAY_PATH"] = str(FIXTURES / "threads-empty.json")
        _, result, _, _ = self.collection()
        self.assertEqual((result["status"], result["count"]), ("success", 0))

    def test_artifact_hash_mutation_stops_handoff(self):
        config, result, _, _ = self.collection()
        Path(result["silver_path"]).write_text("{}")
        with self.assertRaisesRegex(ValueError, "hash changed"):
            pipeline.collect(config, "threads", self.root / "another-run")

    def test_rehashed_wrong_run_still_stops_handoff(self):
        _, result, directory, manifest = self.collection()
        path = Path(result["silver_path"])
        payload = json.loads(path.read_text())
        payload["run_id"] = "wrong-run"
        path.write_text(json.dumps(payload))
        manifest["hashes"]["silver_path"] = pipeline.digest(path)
        with self.assertRaisesRegex(ValueError, "lineage mismatch"):
            pipeline.check_artifacts("threads", manifest, directory)

    def test_count_mismatch_stops_handoff(self):
        _, _, directory, manifest = self.collection()
        manifest["result"]["count"] += 1
        with self.assertRaisesRegex(ValueError, "counts/status"):
            pipeline.check_artifacts("threads", manifest, directory)

    def test_input_changed_after_resolution_stops_replay(self):
        path = self.root / "input.json"
        path.write_bytes((FIXTURES / "threads.json").read_bytes())
        os.environ["THREADS_REPLAY_PATH"] = str(path)
        config = pipeline.resolve_config()
        path.write_text("{}")
        with self.assertRaisesRegex(ValueError, "changed after preflight"):
            pipeline.collect(config, "threads", self.root / "run")

    def test_artifact_outside_collection_directory_is_rejected(self):
        _, _, _, manifest = self.collection()
        with self.assertRaisesRegex(ValueError, "outside"):
            pipeline.check_artifacts("threads", manifest, self.root / "elsewhere")

    def test_failure_emits_json_and_nonzero_exit(self):
        os.environ.update(
            KAFKA_SOCIAL_POST_TOPIC="one.v1", DSP_KAFKA_SOCIAL_POST_TOPIC="two.v1"
        )
        from contextlib import redirect_stdout
        from io import StringIO

        output = StringIO()
        with redirect_stdout(output):
            code = pipeline.main([])
        result = json.loads(output.getvalue())
        self.assertEqual(code, 1)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["stage"], "configuration")

    def test_failure_reports_mode_resolved_from_environment_file(self):
        path = self.root / "live.env"
        path.write_text("PIPELINE_MODE=live\n")
        del os.environ["PIPELINE_MODE"]
        os.environ["PIPELINE_ENV_FILE"] = str(path)
        from contextlib import redirect_stdout
        from io import StringIO

        output = StringIO()
        with redirect_stdout(output):
            code = pipeline.main([])
        self.assertEqual(code, 1)  # Live must not use the configured replay paths.
        self.assertEqual(json.loads(output.getvalue())["mode"], "live")


if __name__ == "__main__":
    unittest.main()
