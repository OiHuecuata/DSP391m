"""Run configuration and replay handoff boundaries; no Airflow internals mocked."""

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "orchestration/airflow"))

from run_job import MODULES, collect, execute
from runtime import RunConfig, read_collection, write_report


class RuntimeTests(unittest.TestCase):
    def test_configuration_defaults_overrides_and_safe_paths(self):
        with patch.dict(
            "os.environ",
            {
                "DSP_AIRFLOW_RUN_ID": "manual/path:with punctuation",
                "DSP_AIRFLOW_PBPLUS_REPLAY": "saved bronze.json",
            },
            clear=True,
        ):
            config = RunConfig.from_env(ROOT)
            self.assertEqual(config.mode, "replay")
            self.assertEqual(config.replay, ROOT / "saved bronze.json")
            self.assertEqual(config.directory.parent, ROOT / ".local/airflow/runs")
            self.assertEqual(len(config.directory.name), 64)
        with patch.dict(
            "os.environ",
            {
                "DSP_AIRFLOW_RUN_ID": "manual",
                "DSP_AIRFLOW_RUN_CONF": json.dumps({"pbplus_mode": "live"}),
            },
            clear=True,
        ):
            self.assertEqual(RunConfig.from_env().mode, "live")

    def test_invalid_configuration_fails_clearly(self):
        for conf in (
            {},
            {"pbplus_mode": "other"},
            {"pbplus_mode": "live", "replay_path": "a"},
            {"pbplus_mode": "live", "unexpected": "x"},
            {"replay_path": 1},
        ):
            with (
                self.subTest(conf=conf),
                patch.dict(
                    "os.environ",
                    {
                        "DSP_AIRFLOW_RUN_ID": "manual",
                        "DSP_AIRFLOW_RUN_CONF": json.dumps(conf),
                    },
                    clear=True,
                ),
                self.assertRaises(ValueError),
            ):
                RunConfig.from_env()

    def test_successful_collection_manifest_is_reused_and_drift_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            replay = root / "input.json"
            replay.write_text("{}")
            config = RunConfig(root, "run", "replay", replay, None)
            config.directory.mkdir(parents=True)
            bronze, silver = (
                config.directory / "bronze.json",
                config.directory / "silver.json",
            )
            bronze.write_text("{}")
            silver.write_text("{}")
            result = {
                "status": "success",
                "bronze_path": str(bronze),
                "silver_path": str(silver),
            }
            write_report(
                config.directory / "collection.json",
                {
                    "inputs": config.input_identity(),
                    "result": result,
                    "hashes": {
                        "bronze_path": hashlib.sha256(bronze.read_bytes()).hexdigest(),
                        "silver_path": hashlib.sha256(silver.read_bytes()).hexdigest(),
                    },
                },
            )
            with patch("run_job.invoke") as invoke:
                self.assertEqual(collect(config), result)
                invoke.assert_not_called()
            silver.write_text("modified")
            with self.assertRaisesRegex(ValueError, "artifact changed"):
                read_collection(config)
            replay.write_text("modified input")
            with self.assertRaisesRegex(ValueError, "inputs changed"):
                read_collection(config)

    def test_module_commands_reuse_existing_entry_points(self):
        config = RunConfig(ROOT, "run", "live", None, None)
        self.assertEqual(
            MODULES["load_market_signals_clickhouse"],
            ("dsp_spark.jobs.batch.load_market_signals_clickhouse",),
        )
        for stage, command in MODULES.items():
            with self.subTest(stage=stage), patch("run_job.invoke") as invoke:
                execute(stage, config)
                invoke.assert_called_once_with(config, *command)
