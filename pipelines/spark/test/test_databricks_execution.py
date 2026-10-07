"""Offline Databricks configuration and managed-session handoff tests."""

import importlib.util
import io
import json
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "pipelines/spark/src"), str(ROOT / "apps/collector/src")]


def load(name, filename):
    spec = importlib.util.spec_from_file_location(
        name, ROOT / "infra/databricks" / filename
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


submit = load("submit_gold", "submit_social_gold.py")
runner = load("run_gold", "run_social_activity_daily.py")

CONFIG = {
    "DATABRICKS_CLUSTER_ID": "offline-fixture-cluster",
    "DSP_DATABRICKS_WHEEL_URI": "/Workspace/fixtures/dsp391_spark_jobs-0.1.0-py3-none-any.whl",
    "DSP_LAKEHOUSE_BASE_URI": "s3a://offline-fixture/lake",
}


class DatabricksExecutionTests(unittest.TestCase):
    def test_market_template_paths_window_and_explicit_end(self):
        config = {
            **CONFIG,
            "DSP_DELTA_SOCIAL_ACTIVITY_DAILY_PATH": "s3a://offline-fixture/input/activity",
            "DSP_DELTA_COURT_SUPPLY_BY_LOCATION_PATH": "s3a://offline-fixture/input/supply",
            "DSP_DELTA_LOCATION_MARKET_SIGNAL_SNAPSHOT_PATH": "s3a://offline-fixture/output/market",
            "DSP_MARKET_SIGNAL_WINDOW_DAYS": "7",
            "DSP_MARKET_SIGNAL_END_DATE": "2026-10-05",
        }
        task = submit.render_job(config, "location_market_signal_snapshot")["tasks"][0]
        self.assertEqual(
            task["python_wheel_task"]["entry_point"], "location_market_signal_snapshot"
        )
        params = task["python_wheel_task"]["parameters"]
        for key in (
            "DSP_DELTA_SOCIAL_ACTIVITY_DAILY_PATH",
            "DSP_DELTA_COURT_SUPPLY_BY_LOCATION_PATH",
            "DSP_DELTA_LOCATION_MARKET_SIGNAL_SNAPSHOT_PATH",
        ):
            self.assertIn(config[key], params)
        self.assertEqual(params[params.index("--window-days") + 1], "7")
        self.assertEqual(params[-2:], ["--end-date", "2026-10-05"])
        for changes in (
            {"DSP_MARKET_SIGNAL_WINDOW_DAYS": "0"},
            {"DSP_MARKET_SIGNAL_END_DATE": "2026-02-30"},
            {"DSP_DELTA_COURT_SUPPLY_BY_LOCATION_PATH": "file:///tmp/supply"},
            {
                "DSP_DELTA_LOCATION_MARKET_SIGNAL_SNAPSHOT_PATH": config[
                    "DSP_DELTA_SOCIAL_ACTIVITY_DAILY_PATH"
                ]
            },
        ):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                submit.render_job(
                    {**config, **changes}, "location_market_signal_snapshot"
                )

    def test_managed_market_job_reuses_session_and_passes_window(self):
        spark = Mock(version="4.0.2")
        session = Mock()
        session.builder.getOrCreate.return_value = spark
        with (
            patch.dict(
                "os.environ",
                {"DATABRICKS_RUNTIME_VERSION": "offline-fixture"},
                clear=True,
            ),
            patch.object(runner, "SparkSession", session),
            patch.object(runner, "market_gold_main") as job,
        ):
            runner.market_signal_main(
                [
                    "--lakehouse-base-uri",
                    CONFIG["DSP_LAKEHOUSE_BASE_URI"],
                    "--window-days",
                    "7",
                    "--end-date",
                    "2026-10-05",
                ]
            )
        self.assertIs(job.call_args.kwargs["spark"], spark)
        self.assertEqual(
            job.call_args.kwargs["settings"].paths.location_market_signal_snapshot,
            "s3a://offline-fixture/lake/gold/location_market_signal_snapshot",
        )
        self.assertEqual(job.call_args.kwargs["market_settings"].window_days, 7)
        self.assertEqual(job.call_args.kwargs["market_settings"].end_date, "2026-10-05")
        spark.stop.assert_not_called()
        session.builder.master.assert_not_called()

    def test_job_targets_existing_compute_and_same_package(self):
        job = submit.render_job(CONFIG)
        task = job["tasks"][0]
        self.assertEqual(task["existing_cluster_id"], CONFIG["DATABRICKS_CLUSTER_ID"])
        self.assertNotIn("new_cluster", task)
        self.assertEqual(
            task["python_wheel_task"]["entry_point"], "social_activity_daily"
        )
        self.assertEqual(
            task["libraries"], [{"whl": CONFIG["DSP_DATABRICKS_WHEEL_URI"]}]
        )
        self.assertIn(
            "s3a://offline-fixture/lake/gold/social_activity_daily",
            task["python_wheel_task"]["parameters"],
        )

    def test_court_supply_template_and_quality_path_overrides(self):
        config = {
            **CONFIG,
            "DSP_DELTA_COURTS_PATH": "s3a://offline-fixture/custom/courts",
            "DSP_DELTA_COURT_SUPPLY_BY_LOCATION_PATH": "s3a://offline-fixture/custom/supply",
            "DSP_DELTA_UNRESOLVED_COURT_LOCATIONS_PATH": "s3a://offline-fixture/custom/quality",
        }
        task = submit.render_job(config, "court_supply_by_location")["tasks"][0]
        self.assertEqual(
            task["python_wheel_task"]["entry_point"], "court_supply_by_location"
        )
        for key in (
            "DSP_DELTA_COURTS_PATH",
            "DSP_DELTA_COURT_SUPPLY_BY_LOCATION_PATH",
            "DSP_DELTA_UNRESOLVED_COURT_LOCATIONS_PATH",
        ):
            self.assertIn(config[key], task["python_wheel_task"]["parameters"])
        with self.assertRaisesRegex(ValueError, "S3A"):
            submit.render_job(
                {
                    **CONFIG,
                    "DSP_DELTA_UNRESOLVED_COURT_LOCATIONS_PATH": "file:///tmp/quality",
                },
                "court_supply_by_location",
            )

    def test_managed_court_supply_uses_same_job_without_stopping_session(self):
        spark = Mock(version="4.0.2")
        session = Mock()
        session.builder.getOrCreate.return_value = spark
        with (
            patch.dict(
                "os.environ",
                {"DATABRICKS_RUNTIME_VERSION": "offline-fixture"},
                clear=True,
            ),
            patch.object(runner, "SparkSession", session),
            patch.object(runner, "court_gold_main") as job,
        ):
            runner.court_supply_main(
                ["--lakehouse-base-uri", CONFIG["DSP_LAKEHOUSE_BASE_URI"]]
            )
        self.assertIs(job.call_args.kwargs["spark"], spark)
        self.assertEqual(
            job.call_args.kwargs["settings"].paths.court_supply_by_location,
            "s3a://offline-fixture/lake/gold/court_supply_by_location",
        )
        spark.stop.assert_not_called()
        session.builder.master.assert_not_called()

    def test_explicit_path_overrides(self):
        job = submit.render_job(
            {
                **CONFIG,
                "DSP_DELTA_SOCIAL_POSTS_PATH": "s3a://offline-fixture/other/silver",
                "DSP_DELTA_SOCIAL_ACTIVITY_DAILY_PATH": "s3a://offline-fixture/other/gold",
            }
        )
        params = job["tasks"][0]["python_wheel_task"]["parameters"]
        self.assertIn("s3a://offline-fixture/other/silver", params)
        self.assertIn("s3a://offline-fixture/other/gold", params)

    def test_missing_config_and_local_cloud_targets_rejected(self):
        with self.assertRaisesRegex(ValueError, "Missing configuration"):
            submit.render_job({})
        with self.assertRaisesRegex(ValueError, "S3A"):
            submit.render_job({**CONFIG, "DSP_LAKEHOUSE_BASE_URI": "file:///tmp/lake"})
        with self.assertRaisesRegex(ValueError, "distinct"):
            submit.render_job(
                {
                    **CONFIG,
                    "DSP_DELTA_SOCIAL_ACTIVITY_DAILY_PATH": "s3a://offline-fixture/lake/silver/social_posts",
                }
            )

    def test_wheel_uri_secrets_and_unsupported_locations_rejected(self):
        for uri in (
            "dbfs:/old-root/file.whl",
            "https://user:secret@host/file.whl",
            "/Workspace/file.whl?token=secret",
            "/Workspace/file.txt",
        ):
            with self.subTest(uri=uri), self.assertRaises(ValueError):
                submit.render_job({**CONFIG, "DSP_DATABRICKS_WHEEL_URI": uri})

    def test_dry_run_needs_no_cli_or_credentials_and_prints_no_token(self):
        output, errors = io.StringIO(), io.StringIO()
        with (
            patch.dict(
                "os.environ",
                {**CONFIG, "DATABRICKS_TOKEN": "secret-test-sentinel"},
                clear=True,
            ),
            redirect_stdout(output),
            redirect_stderr(errors),
            patch.object(
                submit.subprocess,
                "run",
                side_effect=AssertionError("dry run contacted CLI"),
            ),
        ):
            code = submit.main(["--dry-run"])
        self.assertEqual(code, 0)
        self.assertIn("no remote", errors.getvalue())
        self.assertNotIn("secret-test-sentinel", output.getvalue() + errors.getvalue())
        self.assertEqual(json.loads(output.getvalue()), submit.render_job(CONFIG))

    def test_submission_missing_auth_fails_honestly(self):
        errors = io.StringIO()
        with patch.dict("os.environ", CONFIG, clear=True), redirect_stderr(errors):
            self.assertEqual(submit.main([]), 1)
        self.assertIn("DATABRICKS_HOST", errors.getvalue())

    def test_submission_requires_cli(self):
        errors = io.StringIO()
        with (
            patch.dict(
                "os.environ",
                {**CONFIG, "DATABRICKS_CONFIG_PROFILE": "offline"},
                clear=True,
            ),
            redirect_stderr(errors),
            patch.object(submit.shutil, "which", return_value=None),
        ):
            self.assertEqual(submit.main([]), 1)
        self.assertIn("CLI", errors.getvalue())

    def test_submission_uses_json_file_and_profile_without_secrets(self):
        def invoke(command, *, check):
            self.assertTrue(check)
            self.assertEqual(command[:3], ["databricks", "jobs", "submit"])
            self.assertIn("--no-wait", command)
            self.assertEqual(command[-2:], ["--profile", "offline-profile"])
            body = command[command.index("--json") + 1]
            self.assertEqual(
                json.loads(Path(body[1:]).read_text()), submit.render_job(CONFIG)
            )
            self.assertNotIn("secret-test-sentinel", " ".join(command))

        with (
            patch.dict(
                "os.environ",
                {
                    **CONFIG,
                    "DATABRICKS_CONFIG_PROFILE": "offline-profile",
                    "DATABRICKS_TOKEN": "secret-test-sentinel",
                },
                clear=True,
            ),
            redirect_stdout(io.StringIO()),
            patch.object(submit.shutil, "which", return_value="/mock/databricks"),
            patch.object(submit.subprocess, "run", side_effect=invoke) as run,
        ):
            self.assertEqual(submit.main([]), 0)
        run.assert_called_once()

    def test_managed_wrapper_hands_same_job_platform_session(self):
        spark = Mock(version="4.0.2")
        session = Mock()
        session.builder.getOrCreate.return_value = spark
        with (
            patch.dict(
                "os.environ",
                {"DATABRICKS_RUNTIME_VERSION": "offline-fixture"},
                clear=True,
            ),
            patch.object(runner, "SparkSession", session),
            patch.object(runner, "gold_main") as job,
        ):
            runner.main(["--lakehouse-base-uri", CONFIG["DSP_LAKEHOUSE_BASE_URI"]])
        self.assertIs(job.call_args.kwargs["spark"], spark)
        self.assertEqual(
            job.call_args.kwargs["settings"].paths.social_activity_daily,
            "s3a://offline-fixture/lake/gold/social_activity_daily",
        )
        spark.stop.assert_not_called()
        spark.conf.set.assert_called_once_with("spark.sql.session.timeZone", "UTC")
        session.builder.master.assert_not_called()

    def test_managed_wrapper_rejects_local_runtime_and_old_spark(self):
        with (
            patch.dict("os.environ", {}, clear=True),
            self.assertRaisesRegex(RuntimeError, "managed"),
        ):
            runner.main(["--lakehouse-base-uri", CONFIG["DSP_LAKEHOUSE_BASE_URI"]])
        session = Mock()
        session.builder.getOrCreate.return_value = Mock(version="3.5.0")
        with (
            patch.dict(
                "os.environ",
                {"DATABRICKS_RUNTIME_VERSION": "offline-fixture"},
                clear=True,
            ),
            patch.object(runner, "SparkSession", session),
            self.assertRaisesRegex(RuntimeError, "Spark 4.0"),
        ):
            runner.main(["--lakehouse-base-uri", CONFIG["DSP_LAKEHOUSE_BASE_URI"]])


if __name__ == "__main__":
    unittest.main()
