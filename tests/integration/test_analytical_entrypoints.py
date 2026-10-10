"""CLI ownership with real locks and mocked write/runtime boundaries."""

import importlib.util
import io
import json
import os
import select
import subprocess
import sys
import tempfile
import unittest
import zipfile
from contextlib import nullcontext, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [
    str(ROOT),
    str(ROOT / "pipelines/spark/src"),
    str(ROOT / "apps/collector/src"),
]

from dsp_spark.jobs.batch import build_court_supply_by_location as court
from dsp_spark.jobs.batch import build_location_market_signal_snapshot as market
from dsp_spark.jobs.batch import build_social_activity_daily as social
from dsp_spark.jobs.batch import ingest_bronze as bronze
from dsp_spark.jobs.batch import ingest_observations as native
from dsp_spark.jobs.batch import ingest_pbplus as silver
from dsp_spark.jobs.batch import load_market_signals_clickhouse as loader
from dsp_spark.jobs.batch import smoke_clickhouse as smoke
from dsp_spark.jobs.streaming import social_kafka_to_delta as stream
from dsp_spark.runtime.clickhouse import ClickHouseSettings
from dsp_spark.runtime.config import LakehousePaths, MarketSignalSettings, SparkSettings
from dsp_spark.runtime.ownership import job_ownership

from orchestration.local import analytical_refresh as refresh
from orchestration.local import collection_worker as worker
from orchestration.local import pipeline

CASES = (
    (bronze, "ingest_bronze", ["--source", "threads", "--artifact", "fixture.json"]),
    (silver, "ingest_pbplus", ["--artifact", "fixture.json"]),
    (social, "build_social_activity_daily", []),
    (court, "build_court_supply_by_location", []),
    (market, "build_location_market_signal_snapshot", []),
    (stream, "start_social_stream", ["--available-now"]),
)

CHILD = """
import sys
from unittest.mock import Mock
from dsp_spark.jobs.batch import build_social_activity_daily as job
from dsp_spark.runtime.config import LakehousePaths, SparkSettings
settings=SparkSettings(LakehousePaths(sys.argv[1]))
job.create_spark=lambda *a,**k: Mock()
def hold(*a,**k):
    print('HELD',flush=True)
    sys.stdin.readline()
    return {'status':'success'}
job.build_social_activity_daily=hold
job.main(settings=settings)
"""


class AnalyticalEntrypointTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="dsp-t02a-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.settings = SparkSettings(LakehousePaths((self.root / "lake").as_uri()))
        self.connection = ClickHouseSettings(
            database="fixture_" + self.root.name.replace("-", "_")
        )
        self.config = SimpleNamespace(spark=self.settings, warehouse=self.connection)

    def assert_blocked(self, operation):
        with self.assertRaisesRegex(RuntimeError, "Another local pipeline writer"):
            operation()

    def call(self, module, argv, *, settings=None, ownership=None):
        kwargs = {"settings": settings or self.settings}
        if ownership is not None:
            kwargs["ownership"] = ownership
        if module is market:
            kwargs["market_settings"] = MarketSignalSettings(end_date="2026-10-05")
        with redirect_stdout(io.StringIO()):
            return module.main(argv, **kwargs)

    def test_all_spark_write_clis_fail_before_runtime_or_first_write(self):
        with pipeline.lakehouse_lock(self.config):
            for module, name, argv in CASES:
                with (
                    self.subTest(module=module.__name__),
                    patch.object(module, "create_spark") as runtime,
                    patch.object(module, name) as write,
                ):
                    self.assert_blocked(
                        lambda module=module, argv=argv: self.call(module, argv)
                    )
                    runtime.assert_not_called()
                    write.assert_not_called()

    def test_native_cli_reuses_shared_ownership_before_runtime(self):
        artifact = self.root / "native-source.json"
        artifact.write_text('{"source":"threads"}')
        with (
            pipeline.lakehouse_lock(self.config),
            patch.object(native, "create_spark") as runtime,
        ):
            self.assert_blocked(
                lambda: native.main(
                    ["--artifact", str(artifact)], settings=self.settings
                )
            )
            runtime.assert_not_called()
        with (
            pipeline.lakehouse_lock(self.config) as ownership,
            patch.object(native, "create_spark", return_value=Mock()) as runtime,
            patch.object(
                native, "ingest_observations", return_value={"status": "fixture"}
            ) as write,
        ):
            native.main(
                ["--artifact", str(artifact)],
                settings=self.settings,
                ownership=ownership,
            )
            write.assert_called_once()
            runtime.return_value.stop.assert_called_once()

    def test_shared_native_path_conflicts_even_with_different_base(self):
        other = SparkSettings(
            LakehousePaths(
                (self.root / "other").as_uri(),
                record_observations=self.settings.paths.record_observations,
            )
        )
        artifact = self.root / "native-source.json"
        artifact.write_text('{"source":"pbplus"}')
        with (
            pipeline.lakehouse_lock(self.config),
            patch.object(native, "create_spark") as runtime,
        ):
            self.assert_blocked(
                lambda: native.main(["--artifact", str(artifact)], settings=other)
            )
            runtime.assert_not_called()

    def test_all_batch_cli_operations_hold_ownership_and_stop_owned_runtime(self):
        for module, name, argv in CASES[:-1]:
            with self.subTest(module=module.__name__):

                def write(*args, **kwargs):
                    self.assert_blocked(
                        lambda: pipeline.execute(self.config, {}, self.root)
                    )
                    return {"status": "success"}

                spark = Mock()
                with (
                    patch.object(module, "create_spark", return_value=spark),
                    patch.object(module, name, side_effect=write) as operation,
                ):
                    self.call(module, argv)
                    operation.assert_called_once()
                    spark.stop.assert_called_once()

    def test_loader_and_migration_fail_before_client_construction(self):
        with pipeline.lakehouse_lock(self.config):
            for argv in ([], ["--migrate-only"]):
                with (
                    self.subTest(argv=argv),
                    patch.object(loader, "ClickHouseClient") as client,
                    patch.object(loader, "create_spark") as runtime,
                    patch.object(loader, "migrate") as migrate,
                    patch.object(loader, "replace_serving") as publish,
                ):
                    self.assert_blocked(
                        lambda argv=argv: loader.main(
                            argv, settings=self.settings, connection=self.connection
                        )
                    )
                    for op in (client, runtime, migrate, publish):
                        op.assert_not_called()

    def test_loader_holds_database_guard_through_publish_and_cleanup(self):
        client, spark = Mock(), Mock()
        client.__enter__ = Mock(return_value=client)
        client.__exit__ = Mock(return_value=False)

        def publish(*args):
            self.assert_blocked(lambda: pipeline.execute(self.config, {}, self.root))
            return {"status": "success"}

        with (
            patch.object(loader, "ClickHouseClient", return_value=client),
            patch.object(loader, "create_spark", return_value=spark),
            patch.object(loader, "read_gold_rows", return_value=[]),
            patch.object(loader, "replace_serving", side_effect=publish),
            redirect_stdout(io.StringIO()),
        ):
            loader.main([], settings=self.settings, connection=self.connection)
        spark.stop.assert_called_once()
        with pipeline.lakehouse_lock(self.config):
            pass

    def test_migrate_only_does_not_load_spark_configuration(self):
        with (
            patch.object(loader, "ClickHouseClient"),
            patch.object(loader, "migrate") as migrate,
            patch.object(
                loader.SparkSettings,
                "from_env",
                side_effect=AssertionError("not needed"),
            ),
            redirect_stdout(io.StringIO()),
        ):
            loader.main(["--migrate-only"], connection=self.connection)
        migrate.assert_called_once()

    def test_smoke_external_guard_precedes_real_and_fixture_writes(self):
        with (
            pipeline.lakehouse_lock(self.config),
            patch.object(smoke, "_run_smoke") as operation,
        ):
            self.assert_blocked(
                lambda: smoke.main(settings=self.settings, connection=self.connection)
            )
            operation.assert_not_called()

        def body(*args):
            self.assert_blocked(lambda: pipeline.execute(self.config, {}, self.root))
            return "done"

        with patch.object(smoke, "_run_smoke", side_effect=body):
            self.assertEqual(
                smoke.main(settings=self.settings, connection=self.connection), "done"
            )

    def test_worker_and_finite_runner_still_contend(self):
        with (
            pipeline.lakehouse_lock(self.config),
            patch.object(pipeline, "resolve_config", return_value=self.config),
            patch.object(worker, "_execute_locked") as write,
            patch.object(pipeline, "_execute_locked") as run,
        ):
            self.assert_blocked(
                lambda: worker.execute(
                    "threads", "run", self.root, {}, {}, lambda _: None
                )
            )
            self.assert_blocked(lambda: pipeline.execute(self.config, {}, self.root))
            write.assert_not_called()
            run.assert_not_called()

    def test_smoke_fixture_delta_database_and_cleanup_are_guarded(self):
        source = json.loads(
            (ROOT / "tests/fixtures/pipeline/gold-baseline.v1.json").read_text()
        )["tables"]["location_market_signal_snapshot"]["rows"]
        marker = self.root / "apps/backend/go.mod"
        marker.parent.mkdir(parents=True)
        marker.write_text("fixture only")
        test = self

        class Client:
            def __init__(self, settings):
                self.settings = settings
                self.rows = []

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def read_rows(self):
                return self.rows

            def query(self, sql):
                return [{"rows": len(self.rows), "identities": len(self.rows)}]

            def execute(self, sql):
                test.assert_blocked(lambda: enter_database(self.settings))

        def enter_database(connection):
            with job_ownership(warehouse=connection):
                self.fail("Smoke database was unguarded")

        def migrate(client):
            self.assert_blocked(lambda: enter_database(client.settings))

        def publish(client, rows):
            self.assert_blocked(lambda: enter_database(client.settings))
            client.rows = rows
            return {"status": "success"}

        def save(path):
            fixture_settings = SparkSettings(
                LakehousePaths(
                    Path(unquote(urlsplit(path).path)).parent.as_uri(),
                    location_market_signal_snapshot=path,
                )
            )

            def enter_delta():
                with job_ownership(
                    fixture_settings, outputs=("location_market_signal_snapshot",)
                ):
                    self.fail("Smoke fixture Delta was unguarded")

            self.assert_blocked(enter_delta)

        spark = Mock()
        spark.createDataFrame.return_value.write.format.return_value.save.side_effect = (
            save
        )
        with (
            patch.object(smoke, "__file__", str(self.root / "pkg/smoke.py")),
            patch.object(smoke, "create_spark", return_value=spark),
            patch.object(smoke, "ClickHouseClient", Client),
            patch.object(smoke, "migrate", side_effect=migrate),
            patch.object(smoke, "replace_serving", side_effect=publish),
            patch.object(
                smoke,
                "read_gold_rows",
                side_effect=lambda _s, p: (
                    source
                    if p == self.settings.paths.location_market_signal_snapshot
                    else smoke.null_fixtures(source)
                ),
            ),
            patch.object(smoke, "backend", side_effect=lambda *a: nullcontext(Mock())),
            patch.object(smoke, "verify_api", return_value={}),
            patch.object(smoke.subprocess, "run"),
            redirect_stdout(io.StringIO()),
        ):
            smoke.main(settings=self.settings, connection=self.connection)
        spark.stop.assert_called_once()

    def test_internal_batch_and_loader_main_reuse_legitimate_context(self):
        with pipeline.lakehouse_lock(self.config) as ownership:
            with patch.object(
                refresh.fcntl, "flock", wraps=refresh.fcntl.flock
            ) as flock:
                for module, name, argv in CASES[:-1]:
                    with (
                        patch.object(module, "create_spark", return_value=Mock()),
                        patch.object(module, name, return_value={}),
                    ):
                        self.call(module, argv, ownership=ownership)
                with (
                    patch.object(stream, "create_spark", return_value=Mock()),
                    patch.object(
                        stream, "start_social_stream", return_value=Mock(isActive=False)
                    ),
                ):
                    self.call(stream, ["--available-now"], ownership=ownership)
                with (
                    patch.object(loader, "ClickHouseClient"),
                    patch.object(loader, "migrate"),
                    redirect_stdout(io.StringIO()),
                ):
                    loader.main(
                        ["--migrate-only"],
                        connection=self.connection,
                        ownership=ownership,
                    )
                self.assertEqual(flock.call_count, 0)
            self.assert_blocked(lambda: pipeline.execute(self.config, {}, self.root))

    def test_forged_and_expired_contexts_cause_no_cli_write(self):
        with pipeline.lakehouse_lock(self.config) as expired:
            forged = refresh.AnalyticalOwnership(expired.target_identity)
        for token in (True, forged, expired):
            with (
                self.subTest(token=type(token).__name__),
                patch.object(social, "create_spark") as runtime,
                patch.object(social, "build_social_activity_daily") as write,
                self.assertRaises(ValueError),
            ):
                self.call(social, [], ownership=token)
            runtime.assert_not_called()
            write.assert_not_called()

    def test_environment_claim_cannot_bypass_guard(self):
        with (
            pipeline.lakehouse_lock(self.config),
            patch.dict(os.environ, {"DSP_ANALYTICAL_OWNERSHIP": "1"}),
            patch.object(social, "create_spark") as runtime,
        ):
            self.assert_blocked(lambda: self.call(social, []))
            runtime.assert_not_called()

    def test_shared_gold_override_conflicts_across_different_bases(self):
        other = SparkSettings(
            LakehousePaths(
                (self.root / "other").as_uri(),
                social_activity_daily=self.settings.paths.social_activity_daily,
            )
        )
        with (
            pipeline.lakehouse_lock(self.config),
            patch.object(social, "create_spark") as runtime,
        ):
            self.assert_blocked(lambda: self.call(social, [], settings=other))
            runtime.assert_not_called()

    def test_alias_and_shared_database_conflicts_with_publisher(self):
        alias = self.root / "alias"
        Path(self.root / "lake").mkdir()
        alias.symlink_to(self.root / "lake", target_is_directory=True)
        equivalent = SparkSettings(LakehousePaths(alias.as_uri()))
        with pipeline.lakehouse_lock(self.config), patch.object(social, "create_spark"):
            self.assert_blocked(lambda: self.call(social, [], settings=equivalent))
            same = ClickHouseSettings(
                host="127.0.0.1", database=self.connection.database
            )
            self.assert_blocked(
                lambda: loader.main(["--migrate-only"], connection=same)
            )

    def test_independent_local_targets_can_execute(self):
        other = SparkSettings(LakehousePaths((self.root / "independent").as_uri()))
        with (
            pipeline.lakehouse_lock(self.config),
            patch.object(social, "create_spark", return_value=Mock()),
            patch.object(social, "build_social_activity_daily", return_value={}),
        ):
            self.call(social, [], settings=other)

    def test_stream_holds_actual_writes_for_entire_query_but_gold_can_coexist(self):
        query = Mock(isActive=True)
        spark = Mock()

        def waiting():
            self.assert_blocked(lambda: pipeline.execute(self.config, {}, self.root))
            with (
                patch.object(social, "create_spark", return_value=Mock()),
                patch.object(social, "build_social_activity_daily", return_value={}),
            ):
                self.call(social, [])
            self.assert_blocked(lambda: self.call(stream, ["--available-now"]))

        query.awaitTermination.side_effect = waiting
        with (
            patch.object(stream, "create_spark", return_value=spark),
            patch.object(stream, "start_social_stream", return_value=query),
        ):
            self.call(stream, [])
        query.stop.assert_called_once()
        spark.stop.assert_called_once()
        with pipeline.lakehouse_lock(self.config):
            pass

    def test_write_exception_releases_main_guard(self):
        spark = Mock()
        with (
            patch.object(social, "create_spark", return_value=spark),
            patch.object(
                social,
                "build_social_activity_daily",
                side_effect=RuntimeError("fixture failure"),
            ),
            self.assertRaisesRegex(RuntimeError, "fixture failure"),
        ):
            self.call(social, [])
        spark.stop.assert_called_once()
        with pipeline.lakehouse_lock(self.config):
            pass

    def test_sigkill_of_standalone_writer_releases_actual_guard(self):
        env = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [str(ROOT / "pipelines/spark/src"), str(ROOT / "apps/collector/src")]
            ),
        }
        child = subprocess.Popen(
            [sys.executable, "-u", "-c", CHILD, self.settings.paths.base],
            cwd=self.root,
            env=env,
            text=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            ready, _, _ = select.select([child.stdout], [], [], 10)
            self.assertTrue(ready)
            self.assertEqual(child.stdout.readline().strip(), "HELD")
            with patch.object(pipeline, "_execute_locked") as write:
                self.assert_blocked(
                    lambda: pipeline.execute(self.config, {}, self.root)
                )
                write.assert_not_called()
            child.kill()
            child.communicate(timeout=5)
            with pipeline.lakehouse_lock(self.config):
                pass
        finally:
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=5)

    def test_remote_managed_gold_keeps_supplied_session_and_no_local_guard(self):
        remote = SparkSettings(LakehousePaths("s3a://fixture/lake"))
        spark = Mock()
        with (
            patch.object(social, "build_social_activity_daily", return_value={}),
            patch.object(social, "create_spark") as create,
            redirect_stdout(io.StringIO()),
        ):
            social.main(spark=spark, settings=remote)
        create.assert_not_called()
        spark.stop.assert_not_called()
        with self.assertRaises(ValueError):
            job_ownership(remote, outputs=("social_activity_daily",), ownership=True)

    def test_remote_base_cannot_hide_a_local_write_override(self):
        mixed = SparkSettings(
            LakehousePaths(
                "s3a://fixture/lake",
                social_activity_daily=self.settings.paths.social_activity_daily,
            )
        )
        with (
            pipeline.lakehouse_lock(self.config),
            patch.object(social, "create_spark") as create,
        ):
            self.assert_blocked(lambda: self.call(social, [], settings=mixed))
            create.assert_not_called()

    def test_portable_wheel_carries_exact_shared_ownership(self):
        spec = importlib.util.spec_from_file_location(
            "ownership_wheel_fixture", ROOT / "infra/databricks/build_wheel.py"
        )
        builder = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(builder)
        output = self.root / "wheel"
        with patch.dict(
            os.environ,
            {
                "POETRY_VIRTUALENVS_CREATE": "false",
                "POETRY_CACHE_DIR": str(self.root / "poetry-cache"),
            },
        ):
            artifact = builder.build_wheel(output)
        unpacked = self.root / "installed"
        with zipfile.ZipFile(artifact) as wheel:
            for relative in (
                "dsp_collector/normalizers/observation.py",
                "orchestration/local/processing_manifest.py",
                "dsp_contracts/data/bronze/record-observation.v1.schema.json",
                "dsp_contracts/data/bronze/observation-artifact.v1.schema.json",
            ):
                source = ROOT / (
                    "apps/collector/src/" + relative
                    if relative.startswith("dsp_collector/")
                    else relative.replace("dsp_contracts/", "contracts/")
                )
                self.assertEqual(wheel.read(relative), source.read_bytes())
            self.assertEqual(
                wheel.read("orchestration/local/analytical_refresh.py"),
                (ROOT / "orchestration/local/analytical_refresh.py").read_bytes(),
            )
            wheel.extractall(unpacked)
        probe = """
import sys
from dsp_spark.runtime.config import LakehousePaths, SparkSettings
from dsp_spark.runtime.ownership import job_ownership
from orchestration.local.analytical_refresh import analytical_ownership
from types import SimpleNamespace
s=SparkSettings(LakehousePaths(sys.argv[1]))
with analytical_ownership(SimpleNamespace(spark=s)) as token:
    with job_ownership(s, outputs=('social_activity_daily',), ownership=token):
        pass
print('portable guard OK')
from dsp_collector.normalizers.observation import observation_schema
from dsp_spark.processing.clean.contracts import contract_documents, spark_schema
import json
assert json.loads(contract_documents())['observation'] == observation_schema()
assert len(spark_schema('observation').names) == 62
print('portable observation contract OK')
"""
        result = subprocess.run(
            [sys.executable, "-c", probe, (self.root / "packaged-lake").as_uri()],
            cwd=self.root,
            env={**os.environ, "PYTHONPATH": str(unpacked)},
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("portable guard OK", result.stdout)


if __name__ == "__main__":
    unittest.main()
