"""Real local flock contention/crash tests; no data services or collection."""

import json
import os
import select
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from orchestration.local import analytical_refresh as refresh
from orchestration.local import collection_worker as worker
from orchestration.local import pipeline

CHILD = """
import json, os, sys
from types import SimpleNamespace
from orchestration.local.analytical_refresh import analytical_ownership
data = json.loads(sys.argv[1])
config = SimpleNamespace(spark=SimpleNamespace(paths=SimpleNamespace(**data['paths'])))
if data['warehouse'] is not None:
    config.warehouse = SimpleNamespace(**data['warehouse'])
with analytical_ownership(config):
    print('HELD', flush=True)
    if sys.stdin.readline().strip() == 'crash':
        os._exit(23)
"""


class AnalyticalOwnershipTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="dsp-r01-ownership-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def config(self, name="a", *, paths=None, database=None, host="localhost"):
        base = self.root / name
        return SimpleNamespace(
            spark=SimpleNamespace(
                paths=SimpleNamespace(base=base.as_uri(), **(paths or {}))
            ),
            warehouse=SimpleNamespace(
                host=host,
                port=8123,
                database="fixture_"
                + self.root.name.replace("-", "_")
                + "_"
                + (database or name),
                user="fixture",
                password="must-not-affect-identity",
            ),
        )

    def assert_competes(self, config):
        with (
            self.assertRaisesRegex(RuntimeError, "Another local pipeline writer"),
            refresh.analytical_ownership(config),
        ):
            self.fail("Competing operation entered")

    def holder(self, config):
        data = {
            "paths": {k: str(v) for k, v in vars(config.spark.paths).items()},
            "warehouse": (
                {k: getattr(config.warehouse, k) for k in ("host", "port", "database")}
                if hasattr(config, "warehouse")
                else None
            ),
        }
        child = subprocess.Popen(
            [sys.executable, "-u", "-c", CHILD, json.dumps(data)],
            cwd=ROOT,
            env={**os.environ, "PYTHONPATH": str(ROOT)},
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        def cleanup():
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=5)

        self.addCleanup(cleanup)
        ready, _, _ = select.select([child.stdout], [], [], 5)
        self.assertTrue(ready, "Child did not report ownership within five seconds")
        line = child.stdout.readline().strip()
        self.assertEqual(
            line, "HELD", child.stderr.read() if child.poll() is not None else line
        )
        return child

    def test_exclusive_acquisition_and_successful_release(self):
        config = self.config()
        with pipeline.lakehouse_lock(config) as ownership:
            self.assertEqual(
                ownership.target_identity, refresh.canonical_targets(config).identity
            )
            self.assert_competes(config)
            self.assertTrue((self.root / "a/.pipeline-writer.lock").exists())
        with pipeline.lakehouse_lock(config):
            pass

    def test_two_processes_compete_then_release(self):
        config = self.config()
        child = self.holder(config)
        self.assert_competes(config)
        child.communicate("finish\n", timeout=5)
        self.assertEqual(child.returncode, 0)
        with refresh.analytical_ownership(config):
            pass

    def test_different_target_sets_in_same_refresh_group_compete(self):
        x, y = (self.root / "gold-x").as_uri(), (self.root / "gold-y").as_uri()
        one = self.config(paths={"gold_x": x})
        both = self.config(paths={"gold_x": x, "gold_y": y})
        self.assertNotEqual(
            refresh.canonical_targets(one).identity,
            refresh.canonical_targets(both).identity,
        )
        with refresh.analytical_ownership(one):
            self.assert_competes(both)

    def test_cross_process_shared_override_with_different_bases_competes(self):
        x = (self.root / "shared-gold").as_uri()
        one = self.config("a", paths={"gold_x": x})
        both = self.config(
            "b", paths={"gold_x": x, "gold_y": (self.root / "y").as_uri()}
        )
        self.holder(one)
        self.assert_competes(both)

    def test_same_base_with_different_serving_databases_competes(self):
        with refresh.analytical_ownership(self.config(database="first")):
            self.assert_competes(self.config(database="second"))

    def test_shared_serving_group_with_different_bases_competes(self):
        self.holder(self.config("a", database="shared"))
        self.assert_competes(self.config("b", database="shared", host="127.0.0.1"))

    def test_disjoint_isolated_targets_do_not_interfere(self):
        self.holder(self.config("a"))
        with refresh.analytical_ownership(self.config("b")):
            pass

    def test_failed_partial_acquisition_releases_other_guards(self):
        x = (self.root / "shared-gold").as_uri()
        with refresh.analytical_ownership(self.config("a", paths={"gold_x": x})):
            self.assert_competes(self.config("b", paths={"gold_x": x}))
            # B first claimed its serving database, then failed at shared X.
            with refresh.analytical_ownership(self.config("c", database="b")):
                pass

    def test_exception_releases_ownership(self):
        config = self.config()
        with (
            self.assertRaisesRegex(RuntimeError, "injected"),
            refresh.analytical_ownership(config),
        ):
            raise RuntimeError("injected")
        with refresh.analytical_ownership(config):
            pass

    def test_crash_without_finally_releases_os_locks(self):
        config = self.config()
        child = self.holder(config)
        child.communicate("crash\n", timeout=5)
        self.assertEqual(child.returncode, 23)
        with refresh.analytical_ownership(config):
            pass

    def test_process_kill_releases_os_locks(self):
        config = self.config()
        child = self.holder(config)
        child.kill()
        child.communicate(timeout=5)
        self.assertLess(child.returncode, 0)
        with refresh.analytical_ownership(config):
            pass

    def test_nested_internal_scope_does_not_reacquire_or_release_outer_lock(self):
        config = self.config(paths={"x": (self.root / "x").as_uri()})
        with (
            patch.object(refresh.fcntl, "flock", wraps=refresh.fcntl.flock) as flock,
            refresh.analytical_ownership(config) as ownership,
        ):
            calls = flock.call_count
            with pipeline.lakehouse_lock(self.config(), ownership=ownership) as nested:
                self.assertIs(nested, ownership)
                self.assertEqual(flock.call_count, calls)
            self.assert_competes(config)

    def test_pipeline_and_worker_internal_calls_reuse_context(self):
        config = self.config()
        report, summary, result = {}, {}, object()
        with (
            refresh.analytical_ownership(config) as ownership,
            patch.object(pipeline, "resolve_config", return_value=config) as resolve,
            patch.object(worker, "_execute_locked", return_value=result) as operation,
        ):

            def nested(*args):
                return worker.execute(
                    "pbplus",  # Legacy full-cycle ownership; Threads now hands off.
                    "run",
                    self.root,
                    {},
                    report,
                    lambda _: None,
                    ownership=ownership,
                )

            with patch.object(pipeline, "_execute_locked", side_effect=nested):
                self.assertIs(
                    pipeline.execute(config, summary, self.root, ownership=ownership),
                    result,
                )
            resolve.assert_called_once()
            self.assertIs(operation.call_args.kwargs["config"], config)
            self.assert_competes(config)

    def test_external_wrappers_hold_ownership_through_operation(self):
        config = self.config()

        def operation(*args, **kwargs):
            self.assert_competes(config)
            return "completed"

        with patch.object(pipeline, "_execute_locked", side_effect=operation):
            self.assertEqual(pipeline.execute(config, {}, self.root), "completed")
        with (
            patch.object(pipeline, "resolve_config", return_value=config),
            patch.object(worker, "_execute_locked", side_effect=operation),
        ):
            self.assertEqual(
                worker.execute("pbplus", "run", self.root, {}, {}, lambda _: None),
                "completed",
            )
        with refresh.analytical_ownership(config):
            pass

    def test_direct_pipeline_script_import_remains_supported(self):
        result = subprocess.run(
            [sys.executable, str(ROOT / "orchestration/local/pipeline.py"), "--help"],
            cwd=self.root,
            env={**os.environ, "PYTHONPATH": ""},
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--preflight", result.stdout)

    def test_invalid_and_forged_contexts_are_rejected(self):
        config = self.config()
        with refresh.analytical_ownership(config) as ownership:

            class ForgedOwnership(refresh.AnalyticalOwnership):
                def __hash__(self):
                    return hash(ownership)

                def __eq__(self, other):
                    return True

            for token in (
                True,
                object(),
                refresh.AnalyticalOwnership(ownership.target_identity),
                ForgedOwnership(ownership.target_identity),
            ):
                with (
                    self.subTest(token=type(token).__name__),
                    self.assertRaisesRegex(ValueError, "ownership context"),
                    refresh.analytical_ownership(config, ownership=token),
                ):
                    self.fail("Forged ownership entered")

    def test_released_context_is_rejected(self):
        config = self.config()
        with refresh.analytical_ownership(config) as ownership:
            pass
        with (
            self.assertRaisesRegex(ValueError, "inactive"),
            refresh.analytical_ownership(config, ownership=ownership),
        ):
            self.fail("Expired ownership entered")

    def test_conflicting_context_rejects_unheld_targets(self):
        with refresh.analytical_ownership(self.config()) as ownership:
            for config in (
                self.config("b"),
                self.config(paths={"x": (self.root / "x").as_uri()}),
            ):
                with (
                    self.assertRaisesRegex(ValueError, "does not cover"),
                    refresh.analytical_ownership(config, ownership=ownership),
                ):
                    self.fail("Unheld targets entered")

    def test_another_thread_cannot_borrow_context(self):
        config = self.config()
        with refresh.analytical_ownership(config) as ownership:
            errors = []

            def borrow():
                try:
                    with refresh.analytical_ownership(config, ownership=ownership):
                        errors.append("entered")
                except ValueError:
                    errors.append("rejected")

            thread = threading.Thread(target=borrow)
            thread.start()
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, ["rejected"])

    def test_forked_child_cannot_borrow_or_unlock_parent_ownership(self):
        config = self.config()
        with refresh.analytical_ownership(config) as ownership:
            pid = os.fork()
            if pid == 0:
                child_status = 1
                try:
                    with refresh.analytical_ownership(config, ownership=ownership):
                        pass
                except ValueError:
                    child_status = 0
                finally:
                    os._exit(child_status)
            _, status = os.waitpid(pid, 0)
            self.assertEqual(os.waitstatus_to_exitcode(status), 0)
            self.assert_competes(config)

    def test_equivalent_paths_and_order_have_identical_identity(self):
        base = self.root / "actual space"
        base.mkdir()
        alias = self.root / "alias"
        alias.symlink_to(base, target_is_directory=True)
        first = self.config(paths={"x": (base / "x").as_uri()})
        first.spark.paths.base = base.as_uri()
        second = self.config(host="127.0.0.1")
        second.spark.paths = SimpleNamespace(
            x=str(alias / "x"), base="file://localhost" + str(alias)
        )
        second.warehouse.user, second.warehouse.password = "other", "different"
        self.assertEqual(
            refresh.canonical_targets(first), refresh.canonical_targets(second)
        )
        with refresh.analytical_ownership(first):
            self.assert_competes(second)

    def test_nonlocal_or_invalid_paths_are_rejected(self):
        for value in (
            "s3a://bucket/lake",
            "file://remote/tmp/lake",
            "file:///tmp/lake?x=1",
            None,
            "",
        ):
            with self.subTest(value=value):
                config = self.config()
                config.spark.paths.base = value
                with (
                    self.assertRaises((ValueError, TypeError)),
                    refresh.analytical_ownership(config),
                ):
                    self.fail("Invalid path entered")

    def test_removed_lock_file_invalidates_internal_context(self):
        config = self.config()
        with refresh.analytical_ownership(config) as ownership:
            (self.root / "a/.pipeline-writer.lock").unlink()
            with (
                self.assertRaisesRegex(ValueError, "inactive"),
                refresh.analytical_ownership(config, ownership=ownership),
            ):
                self.fail("Unverifiable ownership entered")
