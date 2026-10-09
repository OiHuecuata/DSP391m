"""Isolated synthetic contracts, immutable persistence and ownership boundaries."""

import copy
import hashlib
import os
import stat
import subprocess
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from orchestration.local import processing_manifest as pm
from orchestration.local.analytical_refresh import (
    AnalyticalOwnership,
    analytical_ownership,
    canonical_targets,
)

STAMP = "2026-10-05T12:00:00.000000Z"
LATER = "2026-10-05T12:01:00.000000Z"


class ProcessingManifestTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="dsp-manifest-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.config = SimpleNamespace(
            spark=SimpleNamespace(
                paths=SimpleNamespace(
                    base=str(self.root / "lake"),
                    gold=str(self.root / "lake" / "gold"),
                )
            ),
            warehouse=None,
        )
        self.artifact = self.root / "source-run.json"
        self.artifact.write_bytes(
            b'{"source":"threads","run_id":"run:/native-01","count":1}\n'
        )
        self.ref = pm.artifact_reference(
            self.artifact,
            source_platform="threads",
            collection_run_id="run:/native-01",
            schema_version="threads.v1",
            record_count=1,
        )
        self.acquisition = {
            "outcome": "success",
            "accepted_records": 1,
            "coverage": {"status": "UNKNOWN"},
            "runs": [
                {
                    "source_platform": "threads",
                    "collection_run_id": "run:/native-01",
                    "outcome": "success",
                    "accepted_records": 1,
                }
            ],
        }
        self.spec = {
            "analysis_end_date": "2026-10-05",
            "window_days": 30,
            "processing_contract_version": "gold.v1",
            "code_version": "git:041b5b3",
            "configuration_hash": pm.content_hash(
                {"threshold": 0.25, "timezone": "UTC"}
            ),
            "reference_version": "location.v1",
            "feature_version": None,
            "policy_version": None,
            "tombstone_version": None,
        }
        self.store = pm.LocalProcessingStore(self.root / "control", self.config)

    def manifest(self, **changes):
        arguments = {
            "inputs": [self.ref],
            "acquisition": self.acquisition,
            "processing_spec": self.spec,
            "config": self.config,
            "readiness": "READY",
            "created_at": STAMP,
        }
        arguments.update(changes)
        return pm.build_manifest(**arguments)

    def ack(self, manifest=None, **changes):
        arguments = {
            "attempt_id": "attempt:one",
            "stage": "validate",
            "outcome": "SUCCESS",
            "completed_at": LATER,
        }
        arguments.update(changes)
        return pm.build_ack(
            self.manifest() if manifest is None else manifest, **arguments
        )

    def partial(self, outcome="partial", count=1):
        acquisition = copy.deepcopy(self.acquisition)
        acquisition.update(outcome=outcome, accepted_records=count)
        acquisition["runs"][0].update(outcome=outcome, accepted_records=count)
        acquisition["coverage"] = {
            "status": "RATE_LIMITED",
            "pagination_complete": False,
        }
        return acquisition

    def test_identical_logical_request_has_same_id_and_known_spec_fingerprint(self):
        first = self.manifest()
        self.assertEqual(first, self.manifest())
        self.assertEqual(
            first["processing_spec_hash"],
            "sha256:aae83953775fa1a673b5c09a60654d27f846a950683ff58b3ff50cd289209961",
        )

    def test_different_actual_artifact_bytes_change_id_preserving_native_identity(self):
        original_bytes = self.artifact.read_bytes()
        original = self.manifest()
        self.assertEqual(original["inputs"][0]["collection_run_id"], "run:/native-01")
        self.assertEqual(original["inputs"][0]["artifact_uri"], self.artifact.as_uri())
        self.assertEqual(
            original["inputs"][0]["content_sha256"],
            hashlib.sha256(original_bytes).hexdigest(),
        )
        self.assertEqual(self.artifact.read_bytes(), original_bytes)
        self.artifact.write_bytes(original_bytes + b" ")
        changed_ref = pm.artifact_reference(
            self.artifact,
            source_platform="threads",
            collection_run_id="run:/native-01",
            schema_version="threads.v1",
            record_count=1,
        )
        self.assertNotEqual(
            original["manifest_id"], self.manifest(inputs=[changed_ref])["manifest_id"]
        )

    def test_all_output_affecting_spec_fields_change_id(self):
        original = self.manifest()["manifest_id"]
        changes = {
            "analysis_end_date": "2026-10-06",
            "window_days": 31,
            "processing_contract_version": "gold.v2",
            "code_version": "git:other",
            "configuration_hash": "b" * 64,
            "reference_version": "location.v2",
            "feature_version": "feature.v1",
            "policy_version": "policy.v1",
            "tombstone_version": "tombstone.v1",
        }
        for key, replacement in changes.items():
            with self.subTest(field=key):
                self.assertNotEqual(
                    original,
                    self.manifest(processing_spec={**self.spec, key: replacement})[
                        "manifest_id"
                    ],
                )

    def test_targets_and_same_physical_aliases(self):
        original = self.manifest()
        self.assertEqual(
            original["target_identity"], canonical_targets(self.config).identity
        )
        alias = copy.deepcopy(self.config)
        alias.spark.paths.base = Path(alias.spark.paths.base).as_uri()
        alias.spark.paths.gold += "/../gold"
        self.assertEqual(
            original["manifest_id"], self.manifest(config=alias)["manifest_id"]
        )
        changed = copy.deepcopy(self.config)
        changed.spark.paths.gold = str(self.root / "other-gold")
        self.assertNotEqual(
            original["manifest_id"], self.manifest(config=changed)["manifest_id"]
        )
        self.assertNotEqual(
            original["manifest_id"], self.manifest(include_group=False)["manifest_id"]
        )

    def test_serving_target_identity_reuses_host_normalization(self):
        config = copy.deepcopy(self.config)
        config.warehouse = SimpleNamespace(
            host="LOCALHOST.", port=8123, database="fixture"
        )
        first = self.manifest(config=config)
        config.warehouse.host = "127.0.0.1"
        self.assertEqual(
            first["manifest_id"], self.manifest(config=config)["manifest_id"]
        )
        config.warehouse.database = "other"
        self.assertNotEqual(
            first["manifest_id"], self.manifest(config=config)["manifest_id"]
        )

    def test_unordered_permutation_and_explicit_order(self):
        other = {
            **self.ref,
            "artifact_uri": (self.root / "second.json").as_uri(),
            "content_sha256": "b" * 64,
        }
        forward = [self.ref, other]
        self.assertEqual(
            self.manifest(inputs=forward)["manifest_id"],
            self.manifest(inputs=forward[::-1])["manifest_id"],
        )
        self.assertNotEqual(
            self.manifest(inputs=forward, input_order="ORDERED")["manifest_id"],
            self.manifest(inputs=forward[::-1], input_order="ORDERED")["manifest_id"],
        )

    def test_creation_logging_and_attempts_do_not_change_manifest_identity(self):
        first = self.manifest()
        later = self.manifest(created_at=LATER, metadata={"origin": "REPLAY"})
        self.assertEqual(first["manifest_id"], later["manifest_id"])
        self.assertNotEqual(first["payload_sha256"], later["payload_sha256"])
        one = self.ack(first, attempt_id=pm.new_attempt_id())
        two = self.ack(first, attempt_id=pm.new_attempt_id())
        self.assertEqual(one["manifest_id"], two["manifest_id"])
        self.assertNotEqual(one["attempt_id"], two["attempt_id"])
        self.assertNotEqual(one["ack_id"], two["ack_id"])

    def test_missing_required_fields_fail_including_nullable_dependencies(self):
        first = self.manifest()
        for scope in ("root", "processing_spec", "inputs", "acquisition"):
            container = (
                first
                if scope == "root"
                else first[scope][0] if scope == "inputs" else first[scope]
            )
            for field in container:
                with self.subTest(scope=scope, field=field):
                    value = copy.deepcopy(first)
                    target = (
                        value
                        if scope == "root"
                        else value[scope][0] if scope == "inputs" else value[scope]
                    )
                    del target[field]
                    with self.assertRaises((ValueError, KeyError)):
                        pm.validate_manifest(value)

    def test_bad_timestamp_hash_version_unknown_fields_and_types(self):
        bad_specs = [
            {"analysis_end_date": "2026-02-30"},
            {"window_days": True},
            {"window_days": 30.0},
            {"window_days": 0},
            {"window_days": 2**63},
            {"configuration_hash": "SHA256:bad"},
            {"code_version": "../version"},
            {"code_version": "v1\n"},
            {"feature_version": ""},
            {"extra": "secret"},
        ]
        for changes in bad_specs:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.manifest(processing_spec={**self.spec, **changes})
        for timestamp in (
            "bad",
            "2026-02-30T00:00:00Z",
            "2026-10-05T12:00:00",
            datetime(2026, 10, 5, tzinfo=None),  # noqa: DTZ001 -- reject naive input
        ):
            with self.subTest(timestamp=timestamp), self.assertRaises(ValueError):
                self.manifest(created_at=timestamp)
        for changes in (
            {"content_sha256": "A" * 64},
            {"artifact_schema_version": ""},
            {"record_count": True},
            {"record_count": -1},
            {"extra": None},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.manifest(inputs=[{**self.ref, **changes}])

    def test_canonical_json_strict_types_nonfinite_and_duplicate_properties(self):
        self.assertEqual(
            pm.canonical_json({"b": None, "a": "Việt"}), '{"a":"Việt","b":null}'
        )
        for value in (float("nan"), float("inf"), {1: "key"}, (1, 2), {"x": Path("/")}):
            with (
                self.subTest(value=repr(value)),
                self.assertRaises((ValueError, TypeError)),
            ):
                pm.canonical_json(value)
        for raw in ('{"x":1,"x":2}', '{"x":{"a":1,"a":2}}', '{"x":NaN}', '{"x":1e999}'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                pm.parse_json(raw)
        self.assertNotEqual(pm.content_hash({"x": None}), pm.content_hash({"x": 0}))
        self.assertNotEqual(pm.content_hash({}), pm.content_hash({"x": None}))
        self.assertNotEqual(pm.content_hash({"x": 1}), pm.content_hash({"x": 1.0}))

    def test_timestamp_normalizes_timezone_and_never_defaults_now(self):
        self.assertEqual(
            self.manifest(created_at="2026-10-05T19:00:00+07:00"), self.manifest()
        )
        with self.assertRaises(ValueError):
            self.manifest(created_at=None)

    def test_minimal_future_native_observation_boundary(self):
        reference = {
            **self.ref,
            "kind": "observations",
            "source_record_id": "native:/id",
            "policy_reference": "policy:v1",
            "record_count": None,
        }
        value = self.manifest(inputs=[reference])
        self.assertEqual(value["inputs"][0], reference)
        del reference["source_record_id"]
        with self.assertRaises(ValueError):
            self.manifest(inputs=[reference])
        reference["source_record_id"] = None
        with self.assertRaises(ValueError):
            self.manifest(inputs=[reference])
        reference["observation_range"] = {
            "first_id": "native:first",
            "last_id": "native:last",
        }
        self.assertEqual(self.manifest(inputs=[reference])["inputs"], [reference])

    def ingestion_refs(self):
        lineage = {"source_platform": "threads", "collection_run_id": "run:/native-01"}
        return [
            {
                **lineage,
                "kind": "delta",
                "uri": (self.root / "input-delta").as_uri(),
                "delta_version": 0,
                "schema_version": "silver.v1",
            },
            {
                **lineage,
                "kind": "kafka_ack",
                "broker_namespace": "local:v1",
                "topic": "existing.synthetic",
                "partition": 0,
                "first_offset": 10,
                "last_offset": 11,
                "acknowledged_count": 2,
            },
        ]

    def test_optional_ingestion_references_are_declarative_and_configuration_sensitive(
        self,
    ):
        refs = self.ingestion_refs()
        original = self.manifest(ingestion_refs=refs)
        self.assertEqual(
            original["manifest_id"],
            self.manifest(ingestion_refs=refs[::-1])["manifest_id"],
        )
        refs[0]["delta_version"] = 1
        self.assertNotEqual(
            original["manifest_id"], self.manifest(ingestion_refs=refs)["manifest_id"]
        )
        refs[1]["last_offset"] = 9
        with self.assertRaises(ValueError):
            self.manifest(ingestion_refs=refs)
        refs[1].update(last_offset=10, acknowledged_count=2)
        with self.assertRaises(ValueError):
            self.manifest(ingestion_refs=refs)

    def test_input_lineage_duplicate_inputs_runs_and_inconsistent_counts_rejected(self):
        with self.assertRaises(ValueError):
            self.manifest(inputs=[{**self.ref, "collection_run_id": "other-run"}])
        with self.assertRaises(ValueError):
            self.manifest(inputs=[self.ref, self.ref])
        acquisition = copy.deepcopy(self.acquisition)
        acquisition["runs"] *= 2
        with self.assertRaises(ValueError):
            self.manifest(acquisition=acquisition)
        with self.assertRaises(ValueError):
            self.manifest(acquisition={**self.acquisition, "accepted_records": 2})

    def test_partial_acquisition_and_coverage_survive_successful_processing(self):
        manifest = self.manifest(acquisition=self.partial(), readiness="PARTIAL_READY")
        self.store.save_manifest(manifest)
        ack = self.ack(manifest)
        self.store.append_ack(ack)
        self.assertEqual(ack["outcome"], "SUCCESS")
        self.assertEqual(ack["collection_outcome"], "partial")
        self.assertEqual(
            self.store.load_manifest(manifest["manifest_id"])["acquisition"],
            self.partial(),
        )
        self.assertNotEqual(manifest["manifest_id"], self.manifest()["manifest_id"])
        with self.assertRaises(ValueError):
            self.manifest(acquisition=self.partial(), readiness="READY")

    def test_failed_acquisition_without_usable_inputs_cannot_be_ready(self):
        failed = self.partial("failed", 0)
        for readiness in ("READY", "PARTIAL_READY"):
            with self.subTest(readiness=readiness), self.assertRaises(ValueError):
                self.manifest(acquisition=failed, inputs=[], readiness=readiness)
        manifest = self.manifest(
            acquisition=failed, inputs=[], readiness="COLLECTION_FAILED"
        )
        ack = self.ack(manifest)
        self.assertEqual(ack["collection_outcome"], "failed")
        self.assertEqual(manifest["acquisition"]["accepted_records"], 0)

    def test_unknown_counts_are_not_fabricated_as_zero(self):
        acquisition = self.partial(count=None)
        manifest = self.manifest(
            acquisition=acquisition,
            inputs=[{**self.ref, "record_count": None}],
            readiness="NOT_READY",
        )
        self.assertIsNone(manifest["acquisition"]["accepted_records"])
        self.assertIsNone(manifest["inputs"][0]["record_count"])
        ack = self.ack(manifest)
        self.assertEqual(ack["input_refs"], [])
        self.assertEqual(ack["output_refs"], [])
        self.assertIsNone(ack["publication_generation_reference"])
        self.assertIsNone(ack["owner_reference"])

    def test_mixed_source_outcomes_preserve_each_run(self):
        acquisition = copy.deepcopy(self.acquisition)
        acquisition["runs"].append(
            {
                "source_platform": "youtube",
                "collection_run_id": "yt-01",
                "outcome": "failed",
                "accepted_records": 0,
            }
        )
        acquisition["outcome"] = "partial"
        manifest = self.manifest(acquisition=acquisition, readiness="PARTIAL_READY")
        self.assertEqual(
            {r["outcome"] for r in manifest["acquisition"]["runs"]},
            {"success", "failed"},
        )
        acquisition["outcome"] = "success"
        with self.assertRaises(ValueError):
            self.manifest(acquisition=acquisition)

    def test_future_uncertain_and_fencing_facts_do_not_change_acquisition(self):
        for readiness in ("FENCING_REQUIRED", "NOT_READY"):
            manifest = self.manifest(readiness=readiness)
            for outcome in ("PUBLICATION_UNCERTAIN", "FENCING_REQUIRED"):
                ack = self.ack(manifest, outcome=outcome, completed_at=None)
                self.assertEqual(ack["collection_outcome"], "success")
                self.assertIsNone(ack["publication_generation_reference"])

    def test_ack_timestamps_required_fields_and_sanitized_error(self):
        started = self.ack(outcome="STARTED", started_at=STAMP, completed_at=None)
        self.assertIsNone(started["completed_at"])
        for changes in (
            {"outcome": "STARTED", "started_at": None, "completed_at": None},
            {"completed_at": None},
            {"started_at": LATER, "completed_at": STAMP},
            {"outcome": "FAILED"},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.ack(**changes)
        error = pm.safe_error(
            RuntimeError("password=secret https://private/?token=secret"),
            code="IO_FAILURE",
            retryable=True,
        )
        ack = self.ack(outcome="FAILED", error=error)
        self.assertEqual(ack["error"], {"code": "IO_FAILURE", "retryable": True})
        self.assertNotIn("secret", pm.canonical_json(ack))
        for field in ack:
            with self.subTest(field=field), self.assertRaises(ValueError):
                pm.validate_ack({k: v for k, v in ack.items() if k != field})
        with self.assertRaises(ValueError):
            self.ack(error={**error, "message": "secret"})

    def test_output_reference_versions_fingerprints_and_owner_are_optional_facts(self):
        manifest = self.manifest()
        output = {
            "kind": "delta",
            "uri": (self.root / "result").as_uri(),
            "schema_version": "gold.v1",
            "validation": "VALIDATED",
            "version": 0,
            "fingerprint": "a" * 64,
        }
        owner = {
            "target_identity": manifest["target_identity"],
            "process_id": os.getpid(),
        }
        ack = self.ack(
            manifest,
            input_refs=manifest["inputs"],
            output_refs=[output],
            owner_reference=owner,
            publication_generation_reference="generation:001",
        )
        self.assertEqual(ack["output_refs"], [output])
        for changes in (
            {"validation": "PENDING"},
            {"fingerprint": "bad"},
            {"version": True},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.ack(output_refs=[{**output, **changes}])
        with self.assertRaises(ValueError):
            self.ack(owner_reference={**owner, "target_identity": "bad"})

    def test_save_reload_identical_replay_and_temporary_cleanup(self):
        manifest = self.manifest()
        path = self.store.save_manifest(manifest)
        first_bytes = path.read_bytes()
        inode = path.stat().st_ino
        fresh = pm.LocalProcessingStore(self.store.root, self.config)
        self.assertEqual(fresh.load_manifest(manifest["manifest_id"]), manifest)
        self.assertEqual(fresh.save_manifest(manifest), path)
        self.assertEqual(path.stat().st_ino, inode)
        self.assertEqual(path.read_bytes(), first_bytes)
        self.assertEqual(list(path.parent.glob("*.tmp")), [])
        self.assertEqual(len(list(path.parent.glob("*.json"))), 1)
        self.assertTrue(path.is_relative_to(self.root))

    def test_changed_canonical_payload_under_logical_id_rejected_without_overwrite(
        self,
    ):
        manifest = self.manifest()
        path = self.store.save_manifest(manifest)
        original = path.read_bytes()
        changed = self.manifest(created_at=LATER)
        self.assertEqual(changed["manifest_id"], manifest["manifest_id"])
        with self.assertRaises(FileExistsError):
            self.store.save_manifest(changed)
        self.assertEqual(path.read_bytes(), original)
        self.assertEqual(list(path.parent.glob("*.tmp")), [])
        tampered = copy.deepcopy(manifest)
        tampered["processing_spec"]["window_days"] = 31
        with self.assertRaises(ValueError):
            self.store.save_manifest(tampered)

    def test_interruption_before_link_leaves_no_partial_record_and_releases_owner(self):
        manifest = self.manifest()
        with (
            patch.object(pm.os, "link", side_effect=InterruptedError("injected")),
            self.assertRaises(InterruptedError),
        ):
            self.store.save_manifest(manifest)
        self.assertEqual(list(self.store.root.rglob("*.json")), [])
        self.assertEqual(list(self.store.root.rglob("*.tmp")), [])
        self.store.save_manifest(manifest)
        self.assertEqual(self.store.load_manifest(manifest["manifest_id"]), manifest)

    def test_process_exit_after_file_fsync_before_publication_leaves_only_ignored_temp(
        self,
    ):
        manifest = self.manifest()
        request = self.root / "request.json"
        request.write_text(pm.canonical_json(manifest))
        script = """
import os, sys
from pathlib import Path
from types import SimpleNamespace
from orchestration.local import processing_manifest as pm
root = Path(sys.argv[1])
config = SimpleNamespace(spark=SimpleNamespace(paths=SimpleNamespace(base=str(root/'lake'), gold=str(root/'lake'/'gold'))), warehouse=None)
store = pm.LocalProcessingStore(root/'control', config)
pm.os.link = lambda *args, **kwargs: os._exit(77)
store.save_manifest(pm.parse_json((root/'request.json').read_bytes()))
"""
        result = subprocess.run(
            [sys.executable, "-c", script, str(self.root)],
            cwd=pm.ROOT,
            timeout=20,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 77, result.stderr.decode())
        self.assertEqual(list(self.store.root.rglob("*.json")), [])
        self.assertEqual(len(list(self.store.root.rglob("*.tmp"))), 1)
        self.store.save_manifest(manifest)
        self.assertEqual(self.store.load_acknowledgements(manifest["manifest_id"]), [])

    def test_file_flush_precedes_link_and_directory_flush_follows_link(self):
        events = []
        link, fsync = pm.os.link, pm.os.fsync

        def record_sync(fd):
            events.append("fsync")
            return fsync(fd)

        def record_link(*args, **kwargs):
            events.append("link")
            return link(*args, **kwargs)

        with (
            patch.object(pm.os, "fsync", side_effect=record_sync),
            patch.object(pm.os, "link", side_effect=record_link),
        ):
            self.store.save_manifest(self.manifest())
        position = events.index("link")
        self.assertEqual(events[position - 1], "fsync")
        self.assertEqual(events[position + 1], "fsync")

    def test_fsync_failure_before_link_publishes_nothing(self):
        (self.store.root / "manifests").mkdir(parents=True)
        fsync = pm.os.fsync

        def fail_file_sync(descriptor):
            if stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise OSError("injected")
            return fsync(descriptor)

        with (
            patch.object(pm.os, "fsync", side_effect=fail_file_sync),
            self.assertRaises(OSError),
        ):
            self.store.save_manifest(self.manifest())
        self.assertEqual(list(self.store.root.rglob("*.json")), [])
        self.assertEqual(list(self.store.root.rglob("*.tmp")), [])

    def test_directory_fsync_failure_after_link_is_explicit_and_identical_retry_reconciles(
        self,
    ):
        manifest = self.manifest()
        (self.store.root / "manifests").mkdir(parents=True)
        directory_sync = pm._directory_sync
        path = (
            self.store.root
            / "manifests"
            / (manifest["manifest_id"].removeprefix("sha256:") + ".json")
        )

        def fail_after_publication(directory):
            if path.exists():
                raise OSError("injected")
            return directory_sync(directory)

        with (
            patch.object(pm, "_directory_sync", side_effect=fail_after_publication),
            self.assertRaises(OSError),
        ):
            self.store.save_manifest(manifest)
        self.assertEqual(self.store.load_manifest(manifest["manifest_id"]), manifest)
        self.store.save_manifest(manifest)
        self.assertEqual(len(list(self.store.root.rglob("*.json"))), 1)

    def test_failed_directory_creation_flush_is_reconciled_on_retry(self):
        sync = pm._directory_sync
        flushed = []

        def fail_new_root_parent(path):
            if self.store.root.exists() and path == self.store.root.parent:
                raise OSError("injected")
            return sync(path)

        with (
            patch.object(pm, "_directory_sync", side_effect=fail_new_root_parent),
            self.assertRaises(OSError),
        ):
            self.store.save_manifest(self.manifest())
        self.assertTrue(self.store.root.exists())
        self.assertEqual(list(self.store.root.rglob("*.json")), [])

        def record_sync(path):
            flushed.append(path)
            return sync(path)

        with patch.object(pm, "_directory_sync", side_effect=record_sync):
            self.store.save_manifest(self.manifest())
        self.assertIn(self.store.root.parent, flushed)

    def test_concurrent_conflicting_publication_never_overwrites_winner(self):
        # Exercise the no-replace primitive directly; public writes also hold ownership.
        values = [self.manifest(), self.manifest(created_at=LATER)]
        directory = self.store.root / "manifests"
        directory.mkdir(parents=True)
        path = directory / (values[0]["manifest_id"].removeprefix("sha256:") + ".json")
        barrier = threading.Barrier(2)

        def publish(value):
            barrier.wait(timeout=5)
            try:
                self.store._publish(path, value, "manifest")
                return value
            except FileExistsError:
                return None

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(publish, values))
        winners = [value for value in results if value is not None]
        self.assertEqual(len(winners), 1)
        self.assertEqual(self.store.load_manifest(values[0]["manifest_id"]), winners[0])
        self.assertEqual(list(directory.glob("*.tmp")), [])

    def test_corrupted_duplicate_and_truncated_saved_json_fail_explicitly(self):
        manifest = self.manifest()
        path = self.store.save_manifest(manifest)
        mutations = [
            b'{"manifest_id":',
            b'{"manifest_id":"x","manifest_id":"y"}',
            pm.canonical_json({**manifest, "created_at": LATER}).encode(),
        ]
        for raw in mutations:
            with self.subTest(raw=raw[:32]):
                path.write_bytes(raw)
                with self.assertRaises(ValueError):
                    self.store.load_manifest(manifest["manifest_id"])
                with self.assertRaises(ValueError):
                    self.store.save_manifest(manifest)
                self.assertEqual(path.read_bytes(), raw)

    def test_append_only_history_duplicates_and_distinct_attempts_survive_reload(self):
        manifest = self.manifest()
        self.store.save_manifest(manifest)
        records = [
            self.ack(outcome="STARTED", started_at=STAMP, completed_at=None),
            self.ack(),
            self.ack(attempt_id="attempt:two"),
        ]
        paths = [self.store.append_ack(record) for record in records]
        snapshots = [p.read_bytes() for p in paths]
        for record in records:
            self.store.append_ack(record)
        fresh = pm.LocalProcessingStore(self.store.root, self.config)
        self.assertEqual(
            {a["ack_id"] for a in fresh.load_acknowledgements(manifest["manifest_id"])},
            {a["ack_id"] for a in records},
        )
        self.assertEqual([p.read_bytes() for p in paths], snapshots)
        self.assertEqual(len(list((self.store.root / "acks").rglob("*.json"))), 3)

    def test_ack_conflict_or_corruption_cannot_overwrite_history(self):
        manifest = self.manifest()
        self.store.save_manifest(manifest)
        ack = self.ack()
        path = self.store.append_ack(ack)
        original = path.read_bytes()
        with self.assertRaises(ValueError):
            self.store.append_ack({**ack, "stage": "publish"})
        self.assertEqual(path.read_bytes(), original)
        path.write_text("{}")
        with self.assertRaises(ValueError):
            self.store.load_acknowledgements(manifest["manifest_id"])
        with self.assertRaises(ValueError):
            self.store.append_ack(ack)
        self.assertEqual(path.read_text(), "{}")

    def test_ack_cannot_rewrite_collection_outcome_or_declare_unlisted_input(self):
        manifest = self.manifest(acquisition=self.partial(), readiness="PARTIAL_READY")
        self.store.save_manifest(manifest)
        ack = self.ack(manifest)
        ack["collection_outcome"] = "success"
        ack["ack_id"] = "sha256:" + pm.content_hash(
            {k: v for k, v in ack.items() if k != "ack_id"}
        )
        with self.assertRaises(ValueError):
            self.store.append_ack(ack)
        with self.assertRaises(ValueError):
            self.ack(input_refs=[{**self.ref, "content_sha256": "f" * 64}])
        self.assertEqual(self.store.load_acknowledgements(manifest["manifest_id"]), [])

    def test_guard_acquires_and_reuses_only_legitimate_active_context(self):
        manifest = self.manifest()
        with analytical_ownership(self.config) as owner:
            with self.assertRaises(RuntimeError):
                self.store.save_manifest(manifest)
            self.assertFalse(self.store.root.exists())
            with self.assertRaises(ValueError):
                self.store.save_manifest(
                    manifest, ownership=AnalyticalOwnership(owner.target_identity)
                )
            self.store.save_manifest(manifest, ownership=owner)
            self.store.append_ack(self.ack(), ownership=owner)
            subset = self.manifest(include_group=False)
            subset_store = pm.LocalProcessingStore(
                self.root / "subset-control", self.config, include_group=False
            )
            subset_store.save_manifest(subset, ownership=owner)
            ack = self.ack(
                subset,
                owner_reference={
                    "target_identity": owner.target_identity,
                    "process_id": os.getpid(),
                },
            )
            self.assertNotEqual(
                ack["owner_reference"]["target_identity"], subset["target_identity"]
            )
            subset_store.append_ack(ack, ownership=owner)
        with self.assertRaises(ValueError):
            self.store.append_ack(self.ack(attempt_id="attempt:two"), ownership=owner)
        self.assertEqual(
            len(self.store.load_acknowledgements(manifest["manifest_id"])), 1
        )

    def test_store_rejects_target_drift_or_foreign_manifest_before_metadata_write(self):
        config = copy.deepcopy(self.config)
        config.spark.paths.gold = str(self.root / "different")
        with self.assertRaises(ValueError):
            self.store.save_manifest(self.manifest(config=config))
        self.assertFalse(self.store.root.exists())
        manifest = self.manifest()
        self.config.spark.paths.gold = str(self.root / "different")
        with self.assertRaises(ValueError):
            self.store.save_manifest(manifest)
        self.assertFalse(self.store.root.exists())

    def test_unsafe_roots_id_paths_and_storage_overlap_rejected(self):
        roots = [
            self.root / ".." / "escape",
            "file:///tmp/control",
            Path(self.config.spark.paths.base),
            Path(self.config.spark.paths.gold) / "metadata",
        ]
        for root in roots:
            with self.subTest(root=str(root)), self.assertRaises(ValueError):
                pm.LocalProcessingStore(root, self.config)
        for identity in (
            "../outside",
            "sha256:" + "a" * 64 + "/../escape",
            "sha256:" + "A" * 64,
        ):
            with self.subTest(identity=identity), self.assertRaises(ValueError):
                self.store.load_manifest(identity)
        self.assertFalse(self.store.root.exists())

    def test_symlink_roots_directories_and_existing_records_rejected(self):
        real = self.root / "real"
        real.mkdir()
        linked = self.root / "linked"
        linked.symlink_to(real, target_is_directory=True)
        with self.assertRaises(ValueError):
            pm.LocalProcessingStore(linked / "child", self.config)
        self.store.root.mkdir()
        (self.store.root / "manifests").symlink_to(real, target_is_directory=True)
        with self.assertRaises(ValueError):
            self.store.save_manifest(self.manifest())
        (self.store.root / "manifests").unlink()
        path = self.store.save_manifest(self.manifest())
        path.unlink()
        path.symlink_to(self.artifact)
        original = self.artifact.read_bytes()
        with self.assertRaises(ValueError):
            self.store.save_manifest(self.manifest())
        self.assertEqual(self.artifact.read_bytes(), original)

    def test_input_and_output_uri_traversal_credentials_or_control_data_rejected(self):
        for uri in (
            "file:///tmp/../escape",
            "file://remote/tmp/artifact",
            "https://user:password@host/input",
            "https://host/input?token=secret",
            "file:///tmp/%2e%2e/escape",
            "file:///tmp/%00",
            "file:///tmp/a%5Cb",
        ):
            with self.subTest(uri=uri), self.assertRaises(ValueError):
                self.manifest(inputs=[{**self.ref, "artifact_uri": uri}])


if __name__ == "__main__":
    unittest.main()
