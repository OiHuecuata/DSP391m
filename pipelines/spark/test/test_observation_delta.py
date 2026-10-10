"""Actual isolated Delta, lineage and native admission failure/recovery tests."""

import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [
    str(ROOT),
    str(ROOT / "pipelines/spark/src"),
    str(ROOT / "apps/collector/src"),
]

from dsp_collector.jobs.collect_courts import process_pbplus_envelope
from dsp_collector.jobs.collect_social import process_threads_envelope
from dsp_collector.jobs.collect_youtube import process_youtube_envelope
from dsp_collector.normalizers.observation import parse_json, run_envelope_context
from dsp_spark.io.delta import read_delta, write_delta
from dsp_spark.jobs.batch.ingest_observations import (
    ingest_observations,
    publish_verified_social,
    verify_silver_lineage,
)
from dsp_spark.jobs.batch.ingest_pbplus import ingest_pbplus
from dsp_spark.processing.clean.contracts import contract_documents, validation_reason
from dsp_spark.runtime.config import LakehousePaths, SparkSettings
from dsp_spark.runtime.spark import create_spark
from pyspark.sql import functions as F
from pyspark.sql import types as T

FIXTURE = ROOT / "tests/fixtures/pipeline/native-source-artifacts.v1.json"


class ObservationDeltaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(
            SparkSettings(LakehousePaths("file:///tmp/dsp-native-runtime-only"))
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="dsp-native-delta-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.settings = SparkSettings(LakehousePaths((self.root / "lake").as_uri()))
        self.artifacts = json.loads(FIXTURE.read_text())
        self.context = run_envelope_context(
            permission_reference="fixture:" + str(FIXTURE.relative_to(ROOT)),
            retention_policy_id="synthetic-local-testing",
        )

    def collect(self, source="threads", *, envelope=None, directory="artifacts"):
        processor = {
            "threads": process_threads_envelope,
            "pbplus": process_pbplus_envelope,
            "youtube": process_youtube_envelope,
        }[source]
        return processor(
            envelope or self.artifacts[source],
            artifact_root=self.root / directory,
            observation_context=self.context,
        )

    def rows(self):
        return sorted(
            [
                r.asDict(recursive=True)
                for r in read_delta(
                    self.spark, self.settings.paths.record_observations
                ).collect()
            ],
            key=lambda r: r["observation_id"],
        )

    def test_three_sources_real_ingestion_exact_payload_provenance_and_lineage(self):
        results = []
        for source in ["pbplus", "threads", "youtube"]:
            result = self.collect(source)
            results.append(result)
            ack = ingest_observations(self.spark, result.bronze_path, self.settings)
            self.assertEqual(ack["observation_count"], 1 if source == "pbplus" else 2)
            self.assertEqual(
                len(ack["observation_references"]), ack["observation_count"]
            )
            verify_silver_lineage(
                self.spark, parse_json(result.silver_path.read_bytes()), self.settings
            )
            archived = (
                read_delta(self.spark, getattr(self.settings.paths, source + "_bronze"))
                .first()
                .payload_json
            )
            self.assertEqual(archived.encode(), result.bronze_path.read_bytes())
        rows = self.rows()
        self.assertEqual(len(rows), 5)
        for row in rows:
            original = next(r for r in results if r.run_id == row["collection_run_id"])
            self.assertEqual(
                row["artifact_sha256"],
                hashlib.sha256(original.bronze_path.read_bytes()).hexdigest(),
            )
            self.assertEqual(row["origin"], "SYNTHETIC")
        court = results[0]
        ingest_pbplus(self.spark, court.silver_path, self.settings)
        self.assertEqual(read_delta(self.spark, self.settings.paths.courts).count(), 1)

    def test_replay_at_other_uri_preserves_rows_ids_and_first_uri(self):
        result = self.collect("youtube")
        before = ingest_observations(self.spark, result.bronze_path, self.settings)
        rows = self.rows()
        path = self.root / "copied.json"
        path.write_bytes(result.bronze_path.read_bytes())
        after = ingest_observations(self.spark, path, self.settings)
        self.assertEqual(before["observation_ids"], after["observation_ids"])
        self.assertEqual(rows, self.rows())

    def test_conflicting_payload_under_same_observation_rejected_before_new_archive(
        self,
    ):
        result = self.collect("pbplus")
        ingest_observations(self.spark, result.bronze_path, self.settings)
        before = self.rows()
        changed = parse_json(result.bronze_path.read_bytes())
        changed["posts"][0]["number_of_courts"] = 12
        path = self.root / "conflict.json"
        path.write_text(json.dumps(changed))
        with (
            patch("dsp_spark.jobs.batch.ingest_bronze.merge_delta") as archive,
            self.assertRaisesRegex(ValueError, "Conflicting native"),
        ):
            ingest_observations(self.spark, path, self.settings)
        archive.assert_not_called()
        self.assertEqual(before, self.rows())

    def test_interruption_after_raw_commit_never_acknowledges_and_retry_recovers(self):
        result = self.collect()
        from dsp_spark.jobs.batch import ingest_observations as module

        original = module.merge_delta

        def fail_native(frame, path, **kwargs):
            if path == self.settings.paths.record_observations:
                raise OSError("native commit interrupted")
            return original(frame, path, **kwargs)

        with (
            patch(
                "dsp_spark.jobs.batch.ingest_observations.merge_delta",
                side_effect=fail_native,
            ),
            self.assertRaises(OSError),
        ):
            ingest_observations(self.spark, result.bronze_path, self.settings)
        self.assertEqual(
            read_delta(self.spark, self.settings.paths.threads_bronze).count(), 1
        )
        self.assertEqual(
            read_delta(self.spark, self.settings.paths.observation_artifacts).count(), 1
        )
        factory = Mock()
        with (
            patch("dsp_collector.publishers.kafka.KafkaPublisher", factory),
            self.assertRaisesRegex(ValueError, "observations missing"),
        ):
            publish_verified_social(
                self.spark, result.silver_path, self.settings, kafka_settings=None
            )
        factory.assert_not_called()
        ack = ingest_observations(self.spark, result.bronze_path, self.settings)
        self.assertEqual(ack["observation_count"], 2)
        verify_silver_lineage(
            self.spark, parse_json(result.silver_path.read_bytes()), self.settings
        )

    def test_readback_incomplete_cannot_acknowledge_even_after_commit(self):
        result = self.collect("pbplus")
        from dsp_spark.jobs.batch import ingest_observations as module

        original = module._saved
        calls = []

        def saved(*args):
            value = original(*args)
            calls.append(value)
            return {} if len(calls) == 3 else value

        with (
            patch.object(module, "_saved", side_effect=saved),
            self.assertRaisesRegex(ValueError, "readback incomplete"),
        ):
            ingest_observations(self.spark, result.bronze_path, self.settings)
        self.assertEqual(len(self.rows()), 1)
        ingest_observations(self.spark, result.bronze_path, self.settings)

    def test_another_run_does_not_supply_native_lineage(self):
        result = self.collect("youtube")
        changed = copy.deepcopy(self.artifacts["youtube"])
        changed["run_id"] = "another-run"
        other = self.collect("youtube", envelope=changed, directory="other")
        ingest_observations(self.spark, other.bronze_path, self.settings)
        with self.assertRaisesRegex(ValueError, "Bronze missing"):
            verify_silver_lineage(
                self.spark, parse_json(result.silver_path.read_bytes()), self.settings
            )

    def test_silver_content_mutation_fails_before_publish_and_court_write(self):
        for source in ["pbplus", "threads", "youtube"]:
            result = self.collect(source)
            ingest_observations(self.spark, result.bronze_path, self.settings)
            batch = parse_json(result.silver_path.read_bytes())
            batch["records"][0][
                "court_name" if source == "pbplus" else "text"
            ] = "source-inconsistent text"
            with self.assertRaisesRegex(ValueError, "transformation lineage"):
                verify_silver_lineage(self.spark, batch, self.settings)
            if source == "pbplus":
                path = self.root / "changed-silver.json"
                path.write_text(json.dumps(batch))
                with (
                    patch("dsp_spark.jobs.batch.ingest_pbplus.merge_delta") as write,
                    self.assertRaises(ValueError),
                ):
                    ingest_pbplus(self.spark, path, self.settings)
                write.assert_not_called()

    def test_empty_native_batches_initialize_explicit_schema_and_replay(self):
        empty = copy.deepcopy(self.artifacts["pbplus"])
        empty.update(count=0, posts=[])
        result = self.collect("pbplus", envelope=empty)
        for _ in range(2):
            ack = ingest_observations(self.spark, result.bronze_path, self.settings)
            self.assertEqual(ack["observation_count"], 0)
            verify_silver_lineage(
                self.spark, parse_json(result.silver_path.read_bytes()), self.settings
            )
        self.assertEqual(self.rows(), [])

    def test_stored_duplicate_is_rejected_and_not_silently_deduplicated(self):
        result = self.collect("pbplus")
        ingest_observations(self.spark, result.bronze_path, self.settings)
        frame = read_delta(self.spark, self.settings.paths.record_observations)
        write_delta(frame, self.settings.paths.record_observations)
        with self.assertRaisesRegex(ValueError, "Duplicate stored"):
            ingest_observations(self.spark, result.bronze_path, self.settings)

    def test_source_payload_bytes_change_breaks_silver_reference(self):
        result = self.collect("youtube")
        ingest_observations(self.spark, result.bronze_path, self.settings)
        batch = parse_json(result.silver_path.read_bytes())
        result.bronze_path.write_bytes(result.bronze_path.read_bytes() + b" ")
        with self.assertRaisesRegex(ValueError, "bytes changed"):
            verify_silver_lineage(self.spark, batch, self.settings)

    def test_executor_validation_uses_packaged_serialized_contract(self):
        result = self.collect("youtube")
        ingest_observations(self.spark, result.bronze_path, self.settings)
        docs = contract_documents()
        reason = F.udf(
            lambda raw: validation_reason(raw, "observation", docs), T.StringType()
        )
        frame = self.spark.createDataFrame(
            [(json.dumps(self.rows()[0]),)], "raw string"
        )
        self.assertIsNone(frame.select(reason("raw")).first()[0])

    def test_committed_observations_reopen_in_new_spark_session(self):
        result = self.collect("youtube")
        ingest_observations(self.spark, result.bronze_path, self.settings)
        before = self.rows()
        self.spark.stop()
        type(self).spark = create_spark(self.settings)
        self.assertEqual(before, self.rows())
        verify_silver_lineage(
            self.spark, parse_json(result.silver_path.read_bytes()), self.settings
        )

    def test_observation_refs_fit_existing_manifest_and_append_ack_store(self):
        from types import SimpleNamespace

        from orchestration.local.processing_manifest import (
            LocalProcessingStore,
            build_ack,
            build_manifest,
            content_hash,
        )

        result = self.collect("pbplus")
        ack = ingest_observations(self.spark, result.bronze_path, self.settings)
        config = SimpleNamespace(spark=self.settings, warehouse=None)
        manifest = build_manifest(
            inputs=ack["observation_references"],
            config=config,
            readiness="READY",
            created_at="2026-10-05T12:00:00Z",
            acquisition={
                "outcome": "success",
                "accepted_records": 1,
                "coverage": {"status": "UNKNOWN"},
                "runs": [
                    {
                        "source_platform": "pbplus",
                        "collection_run_id": result.run_id,
                        "outcome": "success",
                        "accepted_records": 1,
                    }
                ],
            },
            processing_spec={
                "analysis_end_date": "2026-10-05",
                "window_days": 30,
                "processing_contract_version": "record-observation.v1",
                "code_version": "fixture",
                "configuration_hash": content_hash({}),
                "reference_version": None,
                "feature_version": None,
                "policy_version": "synthetic-local-testing",
                "tombstone_version": None,
            },
        )
        store = LocalProcessingStore(self.root / "control", config)
        store.save_manifest(manifest)
        event = build_ack(
            manifest,
            attempt_id="attempt:fixture",
            stage="bronze",
            outcome="SUCCESS",
            completed_at="2026-10-05T12:01:00Z",
            input_refs=ack["observation_references"],
        )
        store.append_ack(event)
        self.assertEqual(store.load_manifest(manifest["manifest_id"]), manifest)
        self.assertEqual(store.load_acknowledgements(manifest["manifest_id"]), [event])

    def test_multiple_queries_and_fetches_in_same_run_persist_without_v1_run_collision(
        self,
    ):
        native_keys = set()
        obs_ids = set()
        for query, fetch in [("q1", "f1"), ("q2", "f1"), ("q1", "f2")]:
            envelope = copy.deepcopy(self.artifacts["pbplus"])
            envelope["native_observation_context"] = {
                **self.context,
                "provenance_scope": "FETCH_BATCH",
                "query_id": query,
                "fetch_batch_id": fetch,
                "observed_at": "2026-10-05T11:59:00.000000Z",
            }
            path = self.root / (query + fetch + ".json")
            path.write_text(json.dumps(envelope))
            ack = ingest_observations(self.spark, path, self.settings)
            self.assertEqual(ack["observation_count"], 1)
            obs_ids.update(ack["observation_ids"])
        self.assertEqual(len(obs_ids), 3)
        rows = self.rows()
        native_keys.update(r["native_record_key"] for r in rows)
        self.assertEqual(len(native_keys), 1)
        self.assertEqual(
            read_delta(self.spark, self.settings.paths.observation_artifacts).count(), 3
        )
        self.assertFalse((self.root / "lake/bronze/pbplus/_delta_log").exists())

    def test_empty_fetch_retains_exact_artifact_and_no_false_native_rows(self):
        envelope = copy.deepcopy(self.artifacts["pbplus"])
        envelope.update(count=0, posts=[])
        envelope["native_observation_context"] = {
            **self.context,
            "provenance_scope": "FETCH_BATCH",
            "query_id": "q1",
            "fetch_batch_id": "empty",
            "observed_at": "2026-10-05T11:59:00.000000Z",
        }
        path = self.root / "empty-fetch.json"
        path.write_text(json.dumps(envelope))
        for _ in range(2):
            ack = ingest_observations(self.spark, path, self.settings)
            self.assertEqual(ack["observation_count"], 0)
        self.assertEqual(
            read_delta(self.spark, self.settings.paths.observation_artifacts).count(), 1
        )
        self.assertEqual(
            read_delta(self.spark, self.settings.paths.observation_artifacts)
            .first()
            .payload_json.encode(),
            path.read_bytes(),
        )

    def test_fetch_scoped_silver_cannot_enter_transport_that_omits_fetch_identity(self):
        context = {
            **self.context,
            "provenance_scope": "FETCH_BATCH",
            "query_id": "q1",
            "fetch_batch_id": "f1",
            "observed_at": "2026-10-05T11:59:00.000000Z",
        }
        result = process_threads_envelope(
            self.artifacts["threads"],
            artifact_root=self.root / "fetch",
            observation_context=context,
        )
        ingest_observations(self.spark, result.bronze_path, self.settings)
        with self.assertRaisesRegex(ValueError, "v1 omits fetch identity"):
            verify_silver_lineage(
                self.spark, parse_json(result.silver_path.read_bytes()), self.settings
            )


if __name__ == "__main__":
    unittest.main()
