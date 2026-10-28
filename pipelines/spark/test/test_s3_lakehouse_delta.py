"""Actual isolated Delta ingestion, generations, failure isolation and rollback."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dsp_collector.runtime.outbox import enqueue, jobs
from dsp_spark.io.delta import merge_delta, read_delta
from dsp_spark.io.intelligence_clickhouse import publish_intelligence
from dsp_spark.io.lakehouse_control import LocalControl
from dsp_spark.jobs.batch.ingest_bronze import BRONZE_SCHEMA, ingest_bronze
from dsp_spark.jobs.batch.lakehouse_generation import (
    build_generation,
    generation_settings,
    rollback,
)
from dsp_spark.jobs.batch.s3_lakehouse import ENVELOPES, bootstrap, process_once
from dsp_spark.processing.intelligence.catalog import GOLD, SILVER
from dsp_spark.runtime.config import LakehousePaths, SparkSettings
from dsp_spark.runtime.spark import create_spark
from test_bronze_observation_delta import youtube


class LakehouseDeltaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(
            SparkSettings(
                LakehousePaths("file:///tmp/dsp-synthetic-generation-session")
            )
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_historical_catchup_overlap_and_partial_bootstrap_use_pinned_versions(self):
        with tempfile.TemporaryDirectory(
            prefix="dsp-synthetic-bootstrap-"
        ) as directory:
            root = Path(directory)
            historical, catchup, target = [
                SparkSettings(LakehousePaths((root / n).as_uri()))
                for n in ("historical", "catchup", "target")
            ]
            policies = json.loads(
                (
                    Path(__file__).resolve().parents[3]
                    / "configs/collection/s3_synthetic_policy.example.json"
                ).read_text()
            )
            # Artificial reviewed policy for isolated local migration fixtures.
            policies = {k: {**v, "synthetic_only": False} for k, v in policies.items()}

            def ingest(settings, run, video):
                raw = youtube(run)
                raw["videos"][0]["id"] = video
                raw["comments"] = []
                artifact = root / (run + ".json")
                artifact.write_text(json.dumps(raw))
                ingest_bronze(
                    self.spark, artifact, settings, source="youtube", policies=policies
                )

            ingest(historical, "synthetic-historical", "v1")
            ingest(catchup, "synthetic-historical", "v1")
            ingest(catchup, "synthetic-catchup", "v2")
            for settings in (historical, catchup):
                for name in ENVELOPES[:2]:
                    merge_delta(
                        self.spark.createDataFrame([], BRONZE_SCHEMA),
                        getattr(settings.paths, name),
                        keys=("source", "run_id"),
                        initialize_empty=True,
                    )
            control = LocalControl(target.paths.base)
            calls = 0

            def interrupted(*args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise RuntimeError("synthetic interrupted bootstrap")
                return merge_delta(*args, **kwargs)

            with (
                patch(
                    "dsp_spark.jobs.batch.s3_lakehouse.merge_delta",
                    side_effect=interrupted,
                ),
                control.writer(),
                self.assertRaisesRegex(RuntimeError, "interrupted"),
            ):
                bootstrap(
                    self.spark,
                    target,
                    control,
                    historical.paths,
                    catchup=catchup.paths,
                    policies=policies,
                )
            self.assertIsNone(control.get("bootstrap.json")[0])
            plan = control.get("bootstrap-plan.json")[0]
            ingest(catchup, "synthetic-late", "v3")
            with control.writer():
                report = bootstrap(
                    self.spark,
                    target,
                    control,
                    historical.paths,
                    catchup=catchup.paths,
                    policies=policies,
                )
            self.assertEqual(
                read_delta(self.spark, target.paths.bronze_social_posts).count(), 2
            )
            self.assertEqual(
                report["tables"]["bronze_social_posts"]["catchup_version"],
                plan["catchup_versions"]["bronze_social_posts"],
            )
            self.assertEqual(report["status"], "VALIDATED")

    def test_ingestion_full_rebuild_retry_new_input_failure_and_rollback(self):
        with tempfile.TemporaryDirectory(
            prefix="dsp-synthetic-lakehouse-"
        ) as directory:
            root = Path(directory)
            settings = SparkSettings(LakehousePaths((root / "lake").as_uri()))
            control = LocalControl(settings.paths.base)
            policies = json.loads(
                (
                    Path(__file__).resolve().parents[3]
                    / "configs/collection/s3_synthetic_policy.example.json"
                ).read_text()
            )
            queue = root / "outbox"
            artifact = root / "synthetic-first.json"
            artifact.write_text(json.dumps(youtube("synthetic-first")))
            enqueue(artifact, queue)
            first = process_once(
                self.spark,
                settings,
                control,
                queue,
                policies=policies,
                synthetic=True,
                as_of="2026-10-12T00:00:00Z",
            )
            report = first["generation"]
            self.assertEqual(report["status"], "VALIDATED")
            self.assertEqual(set(report["after"]), set(SILVER + GOLD))
            self.assertTrue(
                all(r["unexplained"] == 0 for r in report["reconciliation"].values())
            )
            self.assertEqual(next(jobs(queue))[1]["status"], "committed")
            enqueue(artifact, queue)
            replay = process_once(
                self.spark,
                settings,
                control,
                queue,
                policies=policies,
                synthetic=True,
                as_of="2026-10-12T00:00:00Z",
            )
            self.assertEqual(
                replay["generation"]["generation_id"], report["generation_id"]
            )
            self.assertEqual(
                replay["generation"]["execution"], "UNCHANGED_PINNED_INPUT"
            )
            before = control.get("current.json")[0]
            # Exercise the real downstream adapter through its validation gates,
            # then fail the mocked serving boundary without any network writes.
            selected = generation_settings(
                settings, settings.paths.base, report["generation_id"]
            )
            with (
                patch(
                    "dsp_spark.jobs.batch.build_intelligence.load_policies",
                    return_value=policies,
                ),
                patch(
                    "dsp_spark.io.intelligence_clickhouse.replace_intelligence_serving",
                    side_effect=RuntimeError("synthetic ClickHouse outage"),
                ),
                self.assertRaisesRegex(RuntimeError, "ClickHouse outage"),
            ):
                publish_intelligence(self.spark, selected, object())
            self.assertEqual(control.get("current.json")[0], before)
            # Fault injection is local/synthetic, after staging starts but before
            # publication. Existing validated data must remain selected.
            with (
                patch(
                    "dsp_spark.jobs.batch.lakehouse_generation.verify_generation",
                    side_effect=[None, RuntimeError("staging failed")],
                ),
                control.writer(),
                self.assertRaisesRegex(RuntimeError, "staging"),
            ):
                build_generation(
                    self.spark,
                    settings,
                    control,
                    policies=policies,
                    as_of="2026-10-12T00:00:00Z",
                    force=True,
                )
            self.assertEqual(control.get("current.json")[0], before)
            second_artifact = root / "synthetic-second.json"
            raw = youtube("synthetic-second", at="2026-10-12T01:00:00Z")
            raw["videos"][0]["id"] = "v2"
            raw["comments"] = []
            second_artifact.write_text(json.dumps(raw))
            enqueue(second_artifact, queue)
            second = process_once(
                self.spark,
                settings,
                control,
                queue,
                policies=policies,
                synthetic=True,
                as_of="2026-10-12T02:00:00Z",
            )["generation"]
            self.assertNotEqual(second["generation_id"], report["generation_id"])
            self.assertEqual(second["after"]["silver_social_posts"]["rows"], 2)
            self.assertEqual(second["rollback_target"], report["generation_id"])
            with control.writer():
                rollback(self.spark, settings, control, report["generation_id"])
            self.assertTrue(control.get("current.json")[0]["paused"])
            with self.assertRaisesRegex(ValueError, "paused"):
                process_once(
                    self.spark,
                    settings,
                    control,
                    queue,
                    policies=policies,
                    synthetic=True,
                )
