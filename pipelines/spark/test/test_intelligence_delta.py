"""Real Spark/Delta integration with synthetic, isolated Bronze fixtures."""

import json
import tempfile
import unittest
from pathlib import Path

from dsp_spark.io.delta import read_delta, reconcile_delta
from dsp_spark.jobs.batch.build_intelligence import (
    assert_generation,
    build_intelligence,
)
from dsp_spark.jobs.batch.ingest_bronze import ingest_bronze
from dsp_spark.processing.clean.source_lifecycle import permitted_legacy_frame
from dsp_spark.processing.intelligence.catalog import GOLD, SILVER
from dsp_spark.runtime.config import LakehousePaths, SparkSettings
from dsp_spark.runtime.spark import create_spark
from test_bronze_observation_delta import youtube


class IntelligenceDeltaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(
            SparkSettings(LakehousePaths("file:///tmp/dsp-intelligence-session"))
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_end_to_end_contracts_replay_and_generation_guard(self):
        with tempfile.TemporaryDirectory(
            prefix="dsp-synthetic-intelligence-"
        ) as directory:
            root = Path(directory)
            settings = SparkSettings(LakehousePaths((root / "lake").as_uri()))
            artifact = root / "synthetic-youtube.json"
            raw = youtube()
            raw["videos"][0]["snippet"][
                "title"
            ] = "Khai trương sân ABC Pickleball, 6 sân, giá 100k/giờ, tại 123 Nguyễn Văn Linh, Đà Nẵng"
            raw["comments"][0]["payload"]["snippet"].update(
                textOriginal="Tìm sân pickleball ở Huế",
                publishedAt="2026-10-10T00:00:00Z",
            )
            artifact.write_text(json.dumps(raw))
            ingest_bronze(self.spark, artifact, settings, source="youtube")
            first = build_intelligence(
                self.spark, settings, as_of="2026-10-12T00:00:00Z"
            )
            self.assertEqual(first["status"], "VALIDATED")
            self.assertEqual(set(first["after"]), set(SILVER + GOLD))
            self.assertEqual(first["after"]["silver_social_posts"]["rows"], 1)
            self.assertEqual(first["after"]["silver_social_comments"]["rows"], 2)
            candidates = read_delta(self.spark, settings.paths.silver_venue_candidates)
            self.assertEqual(
                candidates.schema["price_min_vnd"].dataType.simpleString(),
                "decimal(20,2)",
            )
            self.assertEqual(str(candidates.first().price_min_vnd), "100000.00")
            replay = build_intelligence(
                self.spark, settings, as_of="2026-10-12T00:00:00Z"
            )
            self.assertEqual(replay["execution"], "UNCHANGED_INPUT_NO_WRITES")
            self.assertEqual(replay["output_versions"], first["output_versions"])
            assert_generation(self.spark, settings, replay)
            kind = "social_intent_daily_v2"
            frame = read_delta(self.spark, getattr(settings.paths, kind))
            # Simulate an external writer after validated materialization.
            frame.write.format("delta").mode("append").save(
                getattr(settings.paths, kind)
            )
            with self.assertRaisesRegex(ValueError, "changed after validated"):
                assert_generation(self.spark, settings, replay)

    def test_keyed_reconciliation_removes_stale_current_rows_and_keeps_history(self):
        with tempfile.TemporaryDirectory(
            prefix="dsp-synthetic-reconcile-"
        ) as directory:
            path = Path(directory).as_uri() + "/derived"
            first = self.spark.createDataFrame(
                [("a", 1), ("b", 2)], "key string, value long"
            )
            reconcile_delta(first, path, keys=("key",))
            second = self.spark.createDataFrame([("a", 3), ("c", 4)], first.schema)
            reconcile_delta(second, path, keys=("key",))
            actual = {r.key: r.value for r in read_delta(self.spark, path).collect()}
            self.assertEqual(actual, {"a": 3, "c": 4})
            original = (
                self.spark.read.format("delta").option("versionAsOf", 0).load(path)
            )
            self.assertEqual(original.count(), 2)
            with self.assertRaisesRegex(ValueError, "unique"):
                reconcile_delta(second.unionByName(second), path, keys=("key",))

    def test_legacy_projection_propagates_verified_deletion_and_keeps_other_content(
        self,
    ):
        with tempfile.TemporaryDirectory(
            prefix="dsp-synthetic-legacy-lifecycle-"
        ) as directory:
            root = Path(directory)
            settings = SparkSettings(LakehousePaths((root / "lake").as_uri()))
            raw = youtube()
            raw["source_changes"] = [
                {
                    "source_record_id": "v1",
                    "record_type": "Video",
                    "change_type": "deleted",
                    "evidence": {
                        "source_url": "https://synthetic.invalid/v1",
                        "source_tombstone": True,
                    },
                }
            ]
            artifact = root / "synthetic-deletion.json"
            artifact.write_text(json.dumps(raw))
            ingest_bronze(self.spark, artifact, settings, source="youtube")
            frame = self.spark.createDataFrame(
                [
                    (
                        "youtube",
                        "v1",
                        "youtube:v1",
                        None,
                        "synthetic",
                        "2026-10-10T00:00:00Z",
                    ),
                    (
                        "youtube_comment",
                        "c1",
                        "youtube-comment:c1",
                        "youtube:v1",
                        "synthetic",
                        "2026-10-10T00:00:00Z",
                    ),
                    (
                        "youtube",
                        "other",
                        "youtube:other",
                        None,
                        "synthetic",
                        "2026-10-10T00:00:00Z",
                    ),
                ],
                "source string, source_record_id string, record_id string, parent_record_id string, run_id string, collected_at string",
            )
            result = permitted_legacy_frame(self.spark, settings, frame)
            self.assertEqual([r.record_id for r in result.collect()], ["youtube:other"])
            self.assertEqual(result.columns, frame.columns)


if __name__ == "__main__":
    unittest.main()
