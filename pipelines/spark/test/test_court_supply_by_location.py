"""Latest venue supply, unknown-count coverage, and real Delta rebuild tests."""

import sys
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "pipelines/spark/src"), str(ROOT / "apps/collector/src")]

from dsp_spark.io.delta import read_delta, replace_delta
from dsp_spark.jobs.batch.build_court_supply_by_location import (
    build_court_supply_by_location,
)
from dsp_spark.processing.aggregate.court_supply_by_location import (
    court_supply_by_location,
    latest_courts,
    resolved_courts,
)
from dsp_spark.processing.clean.contracts import spark_schema, validate
from dsp_spark.runtime.config import LakehousePaths, SparkSettings
from dsp_spark.runtime.spark import create_spark
from pyspark.sql import types as T
from test_contracts import court

GENERATED = "2026-10-05T12:00:00.000000Z"


class CourtSupplyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(
            SparkSettings(LakehousePaths("file:///tmp/dsp-court-supply-runtime-test"))
        )
        cls.schema = T.StructType(
            spark_schema("court").fields
            + [
                T.StructField("lakehouse_ingested_at", T.TimestampType()),
                T.StructField("source_artifact_uri", T.StringType()),
            ]
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="dsp-court-supply-test-")
        self.addCleanup(temporary.cleanup)
        self.settings = SparkSettings(LakehousePaths(Path(temporary.name).as_uri()))

    def record(self, record_id="pbplus:one", **changes):
        row = {
            **court(),
            "record_id": record_id,
            "address": "12 Road, Thành phố Hà Nội",
            "address_source": None,
            "number_of_courts": 4,
            "collected_at": "2026-10-05T00:00:00.000000Z",
            "normalized_at": "2026-10-05T01:00:00.000000Z",
            "geocoded_at": None,
            "run_id": "a",
            "lakehouse_ingested_at": datetime(2026, 10, 5, tzinfo=UTC),
            "source_artifact_uri": "file:///fixture.json",
        }
        row.update(changes)
        return row

    def frame(self, rows):
        return self.spark.createDataFrame(rows, self.schema)

    def aggregate(self, rows):
        return court_supply_by_location(
            resolved_courts(self.frame(rows)), generated_at=GENERATED
        )

    def test_newer_run_one_venue_and_older_replay_cannot_override(self):
        old = self.record(
            run_id="old",
            collected_at="2026-10-03T00:00:00Z",
            number_of_courts=99,
            geocoded_at="2026-10-09T00:00:00Z",
            normalized_at="2026-10-10T00:00:00Z",
            lakehouse_ingested_at=datetime(2026, 10, 11, tzinfo=UTC),
        )
        new = self.record(run_id="new", number_of_courts=7)
        row = self.aggregate([old, new, old]).collect()[0]
        self.assertEqual((row.venue_count, row.known_court_count), (1, 7))

    def test_ties_use_geocoding_normalization_ingestion_and_run(self):
        pairs = [
            (
                self.record(run_id="z"),
                self.record(run_id="a", geocoded_at="2026-10-05T00:30:00Z"),
            ),
            (
                self.record(run_id="z"),
                self.record(run_id="a", normalized_at="2026-10-05T02:00:00Z"),
            ),
            (
                self.record(run_id="z"),
                self.record(
                    run_id="a", lakehouse_ingested_at=datetime(2026, 10, 6, tzinfo=UTC)
                ),
            ),
            (self.record(run_id="a"), self.record(run_id="z")),
        ]
        for losing, winning in pairs:
            with self.subTest(winner=winning["run_id"]):
                rows = latest_courts(self.frame([losing, winning])).collect()
                self.assertEqual(rows[0].run_id, winning["run_id"])

    def test_distinct_venues_aliases_and_known_unknown_coverage(self):
        row = self.aggregate(
            [
                self.record(address="12 Road, Hanoi", number_of_courts=3),
                self.record(
                    "pbplus:two", address="13 Road, Ha Noi", number_of_courts=None
                ),
                self.record("pbplus:three", number_of_courts=0),
            ]
        ).collect()[0]
        self.assertEqual((row.location_id, row.location_name), ("vn-hanoi", "Hà Nội"))
        self.assertEqual(
            (
                row.venue_count,
                row.known_court_count,
                row.venues_with_known_court_count,
                row.venues_without_known_court_count,
            ),
            (3, 3, 2, 1),
        )

    def test_all_unknown_total_is_null_not_zero_or_one(self):
        row = self.aggregate(
            [
                self.record(number_of_courts=None),
                self.record("pbplus:two", number_of_courts=None),
            ]
        ).collect()[0]
        self.assertIsNone(row.known_court_count)
        self.assertEqual(
            (
                row.venue_count,
                row.venues_with_known_court_count,
                row.venues_without_known_court_count,
            ),
            (2, 0, 2),
        )
        validate(row.asDict(), "court_supply_by_location")

    def test_different_places_stay_separate_and_preserve_vietnamese(self):
        rows = self.aggregate(
            [self.record(), self.record("pbplus:two", address="12 Road, TP.HCM.")]
        ).collect()
        self.assertEqual(
            {(r.location_id, r.location_name) for r in rows},
            {("vn-hanoi", "Hà Nội"), ("vn-ho-chi-minh-city", "TP. Hồ Chí Minh")},
        )
        self.assertTrue(all(r.venue_count == 1 for r in rows))

    def test_unknown_coordinates_and_fallback_do_not_invent_place(self):
        rows = [
            self.record(address=None, latitude=21.0, longitude=105.8),
            self.record("pbplus:two", address="12 Road, Atlantis"),
            self.record(
                "pbplus:three", address="Hà Nội", address_source="court_name_fallback"
            ),
        ]
        resolved = resolved_courts(self.frame(rows)).collect()
        self.assertTrue(all(r.location_id is None for r in resolved))
        self.assertEqual(self.aggregate(rows).count(), 0)

    def test_empty_input_and_latest_missing_address_remove_previous_place(self):
        self.assertEqual(self.aggregate([]).count(), 0)
        rows = [
            self.record(),
            self.record(run_id="b", collected_at="2026-10-06T00:00:00Z", address=None),
        ]
        self.assertEqual(self.aggregate(rows).count(), 0)
        self.assertEqual(resolved_courts(self.frame(rows)).count(), 1)

    def test_source_timestamp_uses_time_order_not_string_order(self):
        row = self.aggregate(
            [
                self.record(collected_at="2026-10-05T00:00:00Z"),
                self.record("pbplus:two", collected_at="2026-10-05T00:00:00.123456Z"),
            ]
        ).collect()[0]
        self.assertEqual(row.latest_source_observed_at, "2026-10-05T00:00:00.123456Z")

    def test_contract_fields_types_and_no_analytical_inference(self):
        frame = self.aggregate([self.record()])
        schema = spark_schema("court_supply_by_location")
        self.assertEqual(set(frame.columns), set(schema.fieldNames()))
        for field in schema:
            self.assertEqual(frame.schema[field.name].dataType, field.dataType)
        for row in frame.collect():
            validate(row.asDict(), "court_supply_by_location")
        self.assertNotIn("geolocated_venue_count", frame.columns)
        self.assertNotIn("demand_score", frame.columns)

    def test_actual_delta_rerun_update_quality_and_reopen(self):
        old = self.record()
        missing = self.record("pbplus:missing", address=None, number_of_courts=None)
        replace_delta(self.frame([old, missing]), self.settings.paths.courts)
        previous = None
        for _ in range(2):
            result = build_court_supply_by_location(self.spark, self.settings)
            self.assertEqual(
                (
                    result["logical_venues"],
                    result["resolved_venues"],
                    result["unresolved_venues"],
                    result["resolution_percentage"],
                ),
                (2, 1, 1, 50),
            )
            reopened = read_delta(
                self.spark.newSession(), self.settings.paths.court_supply_by_location
            )
            metrics = [
                {k: v for k, v in r.asDict().items() if k != "gold_generated_at"}
                for r in reopened.collect()
            ]
            if previous is not None:
                self.assertEqual(metrics, previous)
            previous = metrics
            quality = read_delta(
                self.spark, self.settings.paths.unresolved_court_locations
            ).collect()
            self.assertEqual(quality[0].record_id, "pbplus:missing")
            self.assertIsNone(quality[0].location_id)
            self.assertEqual(quality[0].location_resolution, "missing_address")
        newest = self.record(
            run_id="b",
            collected_at="2026-10-06T00:00:00Z",
            number_of_courts=8,
            address="14 Road, Sài Gòn",
        )
        fixed = self.record(
            "pbplus:missing", run_id="b", collected_at="2026-10-06T00:00:00Z"
        )
        replace_delta(
            self.frame([old, missing, newest, fixed]), self.settings.paths.courts
        )
        result = build_court_supply_by_location(self.spark, self.settings)
        self.assertEqual(
            (
                result["logical_venues"],
                result["unresolved_venues"],
                result["gold_rows"],
            ),
            (2, 0, 2),
        )
        self.assertEqual(
            read_delta(
                self.spark, self.settings.paths.unresolved_court_locations
            ).count(),
            0,
        )
        rows = read_delta(
            self.spark, self.settings.paths.court_supply_by_location
        ).collect()
        self.assertEqual(
            {r.location_id: r.known_court_count for r in rows},
            {"vn-hanoi": 4, "vn-ho-chi-minh-city": 8},
        )
        self.assertEqual(read_delta(self.spark, self.settings.paths.courts).count(), 4)


if __name__ == "__main__":
    unittest.main()
