"""Window, evidence joins and actual Delta replacement, without crawling/cloud."""

import math
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "pipelines/spark/src"), str(ROOT / "apps/collector/src")]

from dsp_spark.io.delta import read_delta, replace_delta
from dsp_spark.jobs.batch.build_location_market_signal_snapshot import (
    build_location_market_signal_snapshot,
)
from dsp_spark.processing.aggregate.location_market_signal_snapshot import (
    SOCIAL_METRICS,
    location_market_signal_snapshot,
    select_analysis_window,
)
from dsp_spark.processing.clean.contracts import spark_schema, validate
from dsp_spark.processing.clean.gold import validated_gold
from dsp_spark.runtime.config import LakehousePaths, MarketSignalSettings, SparkSettings
from dsp_spark.runtime.spark import create_spark
from jsonschema import Draft202012Validator, ValidationError

AT = "2026-10-05T12:00:00.000000Z"


def social(day="2026-10-05", location_id="vn-hanoi", name="Hà Nội", **changes):
    result = {
        "schema_version": "1",
        "activity_date": day,
        "location_id": location_id,
        "location_name": name,
        "post_count": 2,
        "like_count": 3,
        "reply_count": 4,
        "repost_count": 5,
        "quote_count": 6,
        "engagement_total": 18,
        "first_published_at": day + "T00:00:00.000000Z",
        "last_published_at": day + "T00:00:00.000000Z",
        "gold_generated_at": AT,
    }
    result.update(changes)
    return result


def supply(location_id="vn-hanoi", name="Hà Nội", **changes):
    result = {
        "schema_version": "1",
        "location_id": location_id,
        "location_name": name,
        "venue_count": 4,
        "known_court_count": 7,
        "venues_with_known_court_count": 3,
        "venues_without_known_court_count": 1,
        "latest_source_observed_at": AT,
        "gold_generated_at": AT,
    }
    result.update(changes)
    return result


class MarketSignalConfigTests(unittest.TestCase):
    def test_environment_defaults_overrides_and_invalid_configuration(self):
        with patch.dict("os.environ", {}, clear=True):
            self.assertEqual(MarketSignalSettings.from_env(), MarketSignalSettings(30))
        with patch.dict(
            "os.environ",
            {
                "DSP_MARKET_SIGNAL_WINDOW_DAYS": "7",
                "DSP_MARKET_SIGNAL_END_DATE": "2026-09-30",
            },
            clear=True,
        ):
            self.assertEqual(
                MarketSignalSettings.from_env(), MarketSignalSettings(7, "2026-09-30")
            )
        for raw in ("", "null", "0", "-1", "1.5", " 30", "true"):
            with (
                self.subTest(raw=raw),
                patch.dict(
                    "os.environ", {"DSP_MARKET_SIGNAL_WINDOW_DAYS": raw}, clear=True
                ),
                self.assertRaisesRegex(ValueError, "WINDOW_DAYS"),
            ):
                MarketSignalSettings.from_env()
        for days in (None, False, 0, -1, 1.5, "30"):
            with (
                self.subTest(days=days),
                self.assertRaisesRegex(ValueError, "WINDOW_DAYS"),
            ):
                MarketSignalSettings(days)
        for end in ("", "null", "2026-02-30", "20261005", "2026-1-01", 42):
            with self.subTest(end=end), self.assertRaisesRegex(ValueError, "END_DATE"):
                MarketSignalSettings(end_date=end)

    def test_central_paths_and_collisions(self):
        paths = LakehousePaths("s3a://fixture/lake")
        self.assertEqual(
            paths.location_market_signal_snapshot,
            "s3a://fixture/lake/gold/location_market_signal_snapshot",
        )
        with self.assertRaisesRegex(ValueError, "distinct"):
            LakehousePaths(
                paths.base, location_market_signal_snapshot=paths.social_activity_daily
            )


class MarketSignalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(
            SparkSettings(LakehousePaths("file:///tmp/dsp-market-runtime-test"))
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="dsp-market-test-")
        self.addCleanup(tmp.cleanup)
        self.settings = SparkSettings(LakehousePaths(Path(tmp.name).as_uri()))

    def frames(self, activity, venues):
        return (
            self.spark.createDataFrame(activity, spark_schema("social_activity_daily")),
            self.spark.createDataFrame(
                venues, spark_schema("court_supply_by_location")
            ),
        )

    def aggregate(self, activity, venues, config=None):
        a, s = self.frames(activity, venues)
        window = select_analysis_window(a, config or MarketSignalSettings())
        return location_market_signal_snapshot(a, s, window=window, generated_at=AT)

    def test_default_max_date_and_inclusive_thirty_day_window(self):
        rows = self.aggregate(
            [
                social("2026-09-05", post_count=100),
                social("2026-09-06"),
                social("2026-10-05"),
            ],
            [supply()],
        ).collect()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(
            (
                row.window_start_date,
                row.window_end_date,
                row.window_days,
                row.snapshot_date,
            ),
            ("2026-09-06", "2026-10-05", 30, "2026-10-05"),
        )
        for name, value in zip(SOCIAL_METRICS, (4, 6, 8, 10, 12, 36), strict=True):
            self.assertEqual(row["social_" + name], value)
            self.assertIs(type(row["social_" + name]), int)

    def test_explicit_days_and_end_exclude_both_outside_boundaries(self):
        row = self.aggregate(
            [
                social("2026-10-01", post_count=100),
                social("2026-10-02"),
                social("2026-10-03"),
                social("2026-10-04", post_count=100),
            ],
            [supply()],
            MarketSignalSettings(2, "2026-10-03"),
        ).collect()[0]
        self.assertEqual(
            (row.window_start_date, row.window_end_date, row.social_post_count),
            ("2026-10-02", "2026-10-03", 4),
        )
        row = self.aggregate([social()], [supply()], MarketSignalSettings(1)).collect()[
            0
        ]
        self.assertEqual(row.window_start_date, row.window_end_date)

    def test_full_outer_join_canonical_names_and_all_metrics(self):
        rows = self.aggregate(
            [
                social(),
                social(location_id="vn-da-nang", name="Đà Nẵng"),
            ],
            [
                supply(),
                supply(
                    "vn-hue",
                    "Huế",
                    known_court_count=None,
                    venues_with_known_court_count=0,
                    venues_without_known_court_count=4,
                ),
            ],
        ).collect()
        by_id = {r.location_id: r for r in rows}
        self.assertEqual(len(rows), 3)
        both = by_id["vn-hanoi"]
        self.assertTrue(both.has_social_signal and both.has_supply_signal)
        self.assertEqual(both.location_name, "Hà Nội")
        self.assertEqual(
            (
                both.venue_count,
                both.known_court_count,
                both.venues_with_known_court_count,
                both.venues_without_known_court_count,
            ),
            (4, 7, 3, 1),
        )
        self.assertEqual(
            (
                both.court_count_coverage_ratio,
                both.social_activity_per_venue,
                both.social_engagement_per_venue,
            ),
            (0.75, 0.5, 4.5),
        )
        only_social = by_id["vn-da-nang"]
        self.assertTrue(only_social.has_social_signal)
        self.assertFalse(only_social.has_supply_signal)
        for key in (
            "venue_count",
            "known_court_count",
            "venues_with_known_court_count",
            "venues_without_known_court_count",
            "court_count_coverage_ratio",
            "social_activity_per_venue",
            "social_engagement_per_venue",
        ):
            self.assertIsNone(only_social[key])
        only_supply = by_id["vn-hue"]
        self.assertFalse(only_supply.has_social_signal)
        self.assertTrue(only_supply.has_supply_signal)
        self.assertEqual(only_supply.location_name, "Huế")
        self.assertIsNone(only_supply.known_court_count)
        self.assertEqual(only_supply.venues_without_known_court_count, 4)
        self.assertEqual(only_supply.court_count_coverage_ratio, 0)
        self.assertEqual(only_supply.social_activity_per_venue, 0)
        self.assertEqual(only_supply.social_engagement_per_venue, 0)
        for name in SOCIAL_METRICS:
            self.assertEqual(only_supply["social_" + name], 0)
        for row in rows:
            validate(row.asDict(), "location_market_signal_snapshot")
            self.assertTrue(
                all(not isinstance(v, float) or math.isfinite(v) for v in row)
            )

    def test_catalog_mismatch_unknown_and_null_ids_rejected_in_either_source(self):
        for a, s in [
            ([social(name="Hanoi")], [supply()]),
            ([social()], [supply(name="Hanoi")]),
            ([social(location_id="vn-unknown")], []),
            ([social(location_id=None)], []),
            ([], [supply(location_id="vn-unknown")]),
        ]:
            with (
                self.subTest(activity=a, venues=s),
                self.assertRaisesRegex(ValueError, "catalog mismatch"),
            ):
                self.aggregate(a, s, MarketSignalSettings(end_date="2026-10-05"))

    def test_duplicate_source_keys_rejected(self):
        for a, s in [([social(), social()], []), ([social()], [supply(), supply()])]:
            with self.assertRaisesRegex(ValueError, "Duplicate Gold key"):
                self.aggregate(a, s)

    def test_empty_social_requires_end_and_then_preserves_supply(self):
        with self.assertRaisesRegex(ValueError, "Empty social Gold requires"):
            self.aggregate([], [supply()])
        row = self.aggregate(
            [], [supply()], MarketSignalSettings(end_date="2026-10-05")
        ).collect()[0]
        self.assertFalse(row.has_social_signal)
        self.assertEqual(row.social_post_count, 0)
        self.assertEqual(row.snapshot_date, "2026-10-05")
        self.assertEqual(
            self.aggregate([], [], MarketSignalSettings(end_date="2026-10-05")).count(),
            0,
        )

    def test_outside_only_social_location_is_not_retained(self):
        rows = self.aggregate(
            [social("2026-08-01"), social("2026-10-05", "vn-hue", "Huế")], [supply()]
        ).collect()
        hanoi = next(r for r in rows if r.location_id == "vn-hanoi")
        self.assertFalse(hanoi.has_social_signal)
        self.assertEqual(hanoi.social_post_count, 0)

    def test_safe_zero_denominator_and_known_zero_courts(self):
        row = self.aggregate(
            [social()],
            [
                supply(
                    venue_count=0,
                    known_court_count=None,
                    venues_with_known_court_count=0,
                    venues_without_known_court_count=0,
                )
            ],
        ).collect()[0]
        for key in (
            "court_count_coverage_ratio",
            "social_activity_per_venue",
            "social_engagement_per_venue",
        ):
            self.assertIsNone(row[key])
        row = self.aggregate([social()], [supply(known_court_count=0)]).collect()[0]
        self.assertEqual(row.known_court_count, 0)
        self.assertEqual(row.social_activity_per_venue, 0.5)

    def test_window_underflow_fails_clearly(self):
        a, _ = self.frames([], [])
        with self.assertRaisesRegex(ValueError, "Invalid social analysis window"):
            select_analysis_window(a, MarketSignalSettings(30, "0001-01-01"))

    def test_contract_alignment_unicode_and_nonfinite_rejection(self):
        frame = self.aggregate([social()], [supply()])
        schema = spark_schema("location_market_signal_snapshot")
        self.assertEqual(set(frame.columns), set(schema.fieldNames()))
        for field in schema:
            self.assertEqual(frame.schema[field.name].dataType, field.dataType)
        validated = validated_gold(frame, "location_market_signal_snapshot")
        row = validated.collect()[0].asDict()
        self.assertEqual(row["location_name"], "Hà Nội")
        for forbidden in (
            "demand_score",
            "gap_score",
            "priority_score",
            "market_rank",
            "recommendation",
            "known_courts_per_venue",
        ):
            self.assertNotIn(forbidden, row)
        for value in (float("inf"), float("nan"), -1):
            with self.assertRaises(ValidationError):
                validate(
                    {**row, "social_activity_per_venue": value},
                    "location_market_signal_snapshot",
                )
        import json

        doc = json.loads(
            (
                ROOT
                / "contracts/data/gold/location-market-signal-snapshot.v1.schema.json"
            ).read_text()
        )
        Draft202012Validator.check_schema(doc)

    def seed(self, activity, venues, quality=True, quality_time=AT):
        a, s = self.frames(activity, venues)
        replace_delta(a, self.settings.paths.social_activity_daily)
        replace_delta(s, self.settings.paths.court_supply_by_location)
        if quality:
            unresolved = self.spark.createDataFrame(
                [("pbplus:unknown", None, quality_time)],
                "record_id string, location_id string, quality_generated_at string",
            )
            replace_delta(unresolved, self.settings.paths.unresolved_court_locations)

    def test_delta_rebuild_reopens_idempotently_and_removes_stale_rows(self):
        self.seed(
            [social(), social(location_id="vn-da-nang", name="Đà Nẵng")],
            [supply(), supply("vn-hue", "Huế")],
        )
        previous = None
        for _ in range(2):
            result = build_location_market_signal_snapshot(
                self.spark, self.settings, market_settings=MarketSignalSettings()
            )
            self.assertEqual(
                (
                    result["output_locations"],
                    result["locations_with_both_signals"],
                    result["social_only_locations"],
                    result["supply_only_locations"],
                ),
                (3, 1, 1, 1),
            )
            self.assertEqual(
                (
                    result["supply_total_logical_venues"],
                    result["supply_resolved_venues"],
                    result["supply_unresolved_venues"],
                    result["supply_location_resolution_ratio"],
                ),
                (9, 8, 1, 8 / 9),
            )
            reopened = read_delta(
                self.spark.newSession(),
                self.settings.paths.location_market_signal_snapshot,
            )
            rows = [
                {k: v for k, v in r.asDict().items() if k != "gold_generated_at"}
                for r in reopened.orderBy("location_id").collect()
            ]
            if previous is not None:
                self.assertEqual(rows, previous)
            previous = rows
        self.seed([], [supply()])
        result = build_location_market_signal_snapshot(
            self.spark,
            self.settings,
            market_settings=MarketSignalSettings(end_date="2026-10-05"),
        )
        self.assertEqual(result["output_locations"], 1)
        self.assertEqual(
            read_delta(self.spark, self.settings.paths.location_market_signal_snapshot)
            .collect()[0]
            .social_post_count,
            0,
        )
        self.seed([], [])
        build_location_market_signal_snapshot(
            self.spark,
            self.settings,
            market_settings=MarketSignalSettings(end_date="2026-10-05"),
        )
        self.assertEqual(
            read_delta(
                self.spark, self.settings.paths.location_market_signal_snapshot
            ).count(),
            0,
        )

    def test_missing_quality_does_not_claim_zero_unresolved(self):
        self.seed([social()], [supply()], quality=False)
        result = build_location_market_signal_snapshot(self.spark, self.settings)
        self.assertFalse(result["supply_quality_available"])
        self.assertEqual(result["supply_resolved_venues"], 4)
        for key in (
            "supply_total_logical_venues",
            "supply_unresolved_venues",
            "supply_location_resolution_ratio",
        ):
            self.assertIsNone(result[key])

    def test_stale_quality_and_invalid_input_fail_before_output(self):
        self.seed([social()], [supply()], quality_time="2026-10-04T12:00:00.000000Z")
        with self.assertRaisesRegex(ValueError, "generations disagree"):
            build_location_market_signal_snapshot(self.spark, self.settings)
        self.assertFalse(
            Path(
                self.settings.paths.location_market_signal_snapshot.removeprefix(
                    "file://"
                )
            ).exists()
        )
        self.seed([social()], [supply(venue_count=0)])
        with self.assertRaisesRegex(ValueError, "Gold contract violation"):
            build_location_market_signal_snapshot(self.spark, self.settings)


if __name__ == "__main__":
    unittest.main()
