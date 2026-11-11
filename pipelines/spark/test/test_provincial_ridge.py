"""Pure protocol tests: calendar, folds, strict snapshots and feature ordering."""

import unittest
from dataclasses import replace
from datetime import date

from dsp_spark.ml.provincial.config import (
    ForecastConfig,
    InsufficientHistory,
    fold_plan,
)
from dsp_spark.ml.provincial.data import normalized_inputs
from dsp_spark.ml.provincial.features import feature_metadata, numeric_names


class ProvincialProtocolTests(unittest.TestCase):
    def setUp(self):
        self.config = ForecastConfig("2026-11-11", "2026-11-10")

    def test_completed_week_asof_and_watermark(self):
        self.assertEqual(self.config.last_complete_week, date(2026, 11, 2))
        self.assertEqual(
            replace(self.config, as_of_date="2026-11-08").last_complete_week,
            date(2026, 10, 26),
        )
        self.assertEqual(
            replace(self.config, observed_through="2026-10-28").last_complete_week,
            date(2026, 10, 19),
        )

    def test_folds_match_array_split_and_expanding_train(self):
        folds = fold_plan(64, self.config)
        self.assertEqual(
            [(f["test_start_index"], f["test_end_exclusive"]) for f in folds],
            [(48, 54), (54, 59), (59, 64)],
        )
        self.assertEqual([f["train_end_exclusive"] for f in folds], [36, 42, 47])
        self.assertEqual(folds[0]["validation_start_index"], 36)
        for fold in folds:
            self.assertEqual(
                fold["validation_end_exclusive"] - fold["validation_start_index"], 12
            )
            self.assertEqual(
                fold["train_end_exclusive"], fold["validation_start_index"]
            )
        with self.assertRaises(InsufficientHistory):
            fold_plan(47, self.config)

    def test_strict_config_and_order(self):
        for kwargs in (
            {"lookback": 7},
            {"horizon": 2},
            {"min_provinces": 2},
            {"alpha": float("nan")},
            {"panel_start": "2025-01-07"},
        ):
            with self.assertRaises(ValueError):
                replace(self.config, **kwargs)
        names = numeric_names()
        self.assertEqual(len(names), 48)
        self.assertEqual(names[:2], ["target_lag8", "geo_linked_posts_lag8"])
        self.assertEqual(names[-1], "commercial_geo_posts_lag1")
        meta = feature_metadata([{"province_id": "p0"}, {"province_id": "p1"}])
        self.assertEqual(meta[-1]["feature_name"], "province_onehot::p1")

    def test_no_implicit_source_fallback(self):
        with self.assertRaises(ValueError):
            normalized_inputs(
                {"tables": {}, "dataset_id": "synthetic", "provenance": "synthetic"}
            )


if __name__ == "__main__":
    unittest.main()
