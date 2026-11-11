"""Local Spark/Delta checks. All generated histories are explicitly synthetic."""

import json
import os
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import numpy as np
from dsp_spark.jobs.batch.provincial_demand import main as ridge_main
from dsp_spark.ml.provincial.audit import compare_sources, readiness_audit
from dsp_spark.ml.provincial.config import CHANNELS, TABLES, ForecastConfig
from dsp_spark.ml.provincial.data import eligibility, read_sources, weekly_counts
from dsp_spark.ml.provincial.evaluation import compare_metrics, validate_predictions
from dsp_spark.ml.provincial.features import (
    assemble,
    fit_scaler,
    lag_features,
    numeric_names,
)
from dsp_spark.ml.provincial.pipeline import (
    forecast,
    load_manifest,
    prepare,
    publish_forecast,
    run_benchmark,
)
from dsp_spark.ml.provincial.ridge import fit_ridge
from dsp_spark.ml.registry import Registry
from dsp_spark.runtime.config import LakehousePaths, SparkSettings
from dsp_spark.runtime.spark import create_spark
from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import LinearRegression
from pyspark.sql import functions as F
from sklearn.linear_model import Ridge


def synthetic_silver(spark, root, weeks=64):
    start = date(2025, 1, 6)
    posts, geo, intents = [], [], []
    for p in range(4):
        for week in range(weeks):
            if p == 3 and week < 56:
                continue
            target = 1 + (week + p) % 4
            for j in range(6):
                cid = f"synthetic-{p}-{week}-{j}"
                published = (start + timedelta(weeks=week, days=j)).isoformat()
                labels = (
                    ["SEEK_COURT", "SEEK_PLAYERS"]
                    if j < target
                    else ["GENERAL_DISCUSSION"]
                )
                posts.append((cid, cid, published, None, labels, j == 5, "3"))
                geo.append((cid, cid, f"p{p}", "RESOLVED", "3"))
                if j == 0:
                    geo.append((cid + "-duplicate", cid, f"p{p}", "RESOLVED", "3"))
                if j < target:
                    intents.extend(
                        [
                            (cid + "-i1", cid, "SEEK_COURT", "3"),
                            (cid + "-i2", cid, "SEEK_PLAYERS", "3"),
                        ]
                    )
    frames = {
        TABLES[0]: spark.createDataFrame(
            posts,
            "post_id string, content_id string, published_date_vn string, published_at string, intent_labels array<string>, is_commercial boolean, schema_version string",
        ),
        TABLES[1]: spark.createDataFrame(
            geo,
            "mention_id string, content_id string, new_province_id string, resolution_status string, schema_version string",
        ),
        TABLES[2]: spark.createDataFrame(
            intents,
            "evidence_id string, content_id string, intent_type string, schema_version string",
        ),
        TABLES[3]: spark.createDataFrame(
            [
                (
                    f"p{p}",
                    f"Synthetic province {p}",
                    "PROVINCE",
                    "VN_ADMIN_POST_2025",
                    "3",
                )
                for p in range(4)
            ],
            "admin_unit_id string, admin_name string, admin_level string, admin_version string, schema_version string",
        ),
    }
    inputs = {
        "dataset_id": "synthetic-provincial-test",
        "provenance": "synthetic",
        "generation": "synthetic-fixed",
        "tables": {},
    }
    for name, frame in frames.items():
        path = (root / "silver" / name).as_uri()
        frame.write.format("delta").save(path)
        inputs["tables"][name] = {"path": path, "version": 0}
    config = ForecastConfig(
        as_of_date=(start + timedelta(weeks=weeks)).isoformat(),
        observed_through=(start + timedelta(weeks=weeks, days=-1)).isoformat(),
    )
    return inputs, config, frames


class ProvincialSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        base = "file:///tmp/dsp-provincial-session"
        if os.getenv("DSP_RIDGE_S3_INPUTS"):
            descriptor = json.loads(Path(os.environ["DSP_RIDGE_S3_INPUTS"]).read_text())
            base = descriptor["tables"][TABLES[0]]["path"]
        cls.spark = create_spark(
            SparkSettings(
                LakehousePaths(base),
                s3_profile=os.getenv("DSP_RIDGE_S3_PROFILE"),
                s3_region="ap-southeast-2",
            )
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_geo_dedup_multi_intent_union_timezone_and_incomplete_week(self):
        s = self.spark
        posts = s.createDataFrame(
            [
                (
                    "a",
                    None,
                    "2025-01-05T18:00:00Z",
                    ["SEEK_COURT", "SEEK_PLAYERS"],
                    True,
                ),
                ("b", "2025-01-12", None, [], False),
                ("c", None, "2025-01-12T18:00:00Z", ["SEEK_COURT"], False),
                ("d", "2025-01-06", None, ["SEEK_COURT"], False),
            ],
            "content_id string, published_date_vn string, published_at string, intent_labels array<string>, is_commercial boolean",
        )
        geo = s.createDataFrame(
            [
                ("a", "p0", "RESOLVED"),
                ("a", "p0", "RESOLVED"),
                ("a", "p1", "RESOLVED"),
                ("b", "p0", "RESOLVED"),
                ("c", "p0", "RESOLVED"),
                ("d", "p0", "AMBIGUOUS"),
                ("a", "p2", "NO_MATCH"),
            ],
            "content_id string,new_province_id string,resolution_status string",
        )
        intents = s.createDataFrame(
            [
                ("a", "SEEK_COURT"),
                ("a", "PRICE_INQUIRY"),
                ("a", "SEEK_COURT"),
                ("b", "PRICE_INQUIRY"),
            ],
            "content_id string,intent_type string",
        )
        admin = s.createDataFrame(
            [
                (f"p{i}", f"Synthetic{i}", "PROVINCE", "VN_ADMIN_POST_2025")
                for i in range(3)
            ],
            "admin_unit_id string,admin_name string,admin_level string,admin_version string",
        )
        counts, _, _, audit = weekly_counts(
            dict(zip(TABLES, [posts, geo, intents, admin])),
            ForecastConfig("2025-01-13", "2025-01-12"),
        )
        result = {r.province_id: r for r in counts.collect()}
        self.assertEqual(
            (
                result["p0"].target,
                result["p0"].geo_linked_posts,
                result["p0"].seek_court,
                result["p0"].seek_players,
                result["p0"].price_inquiry,
                result["p0"].commercial_geo_posts,
            ),
            (2, 2, 1, 1, 2, 1),
        )
        self.assertEqual(result["p1"].target, 1)
        self.assertEqual(result["p0"].week_start, date(2025, 1, 6))
        self.assertNotIn("p2", result)
        self.assertEqual(audit["posts_missing_publication_date"], 0)

    def panel(self, weeks=64):
        start = date(2025, 1, 6)
        rows = [
            (
                f"p{p}",
                f"Synthetic {p}",
                start + timedelta(weeks=w),
                w,
                *[float((w + 1) * (i + 1) + p) for i in range(6)],
            )
            for p in range(3)
            for w in range(weeks)
        ]
        return self.spark.createDataFrame(
            rows,
            "province_id string,province_name string,week_start date,week_index int,"
            + ",".join(f"{c} double" for c in CHANNELS),
        )

    def test_lag_order_no_target_leakage_scaler_train_only_and_stable_onehot(self):
        panel = self.panel()
        samples = lag_features(panel)
        train = samples.filter("week_index < 30")
        scaler = fit_scaler(train)
        matrix = np.array(
            [
                [r[n] for n in numeric_names()]
                for r in train.orderBy("province_id", "week_index").collect()
            ]
        ).reshape(-1, 6)
        for i, c in enumerate(CHANNELS):
            self.assertAlmostEqual(scaler[c]["mean"], matrix[:, i].mean(), places=10)
            self.assertAlmostEqual(scaler[c]["scale"], matrix[:, i].std(), places=10)
        row = samples.filter("province_id='p0' and week_index=8").first()
        self.assertEqual((row.target_lag8, row.target_lag1, row.label), (1.0, 8.0, 9.0))
        mapping = [
            {"province_id": f"p{p}", "province_name": f"Synthetic {p}"}
            for p in range(3)
        ]
        first = (
            assemble(train, scaler, mapping)
            .orderBy("province_id", "week_index")
            .first()
            .features.toArray()
        )
        self.assertEqual(len(first), 51)
        self.assertEqual(list(first[-3:]), [1.0, 0.0, 0.0])
        occluded = (
            assemble(train, scaler, mapping, occlude="target")
            .orderBy("province_id", "week_index")
            .first()
            .features.toArray()
        )
        np.testing.assert_array_equal(occluded[np.arange(0, 48, 6)], np.zeros(8))
        changed = train.withColumn("label", F.lit(1e12))
        other = (
            assemble(changed, scaler, mapping)
            .orderBy("province_id", "week_index")
            .first()
            .features.toArray()
        )
        np.testing.assert_array_equal(first, other)
        future_changed = panel.withColumn(
            "target", F.when(F.col("week_index") >= 30, 1e12).otherwise(F.col("target"))
        )
        self.assertEqual(
            scaler, fit_scaler(lag_features(future_changed).filter("week_index < 30"))
        )
        constant = train
        for n in numeric_names():
            if n.startswith("price_inquiry_"):
                constant = constant.withColumn(n, F.lit(0.0))
        self.assertEqual(fit_scaler(constant)["price_inquiry"]["scale"], 1.0)

    def test_missing_duplicate_weeks_and_unknown_province_fail(self):
        panel = self.panel(12)
        with self.assertRaisesRegex(ValueError, "missing week"):
            lag_features(panel.filter("week_index != 5"))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            lag_features(panel.unionByName(panel.limit(1)))
        samples = lag_features(panel)
        with self.assertRaisesRegex(ValueError, "unknown province"):
            assemble(samples, fit_scaler(samples), [{"province_id": "p0"}])

    def test_spark_regularization_equivalence_and_constant_label(self):
        rng = np.random.default_rng(42)
        x = np.c_[rng.normal(size=(80, 5)), np.eye(3)[np.arange(80) % 3], np.ones(80)]
        y = (
            20
            + x @ np.array([2.0, -3.0, 0.5, 1.0, -2.0, -1.0, 0.0, 1.0, 0.0])
            + rng.normal(size=80) * 0.1
        )
        frame = self.spark.createDataFrame(
            [(Vectors.dense(v), float(t)) for v, t in zip(x, y)], ["features", "label"]
        )
        model, meta = fit_ridge(frame)
        reference = Ridge(alpha=10.0).fit(x, y)
        coefficient_difference = float(
            np.max(np.abs(model.coefficients.toArray() - reference.coef_))
        )
        prediction_difference = float(
            np.max(
                np.abs(
                    np.array([model.predict(Vectors.dense(v)) for v in x])
                    - reference.predict(x)
                )
            )
        )
        self.assertLess(coefficient_difference, 1e-7)
        self.assertLess(prediction_difference, 1e-7)
        wrong = LinearRegression(
            featuresCol="features",
            labelCol="label",
            standardization=False,
            regParam=10 / 80,
            elasticNetParam=0.0,
            solver="auto",
        ).fit(frame)
        self.assertGreater(
            np.max(np.abs(wrong.coefficients.toArray() - reference.coef_)), 1e-3
        )
        constant, _ = fit_ridge(frame.withColumn("label", F.lit(7.0)))
        self.assertAlmostEqual(constant.intercept, 7.0)
        self.assertEqual(np.linalg.norm(constant.coefficients.toArray()), 0)
        Path(".local/ml/provincial-ridge").mkdir(parents=True, exist_ok=True)
        Path(".local/ml/provincial-ridge/test-parity.json").write_text(
            json.dumps(
                {
                    **meta,
                    "max_coefficient_difference": coefficient_difference,
                    "max_prediction_difference": prediction_difference,
                },
                indent=2,
            )
        )

    def test_metrics_bias_macro_and_zero_baseline(self):
        frame = self.spark.createDataFrame(
            [
                ("p0", date(2025, 1, 6), 1.0, -2.0, 0.0),
                ("p0", date(2025, 1, 13), 3.0, 4.0, 2.0),
                ("p1", date(2025, 1, 6), 2.0, 0.0, 0.0),
            ],
            "province_id string,forecast_week date,label double,raw_prediction double,moving_average double",
        ).withColumn("prediction", F.greatest(F.lit(0.0), F.col("raw_prediction")))
        validate_predictions(frame)
        score = compare_metrics(frame).first()
        self.assertAlmostEqual(score.rmse, np.sqrt(2.0))
        self.assertAlmostEqual(score.mae, 4 / 3)
        self.assertAlmostEqual(score.prediction_bias, -2 / 3)
        provinces = compare_metrics(frame, ("province_id",))
        self.assertAlmostEqual(provinces.agg(F.avg("mae")).first()[0], 1.5)
        zeros = frame.withColumn("label", F.lit(0.0)).withColumn(
            "moving_average", F.lit(0.0)
        )
        self.assertIsNone(compare_metrics(zeros).first().rmse_skill)
        with self.assertRaisesRegex(ValueError, "nonfinite"):
            validate_predictions(
                frame.withColumn("raw_prediction", F.lit(float("nan")))
            )
        for column in ("label", "prediction"):
            with self.assertRaisesRegex(ValueError, "nonfinite"):
                validate_predictions(frame.withColumn(column, F.lit(float("nan"))))
        with self.assertRaisesRegex(ValueError, "nonfinite"):
            validate_predictions(
                frame.withColumn("prediction", F.lit(None).cast("double"))
            )

    def test_full_silver_benchmark_refit_saved_forecast_and_idempotency(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            inputs, config, _ = synthetic_silver(self.spark, root)
            store = Registry(str(root / "staging/registry"))
            panel, audit, folds, mapping, _prepared = prepare(
                self.spark, inputs, config
            )
            self.assertEqual([p["province_id"] for p in mapping], ["p0", "p1", "p2"])
            self.assertEqual(
                audit.filter("province_id='p3'").first().historical_direct_posts, 0
            )
            mutated = panel.withColumn(
                "target",
                F.when(
                    F.col("week_index") >= folds[0]["validation_start_index"], 1e12
                ).otherwise(F.col("target")),
            )
            self.assertEqual(eligibility(mutated, folds, config)[1], mapping)
            input_file = root / "inputs.json"
            input_file.write_text(json.dumps(inputs))
            config_file = root / "config.json"
            config_file.write_text(json.dumps(config.to_dict()))
            training_report = root / "training.json"
            args = [
                "--inputs",
                str(input_file),
                "--config",
                str(config_file),
                "--registry",
                str(root / "staging/registry"),
                "--report",
                str(training_report),
            ]
            with (
                patch(
                    "dsp_spark.jobs.batch.provincial_demand.create_spark",
                    return_value=self.spark,
                ),
                patch.object(self.spark, "stop"),
            ):
                ridge_main(
                    ["train", *args, "--run-id", "synthetic-ridge-test", "--execute"]
                )
                files_before = {str(p) for p in (root / "staging").rglob("*")}
                ridge_main(["evaluate", *args])
                self.assertEqual(
                    files_before, {str(p) for p in (root / "staging").rglob("*")}
                )
            result = json.loads(
                (
                    root
                    / "staging/registry/provincial_ridge/runs/synthetic-ridge-test/manifest.json"
                ).read_text()
            )
            self.assertEqual(result["status"], "TRAINED")
            self.assertEqual(result["feature_dimension"], 51)
            self.assertEqual(result["summary"]["rows"], 48)
            self.assertEqual(result["final_model"]["training_samples"], 3 * (64 - 8))
            self.assertNotEqual(
                result["fold_fits"][-1]["regularization"]["training_samples"],
                result["final_model"]["training_samples"],
            )
            first, report = forecast(self.spark, inputs, store, "synthetic-ridge-test")
            second, _ = forecast(self.spark, inputs, store, "synthetic-ridge-test")
            cols = [
                "province_id",
                "forecast_week",
                "raw_prediction",
                "predicted_geo_linked_intent_posts",
            ]
            self.assertEqual(
                first.select(*cols).orderBy("province_id").collect(),
                second.select(*cols).orderBy("province_id").collect(),
            )
            self.assertEqual(first.count(), 3)
            self.assertEqual(report["forecast_week"], config.as_of_date)
            self.assertEqual(
                first.filter("predicted_geo_linked_intent_posts < 0").count(), 0
            )
            gold = publish_forecast(first, str(root / "staging/output"))
            publish_forecast(second, str(root / "staging/output"))
            self.assertEqual(self.spark.read.format("delta").load(gold).count(), 3)
            forecast_report = root / "forecast.json"
            with (
                patch(
                    "dsp_spark.jobs.batch.provincial_demand.create_spark",
                    return_value=self.spark,
                ),
                patch.object(self.spark, "stop"),
            ):
                ridge_main(
                    [
                        "predict",
                        "--inputs",
                        str(input_file),
                        "--registry",
                        str(root / "staging/registry"),
                        "--run-id",
                        "synthetic-ridge-test",
                        "--execute",
                        "--publish",
                        "--output-root",
                        str(root / "staging/output"),
                        "--report",
                        str(forecast_report),
                    ]
                )
            self.assertEqual(
                len(json.loads(forecast_report.read_text())["forecasts"]), 3
            )
            from datetime import UTC

            from jsonschema import Draft202012Validator, FormatChecker

            contract = json.loads(
                (
                    Path(__file__).resolve().parents[3]
                    / "contracts/data/gold/provincial_demand_forecast_v1.schema.json"
                ).read_text()
            )
            example = first.first().asDict()
            for name in ("forecast_week", "training_cutoff"):
                example[name] = example[name].isoformat()
            example["generated_at"] = (
                example["generated_at"].replace(tzinfo=UTC).isoformat()
            )
            Draft202012Validator(contract, format_checker=FormatChecker()).validate(
                example
            )
            table_root = store.uri("provincial_ridge/runs/synthetic-ridge-test/tables")
            self.assertEqual(
                self.spark.read.format("delta")
                .load(table_root + "/ridge_grouped_feature_occlusion")
                .count(),
                6,
            )
            self.assertEqual(
                self.spark.read.format("delta")
                .load(table_root + "/ridge_metrics_by_fold")
                .count(),
                3,
            )
            self.assertEqual(
                self.spark.read.format("delta")
                .load(table_root + "/ridge_province_effects")
                .count(),
                3,
            )
            for name in (
                "walk_forward_folds",
                "province_eligibility_audit",
                "ridge_predictions_by_province_week",
                "ridge_metrics_by_province",
                "ridge_benchmark_summary",
                "ridge_model_errors",
            ):
                self.spark.read.format("delta").load(table_root + "/" + name).count()
            with self.assertRaises(FileExistsError):
                run_benchmark(
                    self.spark,
                    inputs,
                    config,
                    store,
                    final_refit=True,
                    run_id="synthetic-ridge-test",
                    execute=True,
                )
            manifest = (
                root
                / "staging/registry/provincial_ridge/runs/synthetic-ridge-test/manifest.json"
            )
            modified = json.loads(manifest.read_text())
            modified["status"] = "EVALUATED"
            modified["final_model"] = None
            manifest.write_text(json.dumps(modified))
            with self.assertRaisesRegex(ValueError, "final refit"):
                load_manifest(store, "synthetic-ridge-test")
            result["test_provenance"] = "SYNTHETIC FIXTURE ONLY; not a market benchmark"
            Path(".local/ml/provincial-ridge/synthetic-benchmark.json").write_text(
                json.dumps(result, indent=2)
            )

    def test_readiness_audit_reconciles_history_deficits_and_snapshot_comparison(self):
        with tempfile.TemporaryDirectory() as temp:
            inputs, config, frames = synthetic_silver(self.spark, Path(temp))
            provinces, stages, report = readiness_audit(self.spark, inputs, config)
            self.assertEqual(report["status"], "READY")
            self.assertEqual(report["province_shortage"], 0)
            for integrity in report["snapshot_integrity"].values():
                self.assertGreater(integrity["active_data_files"], 0)
                self.assertEqual(
                    integrity["physical_file_inventory"]["status"], "COMPLETE"
                )
            future = provinces.filter("province_id='p3'").first()
            self.assertFalse(future.is_eligible)
            self.assertEqual(
                (future.geo_posts_history, future.direct_posts_history), (0.0, 0.0)
            )
            self.assertEqual(future.minimum_additional_qualifying_posts, 5.0)
            self.assertTrue(future.would_pass_using_future_incorrectly)
            self.assertIn("INSUFFICIENT_DIRECT_POSTS_HISTORY", future.exclusion_reason)
            self.assertEqual(
                stages.filter(
                    "pipeline='geo_pairs' and stage='before_first_validation'"
                )
                .first()
                .retained_rows,
                sum(r.geo_posts_history for r in provinces.collect()),
            )
            self.assertTrue(
                all(
                    v["exact_content_match"]
                    for v in compare_sources(frames, frames).values()
                )
            )
            altered = dict(frames)
            altered[TABLES[0]] = frames[TABLES[0]].limit(1)
            self.assertFalse(
                compare_sources(frames, altered)[TABLES[0]]["exact_content_match"]
            )

    @unittest.skipUnless(
        os.getenv("DSP_RIDGE_S3_INPUTS"),
        "SKIPPED: no explicitly configured S3 snapshot descriptor/credentials",
    )
    def test_s3_pinned_silver_reads(self):
        sources, _ = read_sources(
            self.spark, json.loads(Path(os.environ["DSP_RIDGE_S3_INPUTS"]).read_text())
        )
        self.assertEqual(set(sources), set(TABLES))


if __name__ == "__main__":
    unittest.main()
