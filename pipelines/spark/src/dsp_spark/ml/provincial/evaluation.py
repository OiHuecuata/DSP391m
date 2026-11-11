"""Distributed metrics, frozen walk-forward boundaries and coefficient analysis."""

from datetime import timedelta

from pyspark.sql import functions as F

from ..data import unique_keys
from .config import CHANNELS
from .features import feature_metadata
from .ridge import fit_prepared, predict


def validate_predictions(frame, expected=None):
    unique_keys(frame, ("province_id", "forecast_week"))
    if (
        frame.filter(
            F.col("label").isNull()
            | F.isnan("label")
            | (F.abs(F.col("label")) == float("inf"))
            | F.col("prediction").isNull()
            | F.isnan("prediction")
            | (F.abs(F.col("prediction")) == float("inf"))
            | F.col("raw_prediction").isNull()
            | F.isnan("raw_prediction")
            | (F.abs(F.col("raw_prediction")) == float("inf"))
            | (F.col("prediction") < 0)
        )
        .limit(1)
        .count()
    ):
        raise ValueError("missing/nonfinite/negative prediction")
    if expected is not None:
        keys = ["province_id", "forecast_week"]
        if (
            expected.select(*keys).exceptAll(frame.select(*keys)).limit(1).count()
            or frame.select(*keys).exceptAll(expected.select(*keys)).limit(1).count()
        ):
            raise ValueError("missing, extra or misaligned province-week prediction")


def metric_table(predictions, keys=(), prediction="prediction"):
    residual = F.col(prediction) - F.col("label")
    return predictions.groupBy(*keys).agg(
        F.count("*").alias("rows"),
        F.sqrt(F.avg(residual * residual)).alias("rmse"),
        F.avg(F.abs(residual)).alias("mae"),
        F.avg(residual).alias("prediction_bias"),
        F.avg((F.col("label") == 0).cast("double")).alias("target_zero_ratio"),
    )


def compare_metrics(predictions, keys=()):
    ridge = metric_table(predictions, keys)
    baseline = metric_table(predictions, keys, "moving_average").select(
        *keys,
        F.col("rmse").alias("baseline_rmse"),
        F.col("mae").alias("baseline_mae"),
        F.col("prediction_bias").alias("baseline_prediction_bias"),
    )
    paired = (
        ridge.join(baseline, list(keys), "inner") if keys else ridge.crossJoin(baseline)
    )
    return paired.withColumn(
        "rmse_skill",
        F.when(F.col("baseline_rmse") != 0, 1 - F.col("rmse") / F.col("baseline_rmse")),
    ).withColumn(
        "mae_skill",
        F.when(F.col("baseline_mae") != 0, 1 - F.col("mae") / F.col("baseline_mae")),
    )


def coefficients(model, mapping, spark):
    metadata = feature_metadata(mapping)
    if len(metadata) != len(model.coefficients):
        raise ValueError("coefficient/feature-order mismatch")
    rows = [
        (
            i,
            m["feature_name"],
            float(c),
            abs(float(c)),
            m["lag"],
            m["feature_group"],
            m["province_id"],
        )
        for i, (m, c) in enumerate(zip(metadata, model.coefficients))
    ]
    return spark.createDataFrame(
        rows,
        "feature_index int, feature_name string, coefficient double, absolute_coefficient double, lag int, feature_group string, province_id string",
    )


def occlusion(model, test, scaler, mapping):
    original = metric_table(predict(model, test, scaler, mapping)).first()
    rows = []
    for channel in CHANNELS:
        score = metric_table(
            predict(model, test, scaler, mapping, occlude=channel)
        ).first()
        rows.append(
            (
                channel,
                float(original.rmse),
                float(original.mae),
                float(score.rmse),
                float(score.mae),
                float(score.rmse - original.rmse),
                float(score.mae - original.mae),
            )
        )
    return test.sparkSession.createDataFrame(
        rows,
        "feature_group string, original_rmse double, original_mae double, occluded_rmse double, occluded_mae double, delta_rmse double, delta_mae double",
    )


def walk_forward(samples, folds, mapping, config, start_date, *, save_fold=None):
    spark = samples.sparkSession
    predictions, val_predictions, fold_rows, fitted = [], [], [], []
    for fold in folds:
        train = samples.filter(F.col("week_index") < fold["train_end_exclusive"])
        validation = samples.filter(
            (F.col("week_index") >= fold["validation_start_index"])
            & (F.col("week_index") < fold["validation_end_exclusive"])
        )
        test = samples.filter(
            (F.col("week_index") >= fold["test_start_index"])
            & (F.col("week_index") < fold["test_end_exclusive"])
        )
        model, scaler, regularization = fit_prepared(train, mapping, config)
        predicted = (
            predict(model, test, scaler, mapping)
            .select(
                "province_id",
                "province_name",
                "forecast_week",
                "label",
                "raw_prediction",
                "prediction",
                "moving_average",
            )
            .withColumn("fold", F.lit(fold["fold"]))
            .persist()
        )
        validate_predictions(predicted, test)
        expected_rows = len(mapping) * (
            fold["test_end_exclusive"] - fold["test_start_index"]
        )
        if predicted.count() != expected_rows:
            raise ValueError("incomplete rectangular province-test-week coverage")
        predictions.append(predicted)
        val_predictions.append(
            predict(model, validation, scaler, mapping)
            .select(
                "province_id",
                "forecast_week",
                "label",
                "raw_prediction",
                "prediction",
                "moving_average",
            )
            .withColumn("fold", F.lit(fold["fold"]))
        )
        if save_fold:
            save_fold(fold["fold"], model, scaler, regularization)
        fitted.append(
            {
                "model": model,
                "scaler": scaler,
                "regularization": regularization,
                "test": test,
            }
        )
        boundary = lambda index: (start_date + timedelta(weeks=index)).isoformat()
        fold_rows.append(
            (
                fold["fold"],
                boundary(fold["train_start_index"]),
                boundary(fold["train_end_exclusive"]),
                boundary(fold["validation_start_index"]),
                boundary(fold["validation_end_exclusive"]),
                boundary(fold["test_start_index"]),
                boundary(fold["test_end_exclusive"]),
                regularization["training_samples"],
                expected_rows,
                regularization["regParam"],
                regularization["label_population_std"],
            )
        )
    combined = predictions[0]
    for frame in predictions[1:]:
        combined = combined.unionByName(frame)
    validate_predictions(combined)
    combined = combined.localCheckpoint(eager=True)
    for frame in predictions:
        frame.unpersist()
    validation = val_predictions[0]
    for frame in val_predictions[1:]:
        validation = validation.unionByName(frame)
    by_province = compare_metrics(combined, ("province_id", "province_name"))
    summary = compare_metrics(combined).crossJoin(
        by_province.agg(
            F.avg("mae").alias("macro_mae"),
            F.avg("baseline_mae").alias("baseline_macro_mae"),
        )
    )
    last = fitted[-1]
    importance = coefficients(last["model"], mapping, spark)
    outputs = {
        "walk_forward_folds": spark.createDataFrame(
            fold_rows,
            "fold int, train_start string, train_end_exclusive string, validation_start string, validation_end_exclusive string, test_start string, test_end_exclusive string, train_rows long, test_rows long, regParam double, label_population_std double",
        ),
        "ridge_predictions_by_province_week": combined.withColumn(
            "residual", F.col("prediction") - F.col("label")
        ),
        "ridge_metrics_by_fold": compare_metrics(combined, ("fold",)),
        "ridge_metrics_by_province": by_province,
        "ridge_benchmark_summary": summary,
        "ridge_validation_metrics": compare_metrics(validation, ("fold",)),
        "ridge_coefficients": importance,
        "ridge_top_positive_coefficients": importance.filter("coefficient > 0")
        .orderBy(F.desc("coefficient"), "feature_index")
        .limit(10),
        "ridge_top_negative_coefficients": importance.filter("coefficient < 0")
        .orderBy("coefficient", "feature_index")
        .limit(10),
        "ridge_province_effects": importance.filter(F.col("province_id").isNotNull())
        .withColumn("intercept", F.lit(float(last["model"].intercept)))
        .withColumn(
            "interpretation",
            F.lit(
                "penalized deviation from shared intercept under full one-hot encoding; association, not causality"
            ),
        ),
        "ridge_coefficients_by_group": importance.groupBy("feature_group").agg(
            F.sum("absolute_coefficient").alias("absolute_coefficient_sum"),
            F.sum("coefficient").alias("signed_coefficient_sum"),
        ),
        "ridge_coefficients_by_lag": importance.filter(F.col("lag").isNotNull())
        .groupBy("lag")
        .agg(
            F.sum("absolute_coefficient").alias("absolute_coefficient_sum"),
            F.sum("coefficient").alias("signed_coefficient_sum"),
        ),
        "ridge_grouped_feature_occlusion": occlusion(
            last["model"], last["test"], last["scaler"], mapping
        ),
    }
    return outputs, [
        {k: v for k, v in fit.items() if k in ("scaler", "regularization")}
        for fit in fitted
    ]
