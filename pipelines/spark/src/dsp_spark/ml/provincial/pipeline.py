"""Repository-integrated walk-forward evaluation, final refit and saved inference."""

import json
import math
import re
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from importlib.metadata import version
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import Window
from pyspark.sql import functions as F

from ...io.delta import merge_delta
from ..config import staging_uri
from ..data import unique_keys
from .config import (
    ARTIFACT_VERSION,
    CHANNELS,
    ForecastConfig,
    fold_plan,
)
from .data import eligibility, make_panel, read_sources, weekly_counts
from .evaluation import coefficients, walk_forward
from .features import feature_metadata, lag_features, validate_panel
from .ridge import fit_prepared, predict


def runtime():
    return {name: version(name) for name in ("pyspark", "delta-spark", "numpy")}


def prefix(run_id):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", run_id or ""):
        raise ValueError("invalid Ridge run ID")
    return f"provincial_ridge/runs/{run_id}"


def write_json(store, key, data):
    store.put(key, json.dumps(data, indent=2, sort_keys=True, allow_nan=False).encode())


def prepare(spark, inputs, config):
    sources, lineage = read_sources(spark, inputs)
    counts, provinces, first, data_audit = weekly_counts(sources, config)
    panel, start, weeks = make_panel(counts, provinces, first, config)
    panel = panel.persist()
    try:
        folds = fold_plan(weeks, config)
        audit, mapping = eligibility(panel, folds, config)
        audit = audit.localCheckpoint(eager=True)
        selected = panel.join(
            audit.filter("eligible").select("province_id"), "province_id", "left_semi"
        )
        report = {
            "lineage": lineage,
            "data_audit": data_audit,
            "config": config.to_dict(),
            "panel_start": start.isoformat(),
            "panel_weeks": weeks,
            "training_cutoff": (
                config.last_complete_week + timedelta(days=6)
            ).isoformat(),
            "province_mapping": mapping,
            "eligibility": [
                r.asDict()
                for r in audit.orderBy("province_id")
                .limit(config.max_provinces + 1)
                .collect()
            ],
            "fold_indices": folds,
            "status": "READY" if len(mapping) >= config.min_provinces else "BLOCKED",
            "reason": (
                None
                if len(mapping) >= config.min_provinces
                else f"only {len(mapping)} eligible provinces; requires {config.min_provinces}"
            ),
        }
        return selected.localCheckpoint(eager=True), audit, folds, mapping, report
    finally:
        panel.unpersist()


def run_benchmark(
    spark, inputs, config, store, *, final_refit=False, run_id=None, execute=False
):
    panel, audit, folds, mapping, report = prepare(spark, inputs, config)
    if not execute:
        report["status"] = "DRY_RUN" if report["status"] == "READY" else "BLOCKED"
        return report
    run_id = (
        run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:12]
    )
    base = prefix(run_id)
    write_json(
        store,
        base + "/reservation.json",
        {"status": "RUNNING", "data_version": report["lineage"]["data_version"]},
    )
    report.update(
        run_id=run_id,
        artifact_version=ARTIFACT_VERSION,
        runtime=runtime(),
        state="candidate",
    )

    def save_table(name, frame):
        frame.write.format("delta").mode("errorifexists").save(
            store.uri(f"{base}/tables/{name}")
        )

    save_table("province_eligibility_audit", audit)
    errors_schema = "stage string, error_type string, message string"
    if report["status"] == "BLOCKED":
        save_table(
            "ridge_model_errors",
            spark.createDataFrame(
                [("eligibility", "InsufficientHistory", report["reason"])],
                errors_schema,
            ),
        )
        write_json(store, base + "/manifest.json", report)
        return report
    samples = lag_features(panel).persist()
    try:

        def save_fold(number, model, scaler, regularization):
            model.write().save(store.uri(f"{base}/folds/{number}/spark_model"))
            write_json(
                store,
                f"{base}/folds/{number}/metadata.json",
                {
                    "scaler": scaler,
                    "regularization": regularization,
                    "province_mapping": mapping,
                    "feature_order": feature_metadata(mapping),
                },
            )

        outputs, fitted = walk_forward(
            samples,
            folds,
            mapping,
            config,
            date.fromisoformat(report["panel_start"]),
            save_fold=save_fold,
        )
        for name, frame in outputs.items():
            save_table(name, frame)
        summary = outputs["ridge_benchmark_summary"].first().asDict()
        report.update(
            status="EVALUATED",
            summary=summary,
            folds=[
                r.asDict()
                for r in outputs["walk_forward_folds"].orderBy("fold").collect()
            ],
            fold_fits=fitted,
            feature_order=feature_metadata(mapping),
            feature_dimension=48 + len(mapping),
            final_model=None,
            evaluation_protocol="rolling one-step-ahead with observed history; fixed hyperparameters; no test refit/scaling/selection",
            comparability="NOT_DIRECTLY_COMPARABLE: reference notebook and source snapshot equality not verified",
        )
        if final_refit:
            model, scaler, reg = fit_prepared(samples, mapping, config)
            model.write().save(store.uri(base + "/final/spark_model"))
            metadata = {
                "scaler": scaler,
                "regularization": reg,
                "province_mapping": mapping,
                "feature_order": feature_metadata(mapping),
                "training_cutoff": report["training_cutoff"],
                "training_weeks": report["panel_weeks"] - config.lookback,
                "training_samples": reg["training_samples"],
            }
            write_json(store, base + "/final/metadata.json", metadata)
            save_table("ridge_final_coefficients", coefficients(model, mapping, spark))
            report.update(status="TRAINED", final_model=metadata)
        save_table("ridge_model_errors", spark.createDataFrame([], errors_schema))
        write_json(store, base + "/manifest.json", report)
        return report
    except Exception as exc:
        # No success manifest on incomplete metrics/models; preserve actionable run errors.
        save_table(
            "ridge_model_errors",
            spark.createDataFrame(
                [
                    (
                        "benchmark",
                        type(exc).__name__,
                        "run incomplete; inspect driver log",
                    )
                ],
                errors_schema,
            ),
        )
        raise
    finally:
        samples.unpersist()


def load_manifest(store, run_id):
    report = json.loads(
        store.get(prefix(run_id) + "/manifest.json", max_bytes=10_000_000)
    )
    if (
        report.get("artifact_version") != ARTIFACT_VERSION
        or report.get("status") != "TRAINED"
        or not report.get("final_model")
    ):
        raise ValueError(
            "inference requires a completed final refit, not the last evaluation fold"
        )
    if report["runtime"] != runtime():
        raise ValueError("incompatible saved Ridge runtime")
    if report["feature_order"] != feature_metadata(report["province_mapping"]):
        raise ValueError("incompatible saved feature order")
    return report


def next_week_features(panel, mapping):
    validate_panel(panel)
    keys = panel.sparkSession.createDataFrame(
        [(p["province_id"],) for p in mapping], "province_id string"
    )
    panel = panel.join(keys, "province_id", "left_semi")
    order = Window.partitionBy("province_id").orderBy("week_start")
    latest = panel.agg(F.max("week_start")).first()[0]
    result = (
        panel.select(
            "province_id",
            "province_name",
            "week_start",
            *[
                F.lag(c, lag - 1).over(order).alias(f"{c}_lag{lag}")
                for lag in range(8, 0, -1)
                for c in CHANNELS
            ],
        )
        .filter(F.col("week_start") == latest)
        .withColumn("forecast_week", F.date_add("week_start", 7))
        .drop("week_start")
    )
    if (
        result.count() != len(mapping)
        or result.filter(F.col("target_lag8").isNull()).limit(1).count()
    ):
        raise ValueError(
            "next-week prediction requires exactly eight complete history weeks per saved province"
        )
    return result


def forecast(spark, inputs, store, run_id, *, as_of_date=None, observed_through=None):
    report = load_manifest(store, run_id)
    config = ForecastConfig(**report["config"])
    config = replace(
        config,
        as_of_date=as_of_date or config.as_of_date,
        observed_through=observed_through or config.observed_through,
    )
    sources, lineage = read_sources(spark, inputs)
    counts, provinces, first, audit = weekly_counts(sources, config)
    panel, _, _ = make_panel(counts, provinces, first, config)
    if config.last_complete_week + timedelta(days=6) < date.fromisoformat(
        report["training_cutoff"]
    ):
        raise ValueError("inference history cannot precede final model training cutoff")
    mapping = report["province_mapping"]
    features = next_week_features(panel, mapping)
    model = LinearRegressionModel.load(store.uri(prefix(run_id) + "/final/spark_model"))
    metadata = report["final_model"]
    if model.numFeatures != report["feature_dimension"] or not math.isclose(
        model.getRegParam(),
        metadata["regularization"]["regParam"],
        rel_tol=1e-12,
        abs_tol=1e-15,
    ):
        raise ValueError("saved Spark model/metadata mismatch")
    predictions = predict(model, features, metadata["scaler"], mapping)
    output = predictions.select(
        "province_id",
        "province_name",
        "forecast_week",
        F.col("prediction").alias("predicted_geo_linked_intent_posts"),
        "raw_prediction",
        F.lit("pooled_provincial_ridge").alias("model_name"),
        F.lit(run_id).alias("model_version"),
        F.lit(report["training_cutoff"]).cast("date").alias("training_cutoff"),
        F.lit(lineage["data_version"]).alias("data_version"),
        F.current_timestamp().alias("generated_at"),
    )
    unique_keys(output, ("province_id", "forecast_week"))
    if (
        output.count() != len(mapping)
        or output.filter(
            F.isnan("raw_prediction")
            | F.col("raw_prediction").isNull()
            | (F.abs(F.col("raw_prediction")) == float("inf"))
            | (F.col("predicted_geo_linked_intent_posts") < 0)
            | (
                F.col("forecast_week")
                != F.lit(config.last_complete_week + timedelta(days=7))
            )
        )
        .limit(1)
        .count()
    ):
        raise ValueError("incomplete or invalid next-week forecast")
    return output, {
        "status": "PREDICTED",
        "as_of_date": config.as_of_date,
        "observed_through": config.observed_through,
        "run_id": run_id,
        "data_version": lineage["data_version"],
        "forecast_week": (config.last_complete_week + timedelta(days=7)).isoformat(),
        "rows": len(mapping),
        "training_cutoff": report["training_cutoff"],
        "data_audit": audit,
    }


def publish_forecast(output, root):
    path = staging_uri(root) + "/gold/provincial_demand_forecast_v1"
    keys = ("province_id", "forecast_week", "model_version", "data_version")
    unique_keys(output, keys)
    if DeltaTable.isDeltaTable(output.sparkSession, path):
        old = output.sparkSession.read.format("delta").load(path)
        joined = output.alias("n").join(old.alias("o"), list(keys))
        if (
            joined.filter(
                ~F.col("n.raw_prediction").eqNullSafe(F.col("o.raw_prediction"))
                | ~F.col("n.province_name").eqNullSafe(F.col("o.province_name"))
                | ~F.col("n.training_cutoff").eqNullSafe(F.col("o.training_cutoff"))
            )
            .limit(1)
            .count()
        ):
            raise ValueError("forecast replay conflicts with persisted business values")
    merge_delta(output, path, keys=keys, initialize_empty=True)
    return path
