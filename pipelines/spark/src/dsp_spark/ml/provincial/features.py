"""Ordered lag features and six train-only pooled population scalers."""

import math

from pyspark.ml.feature import VectorAssembler
from pyspark.sql import Window
from pyspark.sql import functions as F

from ..data import unique_keys
from .config import CHANNELS


def numeric_names(lookback=8):
    return [f"{c}_lag{lag}" for lag in range(lookback, 0, -1) for c in CHANNELS]


def feature_metadata(mapping, lookback=8):
    return [
        {
            "feature_name": f"{c}_lag{lag}",
            "lag": lag,
            "feature_group": c,
            "province_id": None,
        }
        for lag in range(lookback, 0, -1)
        for c in CHANNELS
    ] + [
        {
            "feature_name": f"province_onehot::{p['province_id']}",
            "lag": None,
            "feature_group": "province_identity",
            "province_id": p["province_id"],
        }
        for p in mapping
    ]


def validate_panel(panel):
    unique_keys(panel, ("province_id", "week_start"))
    order = Window.partitionBy("province_id").orderBy("week_start")
    checked = panel.withColumn("_previous_week", F.lag("week_start").over(order))
    if (
        checked.filter(
            F.col("_previous_week").isNotNull()
            & (F.datediff("week_start", "_previous_week") != 7)
        )
        .limit(1)
        .count()
    ):
        raise ValueError("missing week in panel before lag construction")
    if panel.filter(F.dayofweek("week_start") != 2).limit(1).count():
        raise ValueError("week_start must be Monday")
    for channel in CHANNELS:
        if (
            panel.filter(
                F.col(channel).isNull()
                | F.isnan(channel)
                | (F.abs(F.col(channel)) == float("inf"))
                | (F.col(channel) < 0)
            )
            .limit(1)
            .count()
        ):
            raise ValueError(f"invalid nonnegative weekly feature: {channel}")


def lag_features(panel, lookback=8):
    validate_panel(panel)
    order = Window.partitionBy("province_id").orderBy("week_start")
    lagged = panel.select(
        "*",
        *[
            F.lag(c, lag).over(order).alias(f"{c}_lag{lag}")
            for lag in range(lookback, 0, -1)
            for c in CHANNELS
        ],
    )
    return (
        lagged.filter(F.col(f"target_lag{lookback}").isNotNull())
        .withColumnRenamed("target", "label")
        .withColumnRenamed("week_start", "forecast_week")
        .withColumn(
            "moving_average",
            sum(F.col(f"target_lag{i}") for i in range(1, 5)) / F.lit(4.0),
        )
    )


def fit_scaler(train, lookback=8):
    pooled = train.select(
        F.explode(
            F.array(
                *[
                    F.struct(
                        *[
                            F.col(f"{c}_lag{lag}").cast("double").alias(c)
                            for c in CHANNELS
                        ]
                    )
                    for lag in range(lookback, 0, -1)
                ]
            )
        ).alias("x")
    ).select("x.*")
    stats = pooled.agg(
        F.count("*").alias("pooled_rows"),
        *[
            fn(F.col(c)).alias(f"{c}_{suffix}")
            for c in CHANNELS
            for fn, suffix in ((F.avg, "mean"), (F.stddev_pop, "std"))
        ],
    ).first()
    if not stats.pooled_rows:
        raise ValueError("empty scaler training set")
    return {
        c: {
            "mean": float(stats[f"{c}_mean"]),
            "scale": float(stats[f"{c}_std"]) if stats[f"{c}_std"] else 1.0,
            "population_std": float(stats[f"{c}_std"]),
            "pooled_rows": stats.pooled_rows,
        }
        for c in CHANNELS
    }


def assemble(frame, scaler, mapping, *, occlude=None, lookback=8):
    if set(scaler) != set(CHANNELS) or len({p["province_id"] for p in mapping}) != len(
        mapping
    ):
        raise ValueError("incompatible scaler/province mapping")
    if not mapping or [p["province_id"] for p in mapping] != sorted(
        p["province_id"] for p in mapping
    ):
        raise ValueError("province mapping must be nonempty and sorted")
    if (
        frame.select("province_id")
        .distinct()
        .join(
            frame.sparkSession.createDataFrame(
                [(p["province_id"],) for p in mapping], "province_id string"
            ),
            "province_id",
            "left_anti",
        )
        .limit(1)
        .count()
    ):
        raise ValueError("unknown province in feature assembly")
    expressions, inputs = [], []
    for i, name in enumerate(numeric_names(lookback)):
        channel = name.rsplit("_lag", 1)[0]
        mean, scale = scaler[channel]["mean"], scaler[channel]["scale"]
        if not math.isfinite(mean) or not math.isfinite(scale) or scale <= 0:
            raise ValueError("invalid saved scaler parameters")
        column = f"_numeric_{i:03}"
        expressions.append(
            (
                F.lit(0.0)
                if channel == occlude
                else (F.col(name) - F.lit(mean)) / F.lit(scale)
            ).alias(column)
        )
        inputs.append(column)
    for i, province in enumerate(mapping):
        column = f"_province_{i:03}"
        expressions.append(
            (F.col("province_id") == province["province_id"])
            .cast("double")
            .alias(column)
        )
        inputs.append(column)
    prepared = frame.select("*", *expressions)
    return VectorAssembler(
        inputCols=inputs, outputCol="features", handleInvalid="error"
    ).transform(prepared)
