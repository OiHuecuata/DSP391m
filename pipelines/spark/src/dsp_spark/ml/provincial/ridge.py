"""Spark-only Ridge; account for Spark 4.0.2 WLS label standardization."""

from pyspark.ml.regression import LinearRegression
from pyspark.sql import functions as F

from .features import assemble, fit_scaler


def fit_ridge(train, alpha=10.0, *, max_iter=1000, tol=1e-6):
    if train.sparkSession.version != "4.0.2":
        raise ValueError("regularization conversion is verified only for Spark 4.0.2")
    stats = train.agg(
        F.count("*").alias("n"), F.stddev_pop("label").alias("label_std")
    ).first()
    if stats.n < 2:
        raise ValueError("Ridge requires at least two training samples")
    dimension = train.select("features").first().features.size
    if dimension > 4096:
        raise ValueError("verified auto/WLS conversion requires <=4096 features")
    # WLS normal objective is SSE/(2*n) + regParam/(2*std_pop(y))*||w||².
    # Therefore regParam = alpha*std_pop(y)/n matches sklearn's SSE+alpha*||w||².
    label_std = float(stats.label_std or 0.0)
    reg_param = float(alpha) * label_std / stats.n
    estimator = LinearRegression(
        featuresCol="features",
        labelCol="label",
        predictionCol="raw_prediction",
        regParam=reg_param,
        elasticNetParam=0.0,
        fitIntercept=True,
        standardization=False,
        loss="squaredError",
        solver="auto",
        maxIter=max_iter,
        tol=tol,
    )
    model = estimator.fit(train)
    return model, {
        "alpha": float(alpha),
        "regParam": reg_param,
        "training_samples": stats.n,
        "label_population_std": label_std,
        "feature_dimension": dimension,
        "conversion": "regParam = alpha * population_std(label_train) / n_train; Spark 4.0.2 auto/WLS, no weights",
        "constant_label": label_std == 0.0,
    }


def fit_prepared(train, mapping, config):
    scaler = fit_scaler(train, config.lookback)
    prepared = assemble(train, scaler, mapping, lookback=config.lookback).persist()
    try:
        model, regularization = fit_ridge(
            prepared, config.alpha, max_iter=config.max_iter, tol=config.tol
        )
    finally:
        prepared.unpersist()
    return model, scaler, regularization


def predict(model, frame, scaler, mapping, *, occlude=None):
    return model.transform(
        assemble(frame, scaler, mapping, occlude=occlude)
    ).withColumn("prediction", F.greatest(F.lit(0.0), F.col("raw_prediction")))
