"""Cosine silhouette on semantic vectors; CH on Euclidean UMAP coordinates."""

import math

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.stat import Summarizer
from pyspark.sql import functions as F


def estimator(k, config, seed=None):
    return KMeans(
        k=k,
        seed=config.seed if seed is None else seed,
        maxIter=config.max_iter,
        tol=config.tol,
        featuresCol="features",
        predictionCol="cluster_id",
        distanceMeasure="euclidean",
        initMode="k-means||",
        initSteps=2,
    )


def calinski_harabasz(predictions):
    """Sufficient statistics only: O(K*d) driver memory, matching sklearn's CH."""
    summary = Summarizer.metrics("count", "mean", "normL2")
    rows = (
        predictions.groupBy("cluster_id")
        .agg(summary.summary(F.col("features")).alias("s"))
        .collect()
    )
    n = sum(r.s["count"] for r in rows)
    k = len(rows)
    if not 1 < k < n:
        return None
    dimension = len(rows[0].s["mean"])
    mean = [
        sum(r.s["count"] * r.s["mean"][i] for r in rows) / n for i in range(dimension)
    ]
    between = sum(
        r.s["count"] * sum((r.s["mean"][i] - mean[i]) ** 2 for i in range(dimension))
        for r in rows
    )
    within = max(
        0.0,
        sum(
            sum(x * x for x in r.s["normL2"])
            - r.s["count"] * sum(x * x for x in r.s["mean"])
            for r in rows
        ),
    )
    return 1.0 if within == 0 else (between / (k - 1)) / (within / (n - k))


def adjusted_rand(paired):
    """Exact ARI from a distributed contingency table; no assignment collection."""
    cells = paired.groupBy("reference_cluster", "cluster_id").count()
    choose = lambda v: v * (v - 1) / 2.0
    cross = cells.agg(
        F.sum(F.col("count") * (F.col("count") - 1) / 2), F.sum("count")
    ).first()
    n = cross[1] or 0
    if n < 2:
        return 1.0
    a = (
        cells.groupBy("reference_cluster")
        .agg(F.sum("count").alias("n"))
        .agg(F.sum(F.col("n") * (F.col("n") - 1) / 2))
        .first()[0]
    )
    b = (
        cells.groupBy("cluster_id")
        .agg(F.sum("count").alias("n"))
        .agg(F.sum(F.col("n") * (F.col("n") - 1) / 2))
        .first()[0]
    )
    expected = a * b / choose(n)
    maximum = (a + b) / 2
    return 1.0 if maximum == expected else (cross[0] - expected) / (maximum - expected)


def evaluate(predictions):
    sizes = {
        str(r.cluster_id): r["count"]
        for r in predictions.groupBy("cluster_id").count().collect()
    }
    n = sum(sizes.values())
    valid = 1 < len(sizes) < n
    return {
        "rows": n,
        "cosine_silhouette_original_embeddings": (
            ClusteringEvaluator(
                featuresCol="semantic_features",
                predictionCol="cluster_id",
                distanceMeasure="cosine",
            ).evaluate(predictions)
            if valid
            else None
        ),
        "calinski_harabasz_umap_euclidean": calinski_harabasz(predictions),
        "coverage": 1.0 if n else 0.0,
        "noise_ratio": 0.0,
        "cluster_sizes": sizes,
        "max_min_cluster_ratio": (
            max(sizes.values()) / min(sizes.values()) if sizes else None
        ),
        "cluster_size_entropy": (
            -sum((s / n) * math.log(s / n) for s in sizes.values()) if n else None
        ),
    }


def stability(features, predictions, k, config):
    reference = predictions.select(
        "content_id", F.col("cluster_id").alias("reference_cluster")
    )
    runs = []
    for seed in config.stability_seeds:
        # Hash threshold yields reproducible membership independent of partition layout.
        subset = features.filter(
            F.pmod(F.xxhash64("content_id", F.lit(seed)), F.lit(1_000_000))
            < int(config.stability_fraction * 1_000_000)
        ).persist()
        try:
            n = subset.count()
            if n <= k:
                runs.append(
                    {
                        "seed": seed,
                        "rows": n,
                        "status": "INSUFFICIENT_ROWS",
                        "ari": None,
                    }
                )
                continue
            model = estimator(k, config, seed).fit(subset)
            paired = (
                model.transform(subset)
                .select("content_id", "cluster_id")
                .join(reference, "content_id")
            )
            runs.append(
                {
                    "seed": seed,
                    "rows": n,
                    "status": "TESTED",
                    "ari": adjusted_rand(paired),
                }
            )
        finally:
            subset.unpersist()
    scores = [r["ari"] for r in runs if r["ari"] is not None]
    return {
        "protocol": "refit KMeans on hash-selected subsets; fixed encoder and fitted UMAP; compare on subset to full fit",
        "fraction": config.stability_fraction,
        "runs": runs,
        "mean_ari": sum(scores) / len(scores) if scores else None,
    }


def compare_baseline(predictions, baseline):
    from .data import unique_keys

    unique_keys(baseline, ("content_id",))
    joined = predictions.select("content_id", "cluster_id").join(
        baseline.select("content_id", F.col("cluster_id").alias("reference_cluster")),
        "content_id",
    )
    overlap = joined.count()
    return {
        "overlap_rows": overlap,
        "ari": adjusted_rand(joined) if overlap else None,
        "comparability": "NOT_DIRECTLY_COMPARABLE",
        "note": "ARI on overlapping IDs only; verify corpus, encoder, UMAP artifact and protocol separately",
    }
