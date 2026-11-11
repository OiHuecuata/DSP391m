"""Train candidates and run batch inference with the exact persisted preprocessing."""

import hashlib
import resource
import time
from dataclasses import asdict
from datetime import UTC, datetime
from uuid import uuid4

from pyspark.ml.clustering import KMeansModel
from pyspark.ml.functions import array_to_vector
from pyspark.sql import functions as F

from .config import ARTIFACT_VERSION, TEXT_VERSION, TrainingConfig, fingerprint
from .data import load_posts
from .embedding import embed
from .evaluation import compare_baseline, estimator, evaluate, stability
from .explain import explain
from .reduction import fit_umap, transform_umap
from .registry import runtime_versions


def add_distance(predictions, model):
    centers = [list(map(float, c)) for c in model.clusterCenters()]
    distance = F.udf(
        lambda vector, cluster: float(
            sum((float(a) - b) ** 2 for a, b in zip(vector, centers[cluster])) ** 0.5
        ),
        "double",
    )
    return predictions.withColumn(
        "distance_to_centroid", distance("features", "cluster_id")
    )


def train(
    spark,
    snapshot,
    config,
    registry,
    cache_path,
    *,
    run_id=None,
    baseline=None,
    partition_encoder=None,
    lexical_ablation=False,
):
    started = time.monotonic()
    posts, audit = load_posts(spark, snapshot)
    if audit["rows"] <= max(config.ks):
        raise ValueError("not enough posts for configured K candidates")
    posts = posts.persist()
    encoded = features = None
    run_id = (
        run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:12]
    )
    # Reserving a run prevents concurrent writers from sharing any artifact path.
    registry.put(f"runs/{run_id}/reservation.json", b'{"status":"RUNNING"}')
    kwargs = {"partition_encoder": partition_encoder} if partition_encoder else {}
    try:
        encoded = embed(
            posts, config.encoder, cache_path, partitions=config.partitions, **kwargs
        ).persist()
        payload, reference, reduction_report = fit_umap(
            encoded, config.reduction, config.encoder.dimension
        )
        registry.put(f"runs/{run_id}/umap.joblib", payload)
        reference.write.format("delta").mode("errorifexists").save(
            registry.uri(f"runs/{run_id}/umap_reference")
        )
        features = (
            encoded.join(reference, ["content_id", "text_hash_ml"])
            .withColumn("features", array_to_vector("reduced"))
            .persist()
        )
        candidates = {}
        for k in config.ks:
            model = estimator(k, config).fit(features)
            model.write().save(registry.uri(f"runs/{run_id}/k{k}/spark_model"))
            predicted = add_distance(model.transform(features), model).persist()
            try:
                metrics = evaluate(predicted)
                metrics["stability"] = stability(features, predicted, k, config)
                metrics["baseline_similarity"] = (
                    compare_baseline(predicted, baseline)
                    if baseline is not None
                    else {
                        "status": "NOT_RUN",
                        "reason": "no versioned Colab content_id assignments supplied",
                    }
                )
                candidates[str(k)] = {
                    "metrics": metrics,
                    "centroids": [list(map(float, c)) for c in model.clusterCenters()],
                    "explainability": explain(
                        predicted, posts, representatives=config.representative_posts
                    ),
                }
                if lexical_ablation:
                    candidates[str(k)]["lexical_ablation"] = lexical_evaluation(
                        posts, predicted, k, config
                    )
            finally:
                predicted.unpersist()
        manifest = {
            "artifact_version": ARTIFACT_VERSION,
            "run_id": run_id,
            "status": "candidate",
            "created_at": datetime.now(UTC).isoformat(),
            "config": asdict(config),
            "text_version": TEXT_VERSION,
            "encoder_key": config.encoder.key,
            "dataset": audit,
            "umap": reduction_report,
            "umap_sha256": hashlib.sha256(payload).hexdigest(),
            "runtime": runtime_versions(),
            "candidates": candidates,
            "primary_candidate": config.primary_candidate,
            "runtime_seconds": time.monotonic() - started,
            "resources": {
                "python_driver_peak_rss_kib": resource.getrusage(
                    resource.RUSAGE_SELF
                ).ru_maxrss,
                "resource_measurement_scope": "Python driver only; JVM/executor peak RSS requires cluster telemetry",
                "spark_master": spark.sparkContext.master,
                "effective_sql_shuffle_partitions": spark.conf.get(
                    "spark.sql.shuffle.partitions"
                ),
                "partitions": config.partitions,
                "spark": {
                    k: v
                    for k, v in spark.sparkContext.getConf().getAll()
                    if k
                    in (
                        "spark.driver.memory",
                        "spark.executor.memory",
                        "spark.executor.cores",
                        "spark.sql.shuffle.partitions",
                    )
                },
                "umap_fit_max_bytes": config.reduction.fit_max_bytes,
            },
            "promotion": "manual review required; no production promotion implemented",
        }
        registry.commit(run_id, manifest)
        return manifest
    finally:
        for frame in (features, encoded, posts):
            if frame is not None:
                frame.unpersist()


def lexical_evaluation(posts, predicted, k, config):
    from pyspark.ml.feature import IDF, HashingTF, Normalizer, RegexTokenizer

    tokens = RegexTokenizer(
        inputCol="text", outputCol="words", pattern="\\W+"
    ).transform(posts)
    tf = HashingTF(inputCol="words", outputCol="tf", numFeatures=4096).transform(tokens)
    tfidf = IDF(inputCol="tf", outputCol="tfidf").fit(tf).transform(tf)
    vectors = Normalizer(inputCol="tfidf", outputCol="features").transform(tfidf)
    lexical = estimator(k, config).fit(vectors).transform(vectors)
    paired = lexical.select("content_id", "cluster_id").join(
        predicted.select("content_id", F.col("cluster_id").alias("reference_cluster")),
        "content_id",
    )
    from .evaluation import adjusted_rand

    return {
        "method": "hashed word TF-IDF 4096 + L2 + Spark KMeans (no UMAP)",
        "ari_to_semantic": adjusted_rand(paired),
        "comparability": "NOT_DIRECTLY_COMPARABLE",
    }


def infer(
    spark,
    snapshot,
    registry,
    run_id,
    k,
    cache_path,
    *,
    after=None,
    partition_encoder=None,
):
    manifest = registry.manifest(run_id)
    config = TrainingConfig.from_dict(manifest["config"])
    if (
        manifest["text_version"] != TEXT_VERSION
        or manifest["encoder_key"] != config.encoder.key
    ):
        raise ValueError("incompatible preprocessing/encoder version")
    if str(k) not in manifest["candidates"]:
        raise ValueError("K is not present in the saved candidate run")
    posts, audit = load_posts(spark, snapshot, after=after)
    kwargs = {"partition_encoder": partition_encoder} if partition_encoder else {}
    encoded = embed(
        posts, config.encoder, cache_path, partitions=config.partitions, **kwargs
    )
    payload = registry.load_umap(run_id, manifest)
    reference = (
        spark.read.format("delta")
        .option("versionAsOf", 0)
        .load(registry.uri(f"runs/{run_id}/umap_reference"))
    )
    features = transform_umap(encoded, payload, reference, config.reduction)
    model = KMeansModel.load(registry.uri(f"runs/{run_id}/k{k}/spark_model"))
    snapshot_id = fingerprint(asdict(snapshot))
    assignments = add_distance(model.transform(features), model).select(
        "content_id",
        F.lit(f"{run_id}/k{k}").alias("model_version"),
        "cluster_id",
        F.current_timestamp().alias("inference_timestamp"),
        F.lit(snapshot_id).alias("source_snapshot_id"),
        F.lit(snapshot.path).alias("source_silver_path"),
        F.lit(snapshot.version).cast("long").alias("source_silver_version"),
        F.lit(snapshot.generation).cast("string").alias("source_generation"),
        F.lit(snapshot.provenance).alias("data_provenance"),
        "distance_to_centroid",
    )
    return assignments, audit
