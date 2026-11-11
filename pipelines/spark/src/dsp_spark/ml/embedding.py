"""Bounded sentence inference in Spark partitions, with a content-addressed Delta cache."""

from itertools import islice

from delta.tables import DeltaTable
from pyspark.ml.functions import array_to_vector
from pyspark.sql import functions as F

from ..io.delta import merge_delta
from .config import staging_uri
from .data import unique_keys

CACHE_KEYS = ("content_id", "text_hash_ml", "encoder_key")


def checked_vectors(values, dimension, *, normalized=False):
    import numpy as np

    values = np.asarray(values, dtype=np.float64)
    if (
        values.ndim != 2
        or values.shape[1] != dimension
        or not np.isfinite(values).all()
    ):
        raise ValueError("invalid feature dimension or non-finite values")
    if normalized and not np.allclose(np.linalg.norm(values, axis=1), 1.0, atol=1e-5):
        raise ValueError("semantic embeddings must be nonzero and L2-normalized")
    return values


def sentence_partition(rows, config):
    first = list(islice(rows, config.batch_size))
    if not first:
        return
    from sentence_transformers import SentenceTransformer

    encoder = SentenceTransformer(
        config.model,
        revision=config.revision,
        device=config.device,
        trust_remote_code=False,
    )
    encoder.max_seq_length = config.max_seq_length
    batch = first
    while batch:
        vectors = checked_vectors(
            encoder.encode(
                [r.text for r in batch],
                batch_size=config.batch_size,
                normalize_embeddings=config.normalize,
                show_progress_bar=False,
            ),
            config.dimension,
            normalized=True,
        )
        for row, vector in zip(batch, vectors):
            yield row.content_id, row.text_hash_ml, config.key, vector.tolist()
        batch = list(islice(rows, config.batch_size))


def embed(
    posts, config, cache_path, *, partitions, partition_encoder=sentence_partition
):
    """The injectable encoder supports offline synthetic integration fixtures only."""
    cache_path = staging_uri(cache_path)
    spark = posts.sparkSession
    requested = posts.select("content_id", "text_hash_ml", "text").withColumn(
        "encoder_key", F.lit(config.key)
    )
    if DeltaTable.isDeltaTable(spark, cache_path):
        existing = spark.read.format("delta").load(cache_path)
        unique_keys(existing, CACHE_KEYS)
        missing = requested.join(
            existing.select(*CACHE_KEYS), list(CACHE_KEYS), "left_anti"
        )
    else:
        existing = None
        missing = requested
    encoded = spark.createDataFrame(
        missing.repartition(partitions, "content_id").rdd.mapPartitions(
            lambda rows: partition_encoder(rows, config)
        ),
        "content_id string, text_hash_ml string, encoder_key string, embedding array<double>",
    ).persist()
    try:
        merge_delta(encoded, cache_path, keys=CACHE_KEYS, initialize_empty=True)
    finally:
        encoded.unpersist()
    cached = spark.read.format("delta").load(cache_path)
    result = requested.join(cached, list(CACHE_KEYS), "inner")
    validate = F.udf(
        lambda v: bool(
            checked_vectors([v], config.dimension, normalized=True).shape[0]
        ),
        "boolean",
    )
    # Force evaluation of validation: count() alone can prune a validation projection.
    if result.filter(~validate("embedding")).limit(1).count():
        raise ValueError("invalid embedding cache")
    if result.count() != requested.count():
        raise ValueError("embedding cache cardinality mismatch")
    return result.withColumn("semantic_features", array_to_vector("embedding"))
