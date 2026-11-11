"""Bounded UMAP fitting; persisted reference coordinates and distributed transform."""

import io

from pyspark.ml.functions import array_to_vector

from .embedding import checked_vectors


def fit_umap(frame, config, dimension):
    import joblib
    import numpy as np
    from umap import UMAP

    count = frame.count()
    # The factor covers array copies and working storage, not a promise of total RSS.
    estimated_bytes = count * (dimension + config.n_components) * 8 * 8
    if count > config.fit_max_rows or estimated_bytes > config.fit_max_bytes:
        raise ValueError(
            "UMAP bounded fit limit exceeded; use an explicitly benchmarked sampling protocol"
        )
    if count <= max(config.n_neighbors, config.n_components + 1):
        raise ValueError("too few rows for configured UMAP baseline")
    rows = (
        frame.select("content_id", "text_hash_ml", "embedding")
        .orderBy("content_id")
        .limit(config.fit_max_rows + 1)
        .collect()
    )
    if len(rows) != count:
        raise ValueError("UMAP input changed during bounded collection")
    matrix = checked_vectors(
        [r.embedding for r in rows], dimension, normalized=True
    ).astype(np.float32)
    reducer = UMAP(
        n_components=config.n_components,
        n_neighbors=config.n_neighbors,
        min_dist=config.min_dist,
        metric=config.metric,
        random_state=config.random_state,
        transform_seed=config.transform_seed,
        n_jobs=1,
    )
    coordinates = checked_vectors(reducer.fit_transform(matrix), config.n_components)
    stream = io.BytesIO()
    joblib.dump(reducer, stream)
    reference = frame.sparkSession.createDataFrame(
        [(r.content_id, r.text_hash_ml, v.tolist()) for r, v in zip(rows, coordinates)],
        "content_id string, text_hash_ml string, reduced array<double>",
    )
    return (
        stream.getvalue(),
        reference,
        {
            "fit_rows": count,
            "estimated_array_working_bytes": estimated_bytes,
            "mode": "bounded_full_corpus",
            "inference_mode": "reference_lookup_then_per_row_transform",
        },
    )


def transform_umap(frame, payload, reference, config):
    """Per-row new-point transform makes results independent of batch/partition order.

    UMAP batch transforms can change with query ordering. This conservative path is
    slower but reproducible. Known training points use their exact fitted coordinates.
    """
    spark = frame.sparkSession
    broadcast = spark.sparkContext.broadcast(payload)
    keys = ["content_id", "text_hash_ml"]
    missing = frame.join(reference.select(*keys), keys, "left_anti")

    def partition(rows):
        import joblib
        import numpy as np

        reducer = None
        for row in rows:
            if reducer is None:
                reducer = joblib.load(io.BytesIO(broadcast.value))
            vector = checked_vectors(
                reducer.transform(np.asarray([row.embedding], dtype=np.float32)),
                config.n_components,
            )[0]
            yield row.content_id, row.text_hash_ml, vector.tolist()

    transformed = spark.createDataFrame(
        missing.rdd.mapPartitions(partition), reference.schema
    )
    coords = reference.join(frame.select(*keys), keys, "left_semi").unionByName(
        transformed
    )
    return frame.join(coords, keys, "inner").withColumn(
        "features", array_to_vector("reduced")
    )
