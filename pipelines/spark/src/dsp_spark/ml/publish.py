"""Dedicated staging assignment Delta table; immutable snapshot keys and rollback."""

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from ..io.delta import merge_delta
from .config import staging_uri
from .data import unique_keys

TABLE_NAME = "demand_cluster_assignments_v1"
KEYS = ("content_id", "model_version", "source_snapshot_id")


def assignment_path(root):
    return staging_uri(root).rstrip("/") + "/gold/" + TABLE_NAME


def publish(assignments, root):
    path = assignment_path(root)
    unique_keys(assignments, KEYS)
    spark = assignments.sparkSession
    before = None
    if DeltaTable.isDeltaTable(spark, path):
        before = int(DeltaTable.forPath(spark, path).history(1).first().version)
        old = spark.read.format("delta").load(path)
        # Replay timestamps may differ; business assignments for one snapshot must agree.
        match = assignments.alias("new").join(old.alias("old"), list(KEYS))
        if (
            match.filter(
                ~F.col("new.cluster_id").eqNullSafe(F.col("old.cluster_id"))
                | ~F.col("new.distance_to_centroid").eqNullSafe(
                    F.col("old.distance_to_centroid")
                )
            )
            .limit(1)
            .count()
        ):
            raise ValueError("non-deterministic assignment for the same model/snapshot")
    merge_delta(assignments, path, keys=KEYS, initialize_empty=True)
    after = int(DeltaTable.forPath(spark, path).history(1).first().version)
    return {
        "path": path,
        "before_version": before,
        "after_version": after,
        "rollback": "restore this dedicated table to before_version; first-write rollback uses reader version selection",
    }


def rollback(spark, root, version):
    if version < 0:
        raise ValueError("rollback requires an existing nonnegative Delta version")
    DeltaTable.forPath(spark, assignment_path(root)).restoreToVersion(version)
