"""Generic Delta persistence with insert-only observation replay merges."""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .s3 import storage_uri


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(storage_uri(path))


def _aligned(existing: DataFrame, incoming: DataFrame) -> None:
    if [(f.name, f.dataType.simpleString()) for f in existing.schema] != [
        (f.name, f.dataType.simpleString()) for f in incoming.schema
    ]:
        raise ValueError("Delta schema mismatch; implicit evolution is disabled")


def write_delta(frame: DataFrame, path: str) -> None:
    path = storage_uri(path)
    if DeltaTable.isDeltaTable(frame.sparkSession, path):
        _aligned(read_delta(frame.sparkSession, path), frame)
    frame.write.format("delta").mode("append").option("mergeSchema", "false").save(path)


def merge_delta(frame: DataFrame, path: str, *, keys: tuple[str, ...]) -> None:
    """Insert unseen keys only; first persisted observation wins exact replay.

    Intended for one writer per table. Delta optimistic concurrency failures are
    surfaced; neither silent overwrite nor schema evolution is enabled.
    """
    if not keys or any(key not in frame.columns for key in keys):
        raise ValueError("merge keys must be present")
    if any("`" in key for key in keys):
        raise ValueError("invalid merge key")
    if (
        frame.filter(F.expr(" OR ".join(f"`{key}` IS NULL" for key in keys)))
        .limit(1)
        .count()
    ):
        raise ValueError("merge keys must be nonnull")
    unique = frame.dropDuplicates(list(keys))
    if unique.isEmpty():
        return
    path = storage_uri(path)
    if not DeltaTable.isDeltaTable(frame.sparkSession, path):
        unique.write.format("delta").mode("errorifexists").option(
            "mergeSchema", "false"
        ).save(path)
        return
    _aligned(read_delta(frame.sparkSession, path), unique)
    condition = " AND ".join(f"target.`{key}` = incoming.`{key}`" for key in keys)
    (
        DeltaTable.forPath(frame.sparkSession, path)
        .alias("target")
        .merge(unique.alias("incoming"), condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


def replace_delta(frame: DataFrame, path: str, *, previous_schema=None) -> None:
    """Atomically replace derived state, including removal of stale/empty groups.

    Observation insert-only merges remain unchanged. An explicit previous schema
    permits a caller-defined derived-table migration; all other schemas fail.
    """
    path = storage_uri(path)
    migrating = False
    if DeltaTable.isDeltaTable(frame.sparkSession, path):
        existing = read_delta(frame.sparkSession, path)
        if previous_schema is not None and [
            (f.name, f.dataType.simpleString()) for f in existing.schema
        ] == [(f.name, f.dataType.simpleString()) for f in previous_schema]:
            migrating = True
        else:
            _aligned(existing, frame)
    frame.write.format("delta").mode("overwrite").option("mergeSchema", "false").option(
        "overwriteSchema", str(migrating).lower()
    ).save(path)
