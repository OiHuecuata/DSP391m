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


def merge_delta(
    frame: DataFrame,
    path: str,
    *,
    keys: tuple[str, ...],
    initialize_empty: bool = False,
) -> None:
    """Insert unseen keys only; first persisted observation wins exact replay.

    Intended for one writer per table. Delta optimistic concurrency failures are
    surfaced; neither silent overwrite nor schema evolution is enabled.
    Required datasets can opt into schema-correct initialization with zero rows.
    Later empty batches validate the schema and never clear existing observations.
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
    path = storage_uri(path)
    if not DeltaTable.isDeltaTable(frame.sparkSession, path):
        if initialize_empty or not unique.isEmpty():
            unique.write.format("delta").mode("errorifexists").option(
                "mergeSchema", "false"
            ).save(path)
        return
    _aligned(read_delta(frame.sparkSession, path), unique)
    if unique.isEmpty():
        return
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


def reconcile_delta(
    frame: DataFrame, path: str, *, keys: tuple[str, ...], previous_schema=None
) -> None:
    """Synchronize a validated derived snapshot by key; retain Delta history.

    Update changed values, insert new keys, remove stale current projections.
    The caller supplies the complete permitted projection, including retained
    historical snapshot dates. Never use this for authoritative Bronze records.
    """
    path = storage_uri(path)
    if not keys or any(k not in frame.columns for k in keys):
        raise ValueError("derived reconciliation requires valid keys")
    if (
        frame.filter(F.expr(" OR ".join(f"`{k}` IS NULL" for k in keys)))
        .limit(1)
        .count()
    ):
        raise ValueError("derived keys must be nonnull")
    if frame.groupBy(*keys).count().filter("count > 1").limit(1).count():
        raise ValueError("derived keys must be unique")
    if not DeltaTable.isDeltaTable(frame.sparkSession, path):
        frame.write.format("delta").mode("errorifexists").option(
            "mergeSchema", "false"
        ).save(path)
        return
    existing = read_delta(frame.sparkSession, path)
    if (
        previous_schema is not None
        and [(f.name, f.dataType.simpleString()) for f in existing.schema]
        == [(f.name, f.dataType.simpleString()) for f in previous_schema]
        and [(f.name, f.dataType.simpleString()) for f in existing.schema]
        != [(f.name, f.dataType.simpleString()) for f in frame.schema]
    ):
        replace_delta(frame, path, previous_schema=previous_schema)
        return
    _aligned(existing, frame)
    condition = " AND ".join(f"target.`{k}` = incoming.`{k}`" for k in keys)
    changed = " OR ".join(
        f"NOT (target.`{c}` <=> incoming.`{c}`)" for c in frame.columns
    )
    (
        DeltaTable.forPath(frame.sparkSession, path)
        .alias("target")
        .merge(frame.alias("incoming"), condition)
        .whenMatchedUpdateAll(condition=changed)
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )
