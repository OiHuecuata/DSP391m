"""Replay-derived Silver tables from admitted observations, with stable ordering."""

from pyspark.sql import Window
from pyspark.sql import functions as F

from ...io.delta import read_delta, replace_delta
from ..clean.contracts import spark_schema
from .contracts import SILVER, path
from .features import content, geo_mentions, intent_evidence, snapshot


def replace_if_changed(frame, location):
    from delta.tables import DeltaTable

    spark = frame.sparkSession
    if DeltaTable.isDeltaTable(spark, location):
        existing = read_delta(spark, location)
        if existing.schema.simpleString() != frame.schema.simpleString():
            raise ValueError("Research schema drift")
        if (
            not existing.exceptAll(frame).limit(1).count()
            and not frame.exceptAll(existing).limit(1).count()
        ):
            return
    replace_delta(frame, location)


def checked(frame, kind, keys):
    from dsp_collector.normalizers.observation import canonical_json

    from ..clean.contracts import contract_documents, validation_reason

    documents = contract_documents()
    bad = (
        frame.rdd.map(
            lambda row: validation_reason(
                canonical_json(row.asDict(recursive=True)), kind, documents
            )
        )
        .filter(lambda reason: reason is not None)
        .take(1)
    )
    if bad:
        raise ValueError(f"{kind}: {bad[0]}")
    if (
        keys
        and frame.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count()
    ):
        raise ValueError(f"Duplicate {kind} grain")
    return frame


def build(spark, settings, *, aliases=(), annotations=None):
    from .references import qualify

    annotations = annotations or {}
    observations = read_delta(spark, path(settings, "admitted_threads_observations"))
    from datetime import UTC, datetime

    now = datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    observations = observations.filter(
        F.col("retention_expires_at").isNull() | (F.col("retention_expires_at") > now)
    )
    latest = (
        observations.withColumn(
            "_rank",
            F.row_number().over(
                Window.partitionBy("native_record_key").orderBy(
                    F.col("observed_at").desc(), F.col("observation_id").desc()
                )
            ),
        )
        .filter("_rank = 1")
        .drop("_rank")
    )
    rows = latest.rdd.map(
        lambda row: qualify(content(row.asDict(recursive=True)), annotations)
    )
    values = spark.createDataFrame(rows, spark_schema("silver_social_posts"))
    duplicate_window = Window.partitionBy("text_hash").orderBy("content_id")
    values = values.withColumn(
        "is_duplicate",
        (F.length("text_normalized") > 0) & (F.row_number().over(duplicate_window) > 1),
    )
    frames = {
        "silver_social_posts": values.filter("content_type = 'Post'"),
        "silver_social_comments": values.filter("content_type = 'Comment'"),
    }
    raw_snapshots = observations.rdd.map(
        lambda row: snapshot(row.asDict(recursive=True))
    )
    # Add derived fields after creating the source-supported part of the schema.
    schema = spark_schema("silver_social_engagement_snapshots")
    source_fields = [
        f
        for f in schema
        if not f.name.endswith("_delta")
        and f.name not in ("previous_observation_id", "is_latest_eligible")
    ]
    from pyspark.sql.types import StructType

    snapshots = spark.createDataFrame(raw_snapshots, StructType(source_fields))
    order = Window.partitionBy("content_id").orderBy("observed_at", "observation_id")
    for metric in ("views", "likes", "comments", "shares", "reposts", "quotes"):
        snapshots = snapshots.withColumn(
            metric + "_delta", F.col(metric) - F.lag(metric).over(order)
        )
    snapshots = snapshots.withColumn(
        "previous_observation_id", F.lag("observation_id").over(order)
    )
    snapshots = snapshots.withColumn(
        "_eligible_rank",
        F.row_number().over(
            Window.partitionBy("content_id", "snapshot_status").orderBy(
                F.col("observed_at").desc(), F.col("observation_id").desc()
            )
        ),
    )
    snapshots = (
        snapshots.withColumn(
            "is_latest_eligible",
            (F.col("snapshot_status") == "OBSERVED") & (F.col("_eligible_rank") == 1),
        )
        .drop("_eligible_rank")
        .select(*schema.fieldNames())
    )
    frames["silver_social_engagement_snapshots"] = snapshots
    feature_rows = values.rdd.map(lambda row: row.asDict(recursive=True))
    frames["silver_social_geo_mentions"] = spark.createDataFrame(
        feature_rows.flatMap(lambda value: geo_mentions(value, aliases)),
        spark_schema("silver_social_geo_mentions"),
    )
    frames["silver_social_intent_evidence"] = spark.createDataFrame(
        feature_rows.flatMap(
            lambda value: intent_evidence(value, annotations.get(value["content_id"]))
        ),
        spark_schema("silver_social_intent_evidence"),
    )
    counts = {}
    from .contracts import GRAINS

    for name in SILVER:
        frame = frames[name].cache()
        checked(frame, name, GRAINS[name])
        replace_if_changed(frame, path(settings, name))
        counts[name] = frame.count()
        frame.unpersist()
    return counts
