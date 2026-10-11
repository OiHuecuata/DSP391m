"""Pure DataFrame activity aggregation over mentioned places, without Delta I/O."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ..transform.location import location_expression

ENGAGEMENT_COLUMNS = ("like_count", "reply_count", "repost_count", "quote_count")


def latest_social_posts(silver: DataFrame) -> DataFrame:
    """Choose newest collection observation per entity, not newest replay.

    Observation time takes precedence over transport/ingestion time, so replaying
    an old run later cannot replace a newer collection. Remaining ties use
    emitted_at, lakehouse_ingested_at and descending lexical event_id. Valid
    Silver event IDs uniquely identify observations; identical event replays
    therefore cannot affect content metrics.
    """
    window = Window.partitionBy("record_id").orderBy(
        F.to_timestamp("collected_at").desc_nulls_last(),
        F.to_timestamp("emitted_at").desc_nulls_last(),
        F.col("lakehouse_ingested_at").desc_nulls_last(),
        F.col("event_id").desc(),
    )
    return (
        silver.withColumn("_observation_rank", F.row_number().over(window))
        .filter(F.col("_observation_rank") == 1)
        .drop("_observation_rank")
    )


def eligible_location_posts(latest: DataFrame) -> DataFrame:
    # Selection precedes exclusion: do not resurrect an older post's publication
    # time or locations when the latest canonical observation lacks them.
    mentions = (
        latest.filter(F.col("published_at").isNotNull())
        .withColumn("location_name", F.explode(F.array_distinct("detected_locations")))
        .filter(F.col("location_name").isNotNull() & (F.length("location_name") > 0))
        # Canonical published_at is a fixed-precision UTC Z string. Reading its
        # calendar component preserves UTC dates regardless of session timezone.
        .withColumn("activity_date", F.substring("published_at", 1, 10))
    )

    # Deduplicate AFTER alias resolution: two aliases in one post contribute once.
    mentions = mentions.withColumn(
        "_location", location_expression(F.col("location_name"))
    ).drop("location_name")
    return (
        mentions.select("*", "_location.*")
        .drop("_location")
        .dropDuplicates(["record_id", "activity_date", "location_id", "location_name"])
    )


def social_activity_daily(silver: DataFrame, *, generated_at: str) -> DataFrame:
    """Full deterministic aggregation; only the supplied rebuild time varies."""
    mentions = eligible_location_posts(
        latest_social_posts(
            silver.filter(
                (F.col("source") == "threads")
                & ~F.coalesce(F.col("is_reply"), F.lit(False))
            )
        )
    )
    grouped = mentions.groupBy("activity_date", "location_id", "location_name").agg(
        F.count("record_id").alias("post_count"),
        *(
            F.sum(F.coalesce(F.col(name), F.lit(0))).alias(name)
            for name in ENGAGEMENT_COLUMNS
        ),
        F.min("published_at").alias("first_published_at"),
        F.max("published_at").alias("last_published_at"),
    )
    return (
        grouped.withColumn("schema_version", F.lit("1"))
        .withColumn(
            "engagement_total",
            sum((F.col(name) for name in ENGAGEMENT_COLUMNS), F.lit(0)),
        )
        .withColumn("gold_generated_at", F.lit(generated_at))
    )


def social_source_activity_daily(silver, *, generated_at):
    """Separate native measures by source; geography is text mentions only."""
    mentions = eligible_location_posts(latest_social_posts(silver))
    metrics = {
        "threads_engagement": (
            "threads",
            sum((F.coalesce(F.col(k), F.lit(0)) for k in ENGAGEMENT_COLUMNS), F.lit(0)),
        ),
        "youtube_views": ("youtube", F.col("view_count")),
        "youtube_likes": ("youtube", F.col("like_count")),
        "youtube_comment_count": ("youtube", F.col("reply_count")),
        "youtube_comment_likes": ("youtube_comment", F.col("like_count")),
    }
    return (
        mentions.groupBy("activity_date", "location_id", "location_name", "source")
        .agg(
            F.count("record_id").alias("record_count"),
            *(
                F.sum(F.when(F.col("source") == source, expression)).alias(name)
                for name, (source, expression) in metrics.items()
            ),
        )
        .withColumn("schema_version", F.lit("1"))
        .withColumn("gold_generated_at", F.lit(generated_at))
    )
