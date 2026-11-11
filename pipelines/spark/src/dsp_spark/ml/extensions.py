"""Separate analytics/forecasting extension contracts; no unbenchmarked training."""

from typing import Protocol

from pyspark.sql import DataFrame


class TrendForecaster(Protocol):
    """Implement only after benchmark, horizon, temporal split and leakage policy review."""

    def fit(self, history: DataFrame, *, cutoff: str, horizon_days: int) -> str: ...
    def predict(self, history: DataFrame, *, artifact_uri: str) -> DataFrame: ...


def spatial_aggregations(venues, mentions, posts, admin_column):
    """Distinct venue supply and distinct interested posts, not supervised learning.

    A post can mention several localities, but counts once within each locality.
    Unknown geography stays excluded and must be reported by the caller.
    """
    from pyspark.sql import functions as F

    from .data import require_schema, unique_keys

    if admin_column not in (
        "old_province_id",
        "old_district_id",
        "old_ward_id",
        "new_province_id",
        "new_ward_id",
    ):
        raise ValueError("choose an explicit supported administrative geography")
    require_schema(venues, {"venue_id": "string", admin_column: "string"})
    require_schema(mentions, {"content_id": "string", admin_column: "string"})
    unique_keys(venues, ("venue_id",))
    unique_keys(posts, ("content_id",))
    supply = (
        venues.filter(F.col(admin_column).isNotNull())
        .groupBy(admin_column)
        .agg(F.countDistinct("venue_id").alias("venue_count"))
    )
    demand = (
        mentions.join(posts.select("content_id"), "content_id", "left_semi")
        .filter(F.col(admin_column).isNotNull())
        .groupBy(admin_column)
        .agg(F.countDistinct("content_id").alias("interested_post_count"))
    )
    return (
        supply.join(demand, admin_column, "full")
        .fillna(0, subset=["venue_count", "interested_post_count"])
        .withColumn(
            "posts_per_venue",
            F.when(
                F.col("venue_count") > 0,
                F.col("interested_post_count") / F.col("venue_count"),
            ),
        )
    )
