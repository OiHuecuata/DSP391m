"""Explainable rolling mentioned-place activity and current supply, without I/O."""

from dataclasses import dataclass
from datetime import date, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ...runtime.config import MarketSignalSettings
from ..transform.location import load_alias_map

SOCIAL_METRICS = (
    "post_count",
    "like_count",
    "reply_count",
    "repost_count",
    "quote_count",
    "engagement_total",
)
SUPPLY_METRICS = (
    "venue_count",
    "known_court_count",
    "venues_with_known_court_count",
    "venues_without_known_court_count",
)


@dataclass(frozen=True)
class AnalysisWindow:
    start_date: str
    end_date: str
    days: int


def select_analysis_window(
    social: DataFrame, settings: MarketSignalSettings
) -> AnalysisWindow:
    end = settings.end_date
    if end is None:
        end = social.agg(F.max("activity_date").alias("end")).first().end
    if end is None:
        raise ValueError(
            "Empty social Gold requires DSP_MARKET_SIGNAL_END_DATE; "
            "no activity date will be invented"
        )
    try:
        last = date.fromisoformat(end)
        first = last - timedelta(days=settings.window_days - 1)
    except (ValueError, TypeError, OverflowError):
        raise ValueError("Invalid social analysis window dates or day range") from None
    return AnalysisWindow(first.isoformat(), last.isoformat(), settings.window_days)


def _canonical_inputs(social, supply):
    catalog = sorted(set(load_alias_map().values()))
    reference = social.sparkSession.createDataFrame(
        catalog, "location_id string, _canonical_name string"
    )
    places = social.select("location_id", "location_name").unionByName(
        supply.select("location_id", "location_name")
    )
    invalid = (
        places.join(reference, "location_id", "left")
        .filter(
            F.col("_canonical_name").isNull()
            | ~F.col("location_name").eqNullSafe(F.col("_canonical_name"))
        )
        .limit(1)
        .collect()
    )
    if invalid:
        raise ValueError(
            "Gold location/catalog mismatch or unmapped location: "
            + str(invalid[0].asDict())
        )
    # Reject accidental duplicate Gold keys rather than multiplying evidence.
    for frame, keys, label in (
        (social, ["activity_date", "location_id"], "social_activity_daily"),
        (supply, ["location_id"], "court_supply_by_location"),
    ):
        if frame.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count():
            raise ValueError(f"Duplicate Gold key in {label}")
    return reference


def location_market_signal_snapshot(
    social: DataFrame,
    supply: DataFrame,
    *,
    window: AnalysisWindow,
    generated_at: str,
) -> DataFrame:
    reference = _canonical_inputs(social, supply)
    activity = (
        social.filter(
            F.col("activity_date").between(window.start_date, window.end_date)
        )
        .groupBy("location_id", "location_name")
        .agg(*(F.sum(name).alias("social_" + name) for name in SOCIAL_METRICS))
        .drop("location_name")
        .withColumn("has_social_signal", F.lit(True))
    )
    current_supply = supply.select("location_id", *SUPPLY_METRICS).withColumn(
        "has_supply_signal", F.lit(True)
    )
    joined = activity.join(current_supply, "location_id", "full_outer").join(
        reference, "location_id", "inner"
    )
    for name in SOCIAL_METRICS:
        column = "social_" + name
        joined = joined.withColumn(
            column, F.coalesce(F.col(column), F.lit(0)).cast("long")
        )
    for name in ("has_social_signal", "has_supply_signal"):
        joined = joined.withColumn(name, F.coalesce(F.col(name), F.lit(False)))
    for name, numerator in (
        ("court_count_coverage_ratio", "venues_with_known_court_count"),
        ("social_activity_per_venue", "social_post_count"),
        ("social_engagement_per_venue", "social_engagement_total"),
    ):
        joined = joined.withColumn(
            name,
            F.when(F.col("venue_count") > 0, F.col(numerator) / F.col("venue_count")),
        )
    return (
        joined.withColumnRenamed("_canonical_name", "location_name")
        .withColumn("schema_version", F.lit("1"))
        .withColumn("snapshot_date", F.lit(window.end_date))
        .withColumn("window_start_date", F.lit(window.start_date))
        .withColumn("window_end_date", F.lit(window.end_date))
        .withColumn("window_days", F.lit(window.days).cast("long"))
        .withColumn("gold_generated_at", F.lit(generated_at))
    )
