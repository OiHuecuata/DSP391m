"""Replace the latest analytical snapshot from existing Gold evidence only."""

import json
from datetime import UTC, datetime

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from ...io.delta import read_delta, replace_delta
from ...processing.aggregate.location_market_signal_snapshot import (
    location_market_signal_snapshot,
    select_analysis_window,
)
from ...processing.clean.gold import validated_gold
from ...runtime.config import MarketSignalSettings, SparkSettings
from ...runtime.ownership import job_ownership
from ...runtime.spark import create_spark


def supply_quality_metadata(spark, paths, supply):
    resolved = supply.agg(F.sum("venue_count").alias("count")).first()["count"] or 0
    result = {
        "supply_total_logical_venues": None,
        "supply_resolved_venues": resolved,
        "supply_unresolved_venues": None,
        "supply_location_resolution_ratio": None,
        "supply_quality_available": False,
    }
    if not DeltaTable.isDeltaTable(spark, paths.unresolved_court_locations):
        result["supply_quality_note"] = (
            "Unresolved-location quality artifact unavailable"
        )
        return result
    quality = read_delta(spark, paths.unresolved_court_locations)
    # Gold supply and quality are two separately written artifacts. Reject
    # observable stale/mixed generations rather than reporting false coverage.
    supply_times = {
        r[0] for r in supply.select("gold_generated_at").distinct().collect()
    }
    quality_times = {
        r[0] for r in quality.select("quality_generated_at").distinct().collect()
    }
    if (
        len(supply_times) > 1
        or len(quality_times) > 1
        or (supply_times and quality_times and supply_times != quality_times)
    ):
        raise ValueError("Supply Gold and unresolved quality generations disagree")
    if quality.filter(F.col("location_id").isNotNull()).limit(1).count():
        raise ValueError("Unresolved supply quality contains a resolved location")
    if quality.groupBy("record_id").count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError("Unresolved supply quality contains duplicate logical venues")
    unresolved = quality.count()
    total = resolved + unresolved
    result.update(
        supply_total_logical_venues=total,
        supply_unresolved_venues=unresolved,
        supply_location_resolution_ratio=resolved / total if total else None,
        supply_quality_available=True,
    )
    return result


def build_location_market_signal_snapshot(
    spark,
    settings: SparkSettings,
    *,
    market_settings: MarketSignalSettings | None = None,
):
    selected = (
        market_settings
        if market_settings is not None
        else MarketSignalSettings.from_env()
    )
    social = read_delta(spark, settings.paths.social_activity_daily).persist()
    supply = read_delta(spark, settings.paths.court_supply_by_location).persist()
    gold = None
    try:
        social_input = validated_gold(social, "social_activity_daily")
        supply_input = validated_gold(supply, "court_supply_by_location")
        window = select_analysis_window(social_input, selected)
        # Preserve unresolved evidence in the existing daily Gold dataset and
        # explicitly report exclusions, including those outside this window.
        social_quality = (
            social_input.filter(F.col("location_id").isNull())
            .agg(
                F.count("*").alias("unresolved_social_location_rows"),
                F.count(
                    F.when(
                        F.col("activity_date").between(
                            window.start_date, window.end_date
                        ),
                        1,
                    )
                ).alias("unresolved_social_location_rows_in_window"),
            )
            .first()
            .asDict()
        )
        generated_at = (
            datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
        )
        gold = validated_gold(
            location_market_signal_snapshot(
                social_input, supply_input, window=window, generated_at=generated_at
            ),
            "location_market_signal_snapshot",
        ).persist()
        quality = supply_quality_metadata(spark, settings.paths, supply_input)
        counts = (
            gold.agg(
                F.count("location_id").alias("output_locations"),
                F.count(
                    F.when(F.col("has_social_signal") & F.col("has_supply_signal"), 1)
                ).alias("locations_with_both_signals"),
                F.count(
                    F.when(F.col("has_social_signal") & ~F.col("has_supply_signal"), 1)
                ).alias("social_only_locations"),
                F.count(
                    F.when(~F.col("has_social_signal") & F.col("has_supply_signal"), 1)
                ).alias("supply_only_locations"),
            )
            .first()
            .asDict()
        )
        replace_delta(gold, settings.paths.location_market_signal_snapshot)
        return {
            "status": "success",
            "snapshot_date": window.end_date,
            "window_start_date": window.start_date,
            "window_end_date": window.end_date,
            "window_days": window.days,
            **counts,
            **quality,
            **social_quality,
            "social_activity_source_path": settings.paths.social_activity_daily,
            "gold_generated_at": generated_at,
            "path": settings.paths.location_market_signal_snapshot,
        }
    finally:
        if gold is not None:
            gold.unpersist()
        supply.unpersist()
        social.unpersist()


def main(argv=None, *, spark=None, settings=None, market_settings=None, ownership=None):
    selected = settings or SparkSettings.from_env()
    market = (
        market_settings
        if market_settings is not None
        else MarketSignalSettings.from_env()
    )
    owned = spark is None
    with job_ownership(
        selected, outputs=("location_market_signal_snapshot",), ownership=ownership
    ):
        spark = create_spark(selected) if owned else spark
        try:
            print(
                json.dumps(
                    build_location_market_signal_snapshot(
                        spark, selected, market_settings=market
                    ),
                    ensure_ascii=False,
                )
            )
        finally:
            if owned:
                spark.stop()


if __name__ == "__main__":
    main()
