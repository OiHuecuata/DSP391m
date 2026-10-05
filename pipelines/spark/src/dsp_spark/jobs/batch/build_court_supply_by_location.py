"""Rebuild source-specific Gold supply and unresolved-address quality tables."""

import json
from datetime import UTC, datetime

from pyspark.sql import functions as F

from ...io.delta import read_delta, replace_delta
from ...processing.aggregate.court_supply_by_location import (
    court_supply_by_location,
    resolved_courts,
)
from ...processing.clean.gold import validated_gold
from ...runtime.config import SparkSettings
from ...runtime.spark import create_spark


def build_court_supply_by_location(spark, settings: SparkSettings):
    silver = read_delta(spark, settings.paths.courts).persist()
    resolved = resolved_courts(silver).persist()
    gold = None
    try:
        generated_at = (
            datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
        )
        gold = validated_gold(
            court_supply_by_location(resolved, generated_at=generated_at),
            "court_supply_by_location",
        ).persist()
        unresolved = resolved.filter(F.col("location_id").isNull())
        logical = resolved.count()
        missing = unresolved.count()
        reason_counts = {
            r.location_resolution: r["count"]
            for r in unresolved.groupBy("location_resolution").count().collect()
        }
        gold_rows = gold.count()
        replace_delta(gold, settings.paths.court_supply_by_location)
        replace_delta(
            unresolved.withColumn("quality_generated_at", F.lit(generated_at)),
            settings.paths.unresolved_court_locations,
        )
        return {
            "status": "success",
            "silver_observations": silver.count(),
            "logical_venues": logical,
            "resolved_venues": logical - missing,
            "unresolved_venues": missing,
            "resolution_percentage": (
                100 * (logical - missing) / logical if logical else None
            ),
            "unresolved_reasons": reason_counts,
            "gold_rows": gold_rows,
            "gold_generated_at": generated_at,
            "path": settings.paths.court_supply_by_location,
            "quality_path": settings.paths.unresolved_court_locations,
        }
    finally:
        if gold is not None:
            gold.unpersist()
        resolved.unpersist()
        silver.unpersist()


def main(argv=None, *, spark=None, settings=None):
    selected = settings or SparkSettings.from_env()
    owned = spark is None
    spark = create_spark(selected) if owned else spark
    try:
        print(
            json.dumps(
                build_court_supply_by_location(spark, selected), ensure_ascii=False
            )
        )
    finally:
        if owned:
            spark.stop()


if __name__ == "__main__":
    main()
