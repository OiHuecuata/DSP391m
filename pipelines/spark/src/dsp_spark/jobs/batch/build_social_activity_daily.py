"""Rebuild Delta Gold activity from the current Silver observation snapshot."""

import json
from datetime import UTC, datetime

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ...io.delta import read_delta, replace_delta
from ...processing.aggregate.social_activity_daily import (
    eligible_location_posts,
    latest_social_posts,
    social_activity_daily,
    social_source_activity_daily,
)
from ...processing.clean.contracts import spark_schema
from ...processing.clean.gold import validated_gold
from ...processing.clean.source_lifecycle import permitted_legacy_frame
from ...runtime.config import SparkSettings
from ...runtime.spark import create_spark


def build_social_activity_daily(spark, settings: SparkSettings):
    silver = permitted_legacy_frame(
        spark, settings, read_delta(spark, settings.paths.social_posts)
    ).persist()
    gold = None
    try:
        generated_at = (
            datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
        )
        gold = social_activity_daily(silver, generated_at=generated_at)
        gold = validated_gold(gold, "social_activity_daily").persist()
        source_silver = silver
        if DeltaTable.isDeltaTable(spark, settings.paths.social_comments):
            source_silver = source_silver.unionByName(
                permitted_legacy_frame(
                    spark, settings, read_delta(spark, settings.paths.social_comments)
                )
            ).dropDuplicates(["event_id"])
        if "view_count" not in source_silver.columns:
            source_silver = source_silver.withColumn(
                "view_count", F.lit(None).cast("long")
            )
        source_gold = validated_gold(
            social_source_activity_daily(source_silver, generated_at=generated_at),
            "social_source_activity_daily",
        )
        replace_delta(source_gold, settings.paths.social_source_activity_daily)
        latest = latest_social_posts(silver.filter(F.col("source") == "threads"))
        logical_posts = (
            eligible_location_posts(latest).select("record_id").distinct().count()
        )
        gold_rows = gold.count()
        legacy_schema = T.StructType(
            [
                f
                for f in spark_schema("social_activity_daily")
                if f.name != "location_id"
            ]
        )
        replace_delta(
            gold, settings.paths.social_activity_daily, previous_schema=legacy_schema
        )
        return {
            "status": "success",
            "silver_observations": silver.count(),
            "logical_posts_used": logical_posts,
            "gold_rows": gold_rows,
            "unmapped_location_mentions": eligible_location_posts(latest)
            .filter(F.col("location_id").isNull())
            .count(),
            "gold_generated_at": generated_at,
            "path": settings.paths.social_activity_daily,
        }
    finally:
        if gold is not None:
            gold.unpersist()
        silver.unpersist()


def main(argv=None, *, spark=None, settings=None):
    # A supplied managed session runs the very same job without local JVM/JAR
    # setup or stopping a platform-owned SparkSession.
    selected = settings or SparkSettings.from_env()
    owned = spark is None
    spark = create_spark(selected) if owned else spark
    try:
        print(
            json.dumps(build_social_activity_daily(spark, selected), ensure_ascii=False)
        )
    finally:
        if owned:
            spark.stop()


if __name__ == "__main__":
    main()
