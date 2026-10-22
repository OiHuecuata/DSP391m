"""Managed-runtime adapter calling the existing dsp_spark Gold entrypoint."""

import argparse
import os

from dsp_spark.jobs.batch.build_court_supply_by_location import main as court_gold_main
from dsp_spark.jobs.batch.build_intelligence import main as intelligence_job_main
from dsp_spark.jobs.batch.build_location_market_signal_snapshot import (
    main as market_gold_main,
)
from dsp_spark.jobs.batch.build_social_activity_daily import main as gold_main
from dsp_spark.runtime.config import LakehousePaths, MarketSignalSettings, SparkSettings
from pyspark.sql import SparkSession


def run(argv, job, job_name):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lakehouse-base-uri", required=True)
    parser.add_argument("--social-posts-path", default="")
    parser.add_argument("--gold-path", default="")
    parser.add_argument("--courts-path", default="")
    parser.add_argument("--quality-path", default="")
    parser.add_argument("--social-activity-path", default="")
    parser.add_argument("--court-supply-path", default="")
    parser.add_argument("--window-days", default="30")
    parser.add_argument("--end-date", default=None)
    args = parser.parse_args(argv)
    if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
        raise RuntimeError("This adapter requires a managed Databricks runtime")
    supply = job_name == "court_supply_by_location"
    market = job_name == "location_market_signal_snapshot"
    paths = LakehousePaths(
        args.lakehouse_base_uri,
        social_posts=args.social_posts_path,
        social_activity_daily=(
            args.social_activity_path
            if market
            else (args.gold_path if not supply else "")
        ),
        courts=args.courts_path,
        court_supply_by_location=(
            args.court_supply_path if market else (args.gold_path if supply else "")
        ),
        unresolved_court_locations=args.quality_path,
        location_market_signal_snapshot=args.gold_path if market else "",
    )
    targets = (
        (
            paths.base,
            paths.social_activity_daily,
            paths.court_supply_by_location,
            paths.unresolved_court_locations,
            paths.location_market_signal_snapshot,
        )
        if market
        else (
            (
                paths.base,
                paths.courts,
                paths.court_supply_by_location,
                paths.unresolved_court_locations,
            )
            if supply
            else (paths.base, paths.social_posts, paths.social_activity_daily)
        )
    )
    if job_name == "silver_gold_intelligence":
        from dsp_spark.processing.intelligence.catalog import GOLD, SILVER

        targets = (
            paths.base,
            *(
                getattr(paths, n)
                for n in (
                    *SILVER,
                    *GOLD,
                    "bronze_court_observations",
                    "bronze_social_posts",
                    "bronze_social_comments",
                    "bronze_engagement_snapshots",
                    "bronze_collection_runs",
                    "bronze_source_changes",
                    "intelligence_manifest",
                )
            ),
        )
    if not all(path.startswith("s3a://") for path in targets):
        raise ValueError("Databricks Gold requires explicitly configured S3A paths")
    settings = SparkSettings(paths)
    # The platform supplies Spark/Delta and S3 access. Do not install the local
    # PySpark/Delta distributions or override the platform's master/credentials.
    spark = SparkSession.builder.getOrCreate()
    if not spark.version.startswith("4.0."):
        raise RuntimeError(
            "Select an existing cluster with Spark 4.0.x and Python 3.12"
        )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if market:
        if not args.window_days.isdecimal():
            raise ValueError("DSP_MARKET_SIGNAL_WINDOW_DAYS must be a positive integer")
        job(
            spark=spark,
            settings=settings,
            market_settings=MarketSignalSettings(int(args.window_days), args.end_date),
        )
    elif job_name == "silver_gold_intelligence":
        # The wrapper has already consumed its own arguments. Do not pass the
        # wheel's --lakehouse-base-uri back into the inner batch CLI parser.
        job([], spark=spark, settings=settings)
    else:
        job(spark=spark, settings=settings)


def main(argv=None):
    run(argv, gold_main, "social_activity_daily")


def court_supply_main(argv=None):
    run(argv, court_gold_main, "court_supply_by_location")


def market_signal_main(argv=None):
    run(argv, market_gold_main, "location_market_signal_snapshot")


def intelligence_main(argv=None):
    run(argv, intelligence_job_main, "silver_gold_intelligence")


if __name__ == "__main__":
    main()
