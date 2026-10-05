"""Managed-runtime adapter calling the existing dsp_spark Gold entrypoint."""

import argparse
import os

from dsp_spark.jobs.batch.build_court_supply_by_location import main as court_gold_main
from dsp_spark.jobs.batch.build_social_activity_daily import main as gold_main
from dsp_spark.runtime.config import LakehousePaths, SparkSettings
from pyspark.sql import SparkSession


def run(argv, job, job_name):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lakehouse-base-uri", required=True)
    parser.add_argument("--social-posts-path", default="")
    parser.add_argument("--gold-path", default="")
    parser.add_argument("--courts-path", default="")
    parser.add_argument("--quality-path", default="")
    args = parser.parse_args(argv)
    if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
        raise RuntimeError("This adapter requires a managed Databricks runtime")
    supply = job_name == "court_supply_by_location"
    paths = LakehousePaths(
        args.lakehouse_base_uri,
        social_posts=args.social_posts_path,
        social_activity_daily=args.gold_path if not supply else "",
        courts=args.courts_path,
        court_supply_by_location=args.gold_path if supply else "",
        unresolved_court_locations=args.quality_path,
    )
    targets = (
        (
            paths.base,
            paths.courts,
            paths.court_supply_by_location,
            paths.unresolved_court_locations,
        )
        if supply
        else (paths.base, paths.social_posts, paths.social_activity_daily)
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
    job(spark=spark, settings=settings)


def main(argv=None):
    run(argv, gold_main, "social_activity_daily")


def court_supply_main(argv=None):
    run(argv, court_gold_main, "court_supply_by_location")


if __name__ == "__main__":
    main()
