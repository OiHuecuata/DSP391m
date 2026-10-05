"""Inspect existing Delta tables without changing their contents."""

import argparse

from ...io.delta import read_delta
from ...runtime.config import SparkSettings
from ...runtime.spark import create_spark


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--table",
        choices=(
            "social_posts",
            "courts",
            "threads_bronze",
            "pbplus_bronze",
            "rejected_social",
            "social_activity_daily",
            "court_supply_by_location",
            "unresolved_court_locations",
        ),
        required=True,
    )
    args = parser.parse_args(argv)
    settings = SparkSettings.from_env()
    spark = create_spark(settings)
    try:
        frame = read_delta(spark, getattr(settings.paths, args.table))
        frame.printSchema()
        print("rows:", frame.count())
        frame.show(20, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
