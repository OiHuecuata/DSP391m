"""Serve P0/P1 tables only after a complete validated Delta generation."""

import json

from ...io.clickhouse import ClickHouseClient
from ...io.intelligence_clickhouse import publish_intelligence
from ...runtime.clickhouse import ClickHouseSettings
from ...runtime.config import SparkSettings
from ...runtime.spark import create_spark


def main():
    settings = SparkSettings.from_env()
    spark = create_spark(settings)
    try:
        with ClickHouseClient(ClickHouseSettings.from_env()) as client:
            print(json.dumps(publish_intelligence(spark, settings, client)))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
