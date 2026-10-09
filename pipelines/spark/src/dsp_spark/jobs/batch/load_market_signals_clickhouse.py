"""Read canonical Delta Gold, validate, and publish its current serving state."""

import argparse
import json

from ...io.clickhouse import ClickHouseClient, migrate, read_gold_rows, replace_serving
from ...runtime.clickhouse import ClickHouseSettings
from ...runtime.config import SparkSettings
from ...runtime.ownership import job_ownership
from ...runtime.spark import create_spark


def main(argv=None, *, settings=None, connection=None, ownership=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--migrate-only", action="store_true")
    args = parser.parse_args(argv)
    connection = connection or ClickHouseSettings.from_env()
    with (
        job_ownership(warehouse=connection, ownership=ownership),
        ClickHouseClient(connection) as client,
    ):
        if args.migrate_only:
            migrate(client)
            print(json.dumps({"status": "success", "migration": "001"}))
            return
        settings = settings or SparkSettings.from_env()
        spark = create_spark(settings)
        try:
            rows = read_gold_rows(spark, settings.paths.location_market_signal_snapshot)
            print(json.dumps(replace_serving(client, rows)))
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
