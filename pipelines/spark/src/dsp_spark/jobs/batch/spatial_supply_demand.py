"""Pinned Spark spatial counts, separate from semantic model training.

KDE/Moran/LISA/Gi*/Spearman are intentionally not reported as trained models.
"""

import argparse
import json
import os
import sys
from pathlib import Path

from pyspark.sql import functions as F

from ...io.delta import merge_delta
from ...io.s3 import storage_uri
from ...ml.config import fingerprint, staging_uri
from ...ml.data import Snapshot, load_posts, require_schema
from ...ml.extensions import spatial_aggregations
from ...processing.intelligence.catalog import contract
from ...runtime.config import LakehousePaths, SparkSettings
from ...runtime.spark import create_spark
from .demand_clustering import cloud_write_guard


def main(argv=None):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--inputs",
        required=True,
        type=Path,
        help="JSON with post_snapshot, venues and mentions paths/versions",
    )
    parser.add_argument("--admin-column", default="old_province_id")
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--allow-cloud-write", action="store_true")
    parser.add_argument("--profile")
    parser.add_argument("--region")
    parser.add_argument("--report", required=True, type=Path)
    args = parser.parse_args(argv)
    inputs = json.loads(args.inputs.read_text())
    snapshot = Snapshot(**inputs["post_snapshot"])
    output = staging_uri(args.output_root) + "/gold/spatial_supply_demand_counts_v1"
    paths = [
        snapshot.path,
        storage_uri(inputs["venues"]["path"]),
        storage_uri(inputs["mentions"]["path"]),
    ]
    for path in paths:
        if (
            output == path
            or output.startswith(path.rstrip("/") + "/")
            or path.startswith(staging_uri(args.output_root) + "/")
        ):
            raise ValueError("spatial write root must be disjoint from Silver inputs")
    if args.execute:
        cloud_write_guard(
            [output],
            allow=args.allow_cloud_write,
            profile=args.profile,
            region=args.region,
        )
    base = next((p for p in [*paths, output] if p.startswith("s3a://")), snapshot.path)
    settings = SparkSettings(
        LakehousePaths(base), s3_profile=args.profile, s3_region=args.region
    )
    spark = create_spark(settings)
    try:
        posts, audit = load_posts(spark, snapshot)
        frames = {}
        for name in ("venues", "mentions"):
            version = inputs[name]["version"]
            if not isinstance(version, int) or version < 0:
                raise ValueError("spatial inputs require pinned Delta versions")
            frames[name] = (
                spark.read.format("delta")
                .option("versionAsOf", version)
                .load(inputs[name]["path"])
            )
            require_schema(frames[name], {"schema_version": "string"})
            kind = (
                "silver_court_venues"
                if name == "venues"
                else "silver_social_geo_mentions"
            )
            expected = contract(kind)["properties"]["schema_version"]["const"]
            if (
                frames[name]
                .filter(~F.col("schema_version").eqNullSafe(expected))
                .limit(1)
                .count()
            ):
                raise ValueError("incompatible spatial Silver schema_version")
        result = spatial_aggregations(
            frames["venues"], frames["mentions"], posts, args.admin_column
        )
        report = {
            "status": "DRY_RUN",
            "inputs": inputs,
            "posts": audit,
            "admin_column": args.admin_column,
            "geographies": result.count(),
            "venues_without_admin": frames["venues"]
            .filter(F.col(args.admin_column).isNull())
            .count(),
            "mentions_without_admin": frames["mentions"]
            .filter(F.col(args.admin_column).isNull())
            .count(),
            "statistics": "NOT_RUN: KDE/Moran/LISA/Gi*/Spearman require a separately reviewed spatial protocol",
        }
        if args.execute:
            result = (
                result.withColumnRenamed(args.admin_column, "admin_unit_id")
                .withColumn("snapshot_id", F.lit(fingerprint(inputs)))
                .withColumn("admin_geography", F.lit(args.admin_column))
            )
            merge_delta(
                result,
                output,
                keys=("admin_unit_id", "snapshot_id", "admin_geography"),
                initialize_empty=True,
            )
            report.update(status="TESTED", output_path=output)
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({"report": str(args.report), "status": report["status"]}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
