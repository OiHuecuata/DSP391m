"""Silver provincial Ridge: train/evaluate/predict with explicit staging execution."""

import argparse
import json
import os
import sys
from dataclasses import replace
from pathlib import Path

from pyspark.sql import functions as F

from ...ml.config import staging_uri
from ...ml.provincial.config import ForecastConfig, InsufficientHistory
from ...ml.provincial.data import normalized_inputs
from ...ml.provincial.pipeline import forecast, publish_forecast, run_benchmark
from ...ml.registry import Registry
from ...runtime.config import LakehousePaths, SparkSettings
from ...runtime.spark import create_spark
from .demand_clustering import cloud_write_guard


def main(argv=None):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile")
    parser.add_argument("--region")
    parser.add_argument("--master")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("train", "evaluate", "predict"):
        cmd = commands.add_parser(name)
        cmd.add_argument("--inputs", type=Path, required=True)
        cmd.add_argument("--registry", required=True)
        cmd.add_argument("--report", type=Path, required=True)
        cmd.add_argument("--execute", action="store_true")
        cmd.add_argument("--allow-cloud-write", action="store_true")
        if name != "predict":
            cmd.add_argument("--config", type=Path, required=True)
            cmd.add_argument("--run-id")
        else:
            cmd.add_argument("--run-id", required=True)
            cmd.add_argument("--as-of-date")
            cmd.add_argument("--observed-through")
            cmd.add_argument("--publish", action="store_true")
            cmd.add_argument("--output-root")
    args = parser.parse_args(argv)
    inputs = normalized_inputs(json.loads(args.inputs.read_text()))
    settings = SparkSettings.from_env()
    settings = replace(
        settings,
        master=args.master or settings.master,
        s3_profile=args.profile or settings.s3_profile,
        s3_region=args.region or settings.s3_region,
    )
    store = Registry(
        args.registry, profile=settings.s3_profile, region=settings.s3_region
    )
    destinations = [store.root]
    if args.command == "predict" and args.publish:
        if not args.output_root:
            raise ValueError("--publish requires --output-root")
        destinations.append(staging_uri(args.output_root))
    for source in inputs["tables"].values():
        for destination in destinations:
            if (
                destination == source["path"]
                or destination.startswith(source["path"] + "/")
                or source["path"].startswith(destination + "/")
            ):
                raise ValueError(
                    "forecast write roots must be disjoint from Silver source tables"
                )
    if args.execute:
        cloud_write_guard(
            destinations,
            allow=args.allow_cloud_write,
            profile=settings.s3_profile,
            region=settings.s3_region,
        )
    paths = [i["path"] for i in inputs["tables"].values()] + destinations
    base = next((p for p in paths if p.startswith("s3a://")), paths[0])
    settings = replace(settings, paths=LakehousePaths(base))
    spark = create_spark(settings)
    try:
        try:
            if args.command in ("train", "evaluate"):
                config = ForecastConfig(**json.loads(args.config.read_text()))
                report = run_benchmark(
                    spark,
                    inputs,
                    config,
                    store,
                    final_refit=args.command == "train",
                    run_id=args.run_id,
                    execute=args.execute,
                )
            elif not args.execute:
                report = {
                    "status": "DRY_RUN",
                    "inputs": inputs,
                    "run_id": args.run_id,
                    "operation": "predict",
                    "publish": bool(args.publish),
                }
            else:
                output, report = forecast(
                    spark,
                    inputs,
                    store,
                    args.run_id,
                    as_of_date=args.as_of_date,
                    observed_through=args.observed_through,
                )
                report["gold_output"] = (
                    publish_forecast(output, args.output_root) if args.publish else None
                )
                report["forecasts"] = [
                    json.loads(r[0])
                    for r in output.orderBy("province_id")
                    .limit(257)
                    .select(F.to_json(F.struct(*output.columns), {"timeZone": "UTC"}))
                    .collect()
                ]
        except InsufficientHistory as exc:
            report = {
                "status": "BLOCKED",
                "error_type": type(exc).__name__,
                "reason": str(exc),
                "inputs": inputs,
            }
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(
                report,
                indent=2,
                allow_nan=False,
                default=lambda value: value.isoformat(),
            )
            + "\n"
        )
        print(
            json.dumps(
                {
                    "status": report["status"],
                    "report": str(args.report),
                    "run_id": report.get("run_id"),
                }
            )
        )
        if report["status"] == "BLOCKED":
            raise SystemExit(2)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
