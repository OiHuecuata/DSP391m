"""Silver-only readiness audit and exact pinned-snapshot comparison; no model fit."""

import argparse
import csv
import hashlib
import json
import os
import sys
from dataclasses import replace
from pathlib import Path

from ...io.s3 import storage_uri
from ...ml.provincial.audit import compare_sources, readiness_audit
from ...ml.provincial.config import TABLES, ForecastConfig
from ...ml.provincial.data import normalized_inputs, read_sources
from ...runtime.config import LakehousePaths, SparkSettings
from ...runtime.spark import create_spark


def main(argv=None):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--compare-inputs", type=Path, action="append", default=[])
    parser.add_argument("--export-manifest", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--profile")
    parser.add_argument("--region")
    args = parser.parse_args(argv)
    inputs = normalized_inputs(json.loads(args.inputs.read_text()))
    comparisons = [
        normalized_inputs(json.loads(p.read_text())) for p in args.compare_inputs
    ]
    destination = args.output_dir.resolve().as_uri()
    paths = [
        t["path"]
        for descriptor in [inputs, *comparisons]
        for t in descriptor["tables"].values()
    ]
    if any(
        destination == p
        or destination.startswith(p + "/")
        or p.startswith(destination + "/")
        for p in paths
    ):
        raise ValueError("audit output must be disjoint from source tables")
    base = next((p for p in paths if p.startswith("s3a://")), paths[0])
    settings = SparkSettings.from_env()
    settings = replace(
        settings,
        paths=LakehousePaths(base),
        s3_profile=args.profile or settings.s3_profile,
        s3_region=args.region or settings.s3_region,
    )
    args.output_dir.mkdir(parents=True, exist_ok=False)
    spark = create_spark(settings)
    try:
        config = ForecastConfig(**json.loads(args.config.read_text()))
        provinces, stages, report = readiness_audit(spark, inputs, config)
        provinces.write.format("delta").mode("errorifexists").save(
            storage_uri(str(args.output_dir / "province_eligibility_audit"))
        )
        stages.write.format("delta").mode("errorifexists").save(
            storage_uri(str(args.output_dir / "filter_join_audit"))
        )
        rows = report["eligibility"]
        with (args.output_dir / "province_eligibility_audit.csv").open(
            "w", newline=""
        ) as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        report["comparisons"] = {}
        left, _ = read_sources(spark, inputs)
        for descriptor in comparisons:
            try:
                right, _ = read_sources(spark, descriptor)
                report["comparisons"][descriptor["dataset_id"]] = {
                    "status": "TESTED",
                    "tables": compare_sources(left, right),
                    "inputs": descriptor,
                }
            except Exception as exc:  # noqa: BLE001
                report["comparisons"][descriptor["dataset_id"]] = {
                    "status": "BLOCKED",
                    "error_type": type(exc).__name__,
                    "inputs": descriptor,
                }
        if args.export_manifest:
            manifest = json.loads(args.export_manifest.read_text())
            if (
                manifest.get("export_type") != "FULL_UNFILTERED_SILVER"
                or manifest.get("source_modified") is not False
            ):
                raise ValueError(
                    "only an explicitly identified unmodified full Silver export can be audited"
                )
            exported = {}
            checksums = {}
            export_root = args.export_manifest.parent.resolve()
            for name in TABLES:
                item = manifest["tables"][name.removeprefix("silver_")]
                path = (export_root / item["file"]).resolve()
                if not path.is_relative_to(export_root):
                    raise ValueError("export file outside manifest directory")
                digest = hashlib.sha256()
                with path.open("rb") as stream:
                    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(chunk)
                checksums[name] = digest.hexdigest() == item["sha256"]
                if not checksums[name]:
                    raise ValueError("Silver export checksum mismatch")
                exported[name] = spark.read.parquet(str(path))
            report["export_comparison"] = {
                "status": "TESTED",
                "dataset_id": manifest["dataset_id"],
                "checksums_match": checksums,
                "tables": compare_sources(left, exported),
                "scope": "comparison only; not restored or used as model training fallback",
            }
        # Source schemas are available from Delta; keep this report aggregate-only.
        for item in report["lineage"]["tables"].values():
            item.pop("schema", None)
        (args.output_dir / "report.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
        )
        print(
            json.dumps(
                {
                    "status": report["status"],
                    "eligible_provinces": len(report["eligible_provinces"]),
                    "report": str(args.output_dir / "report.json"),
                }
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
