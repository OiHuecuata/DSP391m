"""Read-only audit/dry-run by default; explicit staging candidate execution."""

import argparse
import json
import os
import sys
from dataclasses import replace
from pathlib import Path

from ...ml.config import TrainingConfig, staging_uri
from ...ml.data import Snapshot, audit_table, load_posts
from ...ml.pipeline import infer, train
from ...ml.publish import publish, rollback
from ...ml.registry import Registry
from ...runtime.config import LakehousePaths, SparkSettings
from ...runtime.spark import create_spark


def cloud_write_guard(paths, *, allow, profile, region):
    if not any(p.startswith("s3a://") for p in paths):
        return
    if not allow:
        raise ValueError(
            "staging cloud writes require --allow-cloud-write after operator authorization"
        )
    import boto3

    identity = (
        boto3.Session(profile_name=profile, region_name=region)
        .client("sts")
        .get_caller_identity()
    )
    if identity["Arn"].endswith(":root"):
        raise ValueError("ML cloud writes require a non-Root runtime identity")


def main(argv=None):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile")
    parser.add_argument("--region")
    parser.add_argument("--master")
    commands = parser.add_subparsers(dest="command", required=True)
    audit = commands.add_parser("audit")
    audit.add_argument("--lakehouse-base", required=True)
    audit.add_argument("--report", type=Path, required=True)
    for name in ("train", "infer"):
        cmd = commands.add_parser(name)
        cmd.add_argument("--snapshot", type=Path, required=True)
        cmd.add_argument("--registry", required=True)
        cmd.add_argument("--cache-root", required=True)
        cmd.add_argument("--execute", action="store_true")
        cmd.add_argument("--allow-cloud-write", action="store_true")
        cmd.add_argument("--report", type=Path, required=True)
        if name == "train":
            cmd.add_argument("--config", type=Path, required=True)
            cmd.add_argument(
                "--baseline-assignments",
                help="versioned Colab content_id/cluster_id Parquet export",
            )
            cmd.add_argument("--lexical-ablation", action="store_true")
        else:
            cmd.add_argument("--run-id", required=True)
            cmd.add_argument("--k", type=int, required=True)
            cmd.add_argument(
                "--after",
                help="exclusive last_seen_at ISO timestamp; operator advances only after successful publish",
            )
            cmd.add_argument("--publish", action="store_true")
            cmd.add_argument("--output-root")
    restore = commands.add_parser("rollback-gold")
    restore.add_argument("--output-root", required=True)
    restore.add_argument("--version", type=int, required=True)
    restore.add_argument("--execute", action="store_true")
    restore.add_argument("--allow-cloud-write", action="store_true")
    args = parser.parse_args(argv)
    settings = SparkSettings.from_env()
    settings = replace(
        settings,
        master=args.master or settings.master,
        s3_profile=args.profile or settings.s3_profile,
        s3_region=args.region or settings.s3_region,
    )
    if args.command in ("train", "infer"):
        snapshot = Snapshot(**json.loads(args.snapshot.read_text()))
        registry = Registry(
            args.registry, profile=settings.s3_profile, region=settings.s3_region
        )
        cache = staging_uri(args.cache_root) + "/embedding_cache_v1"
        write_paths = [registry.root, cache]
        if args.command == "infer" and args.publish:
            if not args.output_root:
                raise ValueError("--publish requires an explicit --output-root")
            write_paths.append(staging_uri(args.output_root))
        if any(
            p == snapshot.path
            or p.startswith(snapshot.path + "/")
            or snapshot.path.startswith(p + "/")
            for p in write_paths
        ):
            raise ValueError(
                "ML write roots must be disjoint from the source Silver table"
            )
        if args.execute:
            cloud_write_guard(
                write_paths,
                allow=args.allow_cloud_write,
                profile=settings.s3_profile,
                region=settings.s3_region,
            )
        base = next(
            (p for p in [snapshot.path, *write_paths] if p.startswith("s3a://")),
            snapshot.path,
        )
    elif args.command == "audit":
        base = args.lakehouse_base
    else:
        base = staging_uri(args.output_root)
        if not args.execute:
            print(
                json.dumps(
                    {"status": "DRY_RUN", "restore_version": args.version, "root": base}
                )
            )
            return
        cloud_write_guard(
            [base],
            allow=args.allow_cloud_write,
            profile=settings.s3_profile,
            region=settings.s3_region,
        )
    settings = replace(settings, paths=LakehousePaths(base))
    spark = create_spark(settings)
    try:
        if args.command == "audit":
            report = {
                "read_only": True,
                "snapshot_consistency": "independently pinned tables; use a generation manifest for cross-table consistency",
                "tables": {},
            }
            for name, keys in (
                ("silver_social_posts", ("post_id",)),
                ("silver_social_intent_evidence", ("evidence_id",)),
                ("silver_social_geo_mentions", ("mention_id",)),
                ("silver_court_venues", ("venue_id",)),
            ):
                try:
                    report["tables"][name] = {
                        "status": "TESTED",
                        **audit_table(spark, getattr(settings.paths, name), keys),
                    }
                except Exception as exc:  # noqa: BLE001 — safe per-table audit failure
                    report["tables"][name] = {
                        "status": "BLOCKED",
                        "error_type": type(exc).__name__,
                    }
        elif args.command == "rollback-gold":
            rollback(spark, base, args.version)
            print(json.dumps({"status": "RESTORED", "version": args.version}))
            return
        elif not args.execute:
            _, report = load_posts(spark, snapshot, after=getattr(args, "after", None))
            report["status"] = "DRY_RUN"
            if args.command == "train":
                report["config"] = json.loads(args.config.read_text())
                TrainingConfig.from_dict(report["config"])
        elif args.command == "train":
            config = TrainingConfig.from_dict(json.loads(args.config.read_text()))
            spark.conf.set("spark.sql.shuffle.partitions", config.partitions)
            baseline = (
                spark.read.parquet(args.baseline_assignments)
                if args.baseline_assignments
                else None
            )
            report = train(
                spark,
                snapshot,
                config,
                registry,
                cache,
                baseline=baseline,
                lexical_ablation=args.lexical_ablation,
            )
        else:
            assignments, report = infer(
                spark, snapshot, registry, args.run_id, args.k, cache, after=args.after
            )
            report["assignment_rows"] = assignments.count()
            report["status"] = "TESTED"
            report["gold"] = (
                publish(assignments, args.output_root)
                if args.publish
                else {"status": "NOT_RUN", "reason": "--publish not selected"}
            )
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
        print(
            json.dumps(
                {
                    "report": str(args.report),
                    "status": report.get("status", "AUDITED"),
                    "run_id": report.get("run_id"),
                }
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
