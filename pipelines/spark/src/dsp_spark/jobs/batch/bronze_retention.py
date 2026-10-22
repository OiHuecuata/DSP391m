"""Inspect source-specific expiry and deletion work without removing data.

A plan includes archives, derived datasets and external copies that must be
handled together. No guessed durations or destructive cloud migrations.
"""

import json
import os
from datetime import UTC, datetime

from delta.tables import DeltaTable
from dsp_collector.runtime.bronze import DATASETS, load_policies
from pyspark.sql import functions as F

from ...io.delta import read_delta
from ...processing.intelligence.catalog import GOLD, SILVER
from ...runtime.config import SparkSettings
from ...runtime.spark import create_spark


def retention_plan(spark, settings, *, now=None, policies=None):
    now = now or datetime.now(UTC)
    if now.utcoffset() is None:
        raise ValueError("retention cutoff must include timezone")
    policies = (
        policies
        if policies is not None
        else load_policies(os.getenv("DSP_SOURCE_POLICY_REGISTRY"))
    )
    tables, artifacts, runs = {}, set(), set()
    for dataset in DATASETS:
        path = getattr(settings.paths, dataset)
        if not DeltaTable.isDeltaTable(spark, path):
            tables[dataset] = {"status": "missing"}
            continue
        frame = read_delta(spark, path)
        expiry = F.col("retention_expires_at")
        for source, policy in policies.items():
            days = policy.get("retention_days")
            if policy.get("storage_permission") is False:
                expiry = F.when(
                    F.col("source_platform") == source, F.col("collected_at")
                ).otherwise(expiry)
            elif days is not None:
                if type(days) is not int or days <= 0:
                    raise ValueError("retention_days must be positive")
                expiry = F.when(
                    F.col("source_platform") == source,
                    F.col("collected_at") + F.expr(f"INTERVAL {days} DAYS"),
                ).otherwise(expiry)
        expired = frame.filter(expiry <= F.lit(now))
        tables[dataset] = {
            "expired_observations": expired.count(),
            "unreviewed_policy_observations": frame.filter(
                F.col("retention_policy_id").isNull()
            ).count(),
        }
        for row in (
            expired.select("source_platform", "collection_run_id", "artifact_uri")
            .distinct()
            .toLocalIterator()
        ):
            runs.add((row.source_platform, row.collection_run_id))
            if row.artifact_uri:
                artifacts.add(row.artifact_uri)
    path = settings.paths.bronze_source_changes
    explicit = []
    if DeltaTable.isDeltaTable(spark, path):
        for row in (
            read_delta(spark, path)
            .filter("change_type IN ('deleted', 'unavailable')")
            .toLocalIterator()
        ):
            policy = policies.get(row.source_platform, {})
            if policy.get("deletion_required"):
                explicit.append(
                    {
                        "source_platform": row.source_platform,
                        "source_record_id": row.source_record_id,
                        "change_type": row.change_type,
                        "evidence_json": row.evidence_json,
                    }
                )
    return {
        "tables": tables,
        "affected_runs": [
            {"source_platform": s, "collection_run_id": r} for s, r in sorted(runs)
        ],
        "artifact_uris": sorted(artifacts),
        "source_deletion_requests": explicit,
        "derived_dependencies": [
            "silver/social_posts",
            "silver/social_comments",
            "silver/courts",
            *SILVER,
            *GOLD,
            "gold",
            "ClickHouse",
        ],
        "external_dependencies": [
            "collector artifacts",
            "Kafka log retention",
            "Delta historical files and backups",
        ],
        "policy_registry_status": "configured" if policies else "unreviewed",
        "status": "plan_only",
    }


def main():
    settings = SparkSettings.from_env()
    spark = create_spark(settings)
    try:
        print(json.dumps(retention_plan(spark, settings), ensure_ascii=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
