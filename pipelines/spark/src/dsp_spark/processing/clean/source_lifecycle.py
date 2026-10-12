"""Apply Bronze deletion/expiry evidence to legacy Silver consumers.

No source absence is interpreted as deletion. This projection preserves v1
columns and semantics while preventing deleted/expired evidence reaching Gold.
"""

import json
import os
from datetime import UTC, datetime

from delta.tables import DeltaTable
from dsp_collector.runtime.bronze import load_policies
from pyspark.sql import functions as F

from ...io.delta import read_delta


def permitted_legacy_frame(spark, settings, frame, *, now=None):
    now = now or datetime.now(UTC)
    policies = load_policies(os.getenv("DSP_SOURCE_POLICY_REGISTRY"))
    result = frame.withColumn(
        "_policy_source", F.regexp_replace("source", "_comment$", "")
    )
    for source, policy in policies.items():
        eligible = F.col("_policy_source") == source
        if policy.get("storage_permission") is False:
            result = result.filter(~eligible)
        elif policy.get("retention_days"):
            result = result.filter(
                ~eligible
                | (
                    F.to_timestamp("collected_at")
                    + F.expr(f"INTERVAL {policy['retention_days']} DAYS")
                    > F.lit(now)
                )
            )
    excluded = json.loads(os.getenv("DSP_INTELLIGENCE_EXCLUDED_RUNS", "[]"))
    if excluded:
        result = result.filter(~F.col("run_id").isin(excluded))
    path = settings.paths.bronze_source_changes
    if DeltaTable.isDeltaTable(spark, path):
        changes = read_delta(spark, path).filter(
            (F.col("collected_at") <= F.lit(now))
            & F.col("change_type").isin("deleted", "unavailable")
        )
        blocked = []
        for change in changes.toLocalIterator():
            evidence = json.loads(change.evidence_json)
            verified = evidence.get("source_url") or evidence.get("verified_endpoint")
            deleted = change.change_type == "deleted" and (
                evidence.get("http_status") == 410
                or evidence.get("source_tombstone") is True
            )
            unavailable = change.change_type == "unavailable" and policies.get(
                change.source_platform, {}
            ).get("deletion_required")
            if verified and (deleted or unavailable):
                blocked.append((change.source_platform, change.source_record_id))
        if blocked:
            block_frame = spark.createDataFrame(
                sorted(set(blocked)), "_blocked_source string, _blocked_id string"
            )
            result = result.join(
                block_frame,
                (F.col("_policy_source") == F.col("_blocked_source"))
                & (F.col("source_record_id") == F.col("_blocked_id")),
                "left_anti",
            )
            if "parent_record_id" in result.columns:
                parents = spark.createDataFrame(
                    [(s + ":" + sid,) for s, sid in sorted(set(blocked))],
                    "_blocked_parent string",
                )
                result = result.join(
                    parents,
                    F.col("parent_record_id") == F.col("_blocked_parent"),
                    "left_anti",
                )
    # Explicit per-observation Bronze retention survives even without a newly
    # configured registry. Do not apply a source-wide inferred expiration.
    for name in (
        "bronze_court_observations",
        "bronze_social_posts",
        "bronze_social_comments",
    ):
        path = getattr(settings.paths, name)
        if not DeltaTable.isDeltaTable(spark, path):
            continue
        expired = (
            read_delta(spark, path)
            .filter(F.col("retention_expires_at") <= F.lit(now))
            .select(
                F.col("source_platform").alias("_expired_source"),
                F.col("source_record_id").alias("_expired_id"),
                F.col("collection_run_id").alias("_expired_run"),
            )
            .distinct()
        )
        result = result.join(
            expired,
            (F.col("_policy_source") == F.col("_expired_source"))
            & (F.col("source_record_id") == F.col("_expired_id"))
            & (F.col("run_id") == F.col("_expired_run")),
            "left_anti",
        )
    return result.drop("_policy_source")
