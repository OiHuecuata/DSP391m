"""Read-only data-readiness diagnostics; forecasting rules remain unchanged."""

import json
from datetime import timedelta
from pathlib import Path
from urllib.parse import unquote, urlsplit

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from .config import DIRECT_INTENTS, TABLES, fold_plan
from .data import (
    INVALID_GEO,
    eligibility,
    make_panel,
    published_dates,
    read_sources,
    weekly_counts,
)


def local_file_inventory(item):
    """Check referenced local data objects when complete JSON commit history is available."""
    uri = urlsplit(item["path"])
    if uri.scheme != "file":
        return {
            "status": "NOT_APPLICABLE_LOCAL_CHECK",
            "reason": "remote snapshots require authenticated object checks",
        }
    root = Path(unquote(uri.path))
    active = {}
    if item["version"] > 10000:
        return {
            "status": "NOT_VERIFIED",
            "reason": "bounded JSON-history inventory limit exceeded",
        }
    for version in range(item["version"] + 1):
        commit = root / "_delta_log" / f"{version:020}.json"
        if not commit.is_file():
            return {
                "status": "NOT_VERIFIED",
                "reason": "checkpoint-aware inventory needed; complete JSON history unavailable",
            }
        for line in commit.read_text().splitlines():
            action = json.loads(line)
            if "add" in action:
                active[action["add"]["path"]] = action["add"]
            if "remove" in action:
                active.pop(action["remove"]["path"], None)
    missing = sum(not (root / unquote(path)).is_file() for path in active)
    wrong_size = sum(
        (root / unquote(path)).is_file()
        and (root / unquote(path)).stat().st_size != entry["size"]
        for path, entry in active.items()
    )
    if missing or wrong_size:
        raise ValueError(
            "incomplete Delta snapshot: missing or wrong-size active data files"
        )
    return {
        "status": "COMPLETE",
        "active_data_files": len(active),
        "missing_data_files": missing,
        "wrong_size_data_files": wrong_size,
        "json_versions_checked": item["version"] + 1,
    }


def snapshot_integrity(frame, item):
    """Force all active row columns to be read, not just Delta count statistics."""
    inventory = local_file_inventory(item)
    encoded = F.to_json(
        F.struct(*frame.columns), {"ignoreNullFields": "false", "timeZone": "UTC"}
    )
    stats = (
        frame.agg(
            F.count("*").alias("rows"),
            F.min(F.sha2(encoded, 256)).alias("min_row_sha256"),
            F.max(F.sha2(encoded, 256)).alias("max_row_sha256"),
            F.sum(F.xxhash64(encoded).cast("decimal(38,0)")).alias("row_hash_sum"),
        )
        .first()
        .asDict()
    )
    stats["row_hash_sum"] = str(stats["row_hash_sum"])
    table = DeltaTable.forPath(frame.sparkSession, item["path"])
    latest = int(table.history(1).first().version)
    # inputFiles() on a cached Delta frame can be empty despite nonempty data.
    # detail() is valid for the pinned file count only when that version is latest.
    files = (
        int(table.detail().select("numFiles").first()[0])
        if latest == item["version"]
        else None
    )
    stats.update(
        path=item["path"],
        version=item["version"],
        active_data_files=files,
        physical_file_inventory=inventory,
        file_count_scope=(
            "matching latest pinned version"
            if files is not None
            else "not inferred for historical version"
        ),
        latest_version_observed=latest,
        status="FULL_PINNED_READ_COMPLETED",
        comparison_note="hash aggregates are diagnostics; equality uses exact distributed exceptAll",
    )
    return stats


def compare_sources(left, right):
    results = {}
    for name in TABLES:
        a, b = left[name], right[name]
        schema_match = [(f.name, f.dataType.simpleString()) for f in a.schema] == [
            (f.name, f.dataType.simpleString()) for f in b.schema
        ]
        results[name] = {
            "schema_match": schema_match,
            "left_rows": a.count(),
            "right_rows": b.count(),
        }
        if schema_match:
            missing = a.exceptAll(b).count()
            extra = b.exceptAll(a).count()
            results[name].update(
                left_only_rows=missing,
                right_only_rows=extra,
                exact_content_match=missing == extra == 0,
            )
        else:
            results[name]["exact_content_match"] = False
    return results


def readiness_audit(spark, inputs, config):
    sources, lineage = read_sources(spark, inputs)
    sources = {name: frame.persist() for name, frame in sources.items()}
    try:
        integrity = {
            name: snapshot_integrity(frame, lineage["inputs"]["tables"][name])
            for name, frame in sources.items()
        }
        counts, provinces, first, base_audit = weekly_counts(sources, config)
        panel, start, weeks = make_panel(counts, provinces, first, config)
        folds = fold_plan(weeks, config)
        audit, mapping = eligibility(panel, folds, config)
        history_end = start + timedelta(weeks=folds[0]["validation_start_index"])
        observed = panel.groupBy("province_id").agg(
            F.count("*").alias("panel_weeks"),
            F.sum("geo_linked_posts").alias("geo_posts_completed_range"),
            F.sum("target").alias("direct_posts_completed_range"),
            F.sum((F.col("geo_linked_posts") > 0).cast("long")).alias(
                "observed_geo_weeks"
            ),
            F.sum((F.col("target") > 0).cast("long")).alias("observed_direct_weeks"),
            F.sum(
                (
                    (F.col("week_index") < folds[0]["validation_start_index"])
                    & (F.col("geo_linked_posts") > 0)
                ).cast("long")
            ).alias("observed_geo_weeks_history"),
        )
        by_province = audit.join(observed, "province_id").select(
            "province_id",
            "province_name",
            "panel_weeks",
            F.col("historical_geo_posts").alias("geo_posts_history"),
            F.col("historical_direct_posts").alias("direct_posts_history"),
            F.col("eligible").alias("is_eligible"),
            "history_weeks",
            "geo_posts_completed_range",
            "direct_posts_completed_range",
            "observed_geo_weeks",
            "observed_direct_weeks",
            "observed_geo_weeks_history",
        )
        reasons = F.array_compact(
            F.array(
                F.when(
                    F.col("geo_posts_history") < config.min_geo_posts_history,
                    F.lit("INSUFFICIENT_GEO_POSTS_HISTORY"),
                ),
                F.when(
                    F.col("direct_posts_history") < config.min_direct_posts_history,
                    F.lit("INSUFFICIENT_DIRECT_POSTS_HISTORY"),
                ),
            )
        )
        by_province = (
            by_province.withColumn(
                "exclusion_reason",
                F.when(F.col("is_eligible"), "SELECTED_HISTORY_ONLY").otherwise(
                    F.concat_ws("|", reasons)
                ),
            )
            .withColumn(
                "additional_geo_posts_needed",
                F.greatest(
                    F.lit(0.0),
                    F.lit(config.min_geo_posts_history) - F.col("geo_posts_history"),
                ),
            )
            .withColumn(
                "additional_direct_posts_needed",
                F.greatest(
                    F.lit(0.0),
                    F.lit(config.min_direct_posts_history)
                    - F.col("direct_posts_history"),
                ),
            )
        )
        by_province = (
            by_province.withColumn(
                "minimum_additional_qualifying_posts",
                F.greatest(
                    "additional_geo_posts_needed", "additional_direct_posts_needed"
                ),
            )
            .withColumn(
                "geo_posts_after_history_cutoff",
                F.col("geo_posts_completed_range") - F.col("geo_posts_history"),
            )
            .withColumn(
                "direct_posts_after_history_cutoff",
                F.col("direct_posts_completed_range") - F.col("direct_posts_history"),
            )
            .withColumn(
                "would_pass_using_future_incorrectly",
                (F.col("geo_posts_completed_range") >= config.min_geo_posts_history)
                & (
                    F.col("direct_posts_completed_range")
                    >= config.min_direct_posts_history
                ),
            )
        )

        posts, geo, intents, _admin = (sources[n] for n in TABLES)
        posts = published_dates(posts)
        direct_evidence = (
            intents.filter(F.col("intent_type").isin(*DIRECT_INTENTS))
            .select("content_id")
            .distinct()
            .withColumn("_evidence_direct", F.lit(True))
        )
        posts = posts.join(direct_evidence, "content_id", "left").withColumn(
            "_direct",
            F.coalesce(F.col("_evidence_direct"), F.lit(False))
            | F.coalesce(
                F.arrays_overlap(
                    "intent_labels", F.array(*[F.lit(x) for x in DIRECT_INTENTS])
                ),
                F.lit(False),
            ),
        )
        end = config.last_complete_week + timedelta(days=6)
        dated = posts.filter(
            (F.col("publication_date") >= F.lit(start))
            & (F.col("publication_date") <= F.lit(end))
        )
        historical = dated.filter(F.col("publication_date") < F.lit(history_end))
        valid_status = geo.filter(
            F.col("resolution_status").isNotNull()
            & (F.trim("resolution_status") != "")
            & ~F.upper(F.trim("resolution_status")).isin(*INVALID_GEO)
        )
        nonnull_province = valid_status.filter(F.col("new_province_id").isNotNull())
        pairs = nonnull_province.select(
            "content_id", F.col("new_province_id").alias("province_id")
        ).dropDuplicates()
        matched = pairs.join(
            provinces.select("province_id"), "province_id", "left_semi"
        )
        post_pairs = matched.join(posts.select("content_id"), "content_id", "left_semi")
        dated_pairs = post_pairs.join(
            dated.select("content_id"), "content_id", "left_semi"
        )
        historical_pairs = dated_pairs.join(
            historical.select("content_id"), "content_id", "left_semi"
        )
        direct_pairs = historical_pairs.join(
            historical.filter("_direct").select("content_id"), "content_id", "left_semi"
        )
        stage_rows = []

        def chain(name, grain, frames):
            previous = None
            for stage, frame in frames:
                count = frame.count()
                removed = previous - count if previous is not None else None
                stage_rows.append(
                    (
                        name,
                        stage,
                        grain,
                        count,
                        previous,
                        removed,
                        removed / previous if previous else None,
                    )
                )
                previous = count

        chain(
            "geo_rows",
            "mention",
            [
                ("raw", geo),
                ("valid_resolution_status", valid_status),
                ("nonnull_new_province_id", nonnull_province),
            ],
        )
        chain(
            "geo_pairs",
            "content_id x province_id",
            [
                ("distinct_pairs_before_admin", pairs),
                ("valid_new_admin_province", matched),
                ("belongs_to_social_post", post_pairs),
                ("completed_publication_range", dated_pairs),
                ("before_first_validation", historical_pairs),
                ("direct_demand_history", direct_pairs),
            ],
        )
        chain(
            "posts",
            "content_id",
            [
                ("raw_posts", posts),
                (
                    "nonnull_publication_date",
                    posts.filter(F.col("publication_date").isNotNull()),
                ),
                ("completed_publication_range", dated),
                ("before_first_validation", historical),
                ("direct_demand_history", historical.filter("_direct")),
                (
                    "direct_history_with_valid_geo",
                    historical.filter("_direct").join(
                        matched.select("content_id").distinct(),
                        "content_id",
                        "left_semi",
                    ),
                ),
            ],
        )
        stages = spark.createDataFrame(
            stage_rows,
            "pipeline string, stage string, grain string, retained_rows long, previous_rows long, excluded_rows long, exclusion_ratio double",
        )
        unknown_ids = (
            pairs.join(provinces.select("province_id"), "province_id", "left_anti")
            .groupBy("province_id")
            .count()
        )
        scope = (
            F.col("content_type")
            if "content_type" in geo.columns
            else F.lit("NOT_PROVIDED")
        )
        nonpost = (
            geo.join(posts.select("content_id"), "content_id", "left_anti")
            .groupBy(scope.alias("content_type"))
            .count()
        )
        direct_history = historical.filter("_direct")
        missing_geo = direct_history.join(
            matched.select("content_id").distinct(), "content_id", "left_anti"
        )
        raw_geo_ids = geo.select("content_id").distinct()
        join_audit = {
            "post_content_id_unique": posts.select("content_id").distinct().count()
            == posts.count(),
            "geo_duplicate_pair_rows_removed": nonnull_province.count() - pairs.count(),
            "admin_unmatched_pairs": pairs.count() - matched.count(),
            "geo_content_ids_absent_from_posts": geo.join(
                posts.select("content_id"), "content_id", "left_anti"
            )
            .select("content_id")
            .distinct()
            .count(),
            "nonpost_geo_scope_counts": [r.asDict() for r in nonpost.collect()],
            "evidence_rows": intents.count(),
            "evidence_distinct_content_ids": intents.select("content_id")
            .distinct()
            .count(),
            "evidence_rows_not_post_scope": intents.join(
                posts.select("content_id"), "content_id", "left_anti"
            ).count(),
            "direct_posts_history_total": direct_history.count(),
            "direct_posts_history_without_valid_geo": missing_geo.count(),
            "direct_history_without_any_geo_evidence": missing_geo.join(
                raw_geo_ids, "content_id", "left_anti"
            ).count(),
            "direct_history_with_only_invalid_or_unmapped_geo": missing_geo.join(
                raw_geo_ids, "content_id", "left_semi"
            ).count(),
        }
        # Check that audit reconstructions match the unchanged production counts.
        expected = by_province.agg(
            F.sum("geo_posts_history"), F.sum("direct_posts_history")
        ).first()
        if (int(expected[0]), int(expected[1])) != (
            historical_pairs.count(),
            direct_pairs.count(),
        ):
            raise ValueError("audit/model count reconciliation failed")
        audit_rows = [
            r.asDict()
            for r in by_province.orderBy("province_id")
            .limit(config.max_provinces + 1)
            .collect()
        ]
        shortages = sorted(
            [r for r in audit_rows if not r["is_eligible"]],
            key=lambda r: (r["minimum_additional_qualifying_posts"], r["province_id"]),
        )
        needed = max(0, config.min_provinces - len(mapping))
        report = {
            "status": "READY" if needed == 0 else "BLOCKED",
            "reason": (
                None
                if needed == 0
                else f"{len(mapping)} eligible provinces; requires {config.min_provinces}"
            ),
            "lineage": lineage,
            "config": config.to_dict(),
            "snapshot_integrity": integrity,
            "panel_start": start.isoformat(),
            "panel_weeks": weeks,
            "history_end_exclusive": history_end.isoformat(),
            "last_complete_week": config.last_complete_week.isoformat(),
            "base_counts": base_audit,
            "eligible_provinces": mapping,
            "eligibility": audit_rows,
            "province_shortage": needed,
            "nearest_recovery_candidates": shortages[:needed],
            "geographic_resolution_counts": [
                r.asDict() for r in geo.groupBy("resolution_status").count().collect()
            ],
            "unknown_new_province_ids": [
                r.asDict()
                for r in unknown_ids.limit(config.max_provinces + 1).collect()
            ],
            "observed_new_province_ids": [
                r.asDict()
                for r in geo.groupBy("new_province_id")
                .count()
                .limit(config.max_provinces + 1)
                .collect()
            ],
            "join_audit": join_audit,
            "stages": stage_rows,
            "source_versions_after": {
                name: int(
                    DeltaTable.forPath(spark, lineage["inputs"]["tables"][name]["path"])
                    .history(1)
                    .first()
                    .version
                )
                for name in TABLES
            },
            "notes": [
                "panel_weeks counts the complete zero-filled calendar, observed weeks are reported separately",
                "geo/evidence IDs outside social_posts may be comment scope, not necessarily broken foreign keys",
                "additional qualifying posts must fall before the fixed history cutoff and be distinct, geo-linked, and source-permitted; direct posts also satisfy the geo threshold",
                "no model/scaler fitting, threshold changes, source writes, or production promotion",
            ],
        }
        return by_province, stages, report
    finally:
        for frame in sources.values():
            frame.unpersist()
