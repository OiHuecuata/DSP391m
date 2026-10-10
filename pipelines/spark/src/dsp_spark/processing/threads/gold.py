"""Threads research components and qualified EDA, alongside untouched legacy Gold."""

from delta.tables import DeltaTable
from dsp_collector.normalizers.observation import digest
from pyspark.sql import Window
from pyspark.sql import functions as F

from ...io.delta import read_delta
from ..clean.contracts import spark_schema
from .contracts import GRAINS, PREVIEW, RESEARCH, SILVER, path
from .silver import checked, replace_if_changed


def _align(frame, kind):
    from pyspark.sql.types import ArrayType

    return frame.select(
        *[
            (
                F.col(field.name)
                if isinstance(field.dataType, ArrayType)
                else F.col(field.name).cast(field.dataType)
            ).alias(field.name)
            for field in spark_schema(kind)
        ]
    )


def build(spark, settings, *, collections, metric_window="Latest", as_of=None):
    if metric_window not in ("Latest", "First72H"):
        raise ValueError("Supported metric windows: Latest, First72H")
    frames = {name: read_delta(spark, path(settings, name)) for name in SILVER}
    posts, comments = frames[SILVER[0]], frames[SILVER[1]]
    contents = posts.unionByName(comments)
    snapshots = frames[SILVER[2]]
    maximum = snapshots.agg(F.max("observed_at")).first()[0]
    as_of = as_of or maximum
    if as_of is None:
        raise ValueError("An explicit as_of is required for an empty acquisition")
    if contents.filter(F.col("observed_at") > as_of).limit(1).count():
        raise ValueError(
            "Historical content state is not available in current Silver; use a pinned earlier generation"
        )
    versions = {
        name: int(
            DeltaTable.forPath(spark, path(settings, name)).history(1).first().version
        )
        for name in SILVER
    }
    snapshot_id = "sha256:" + digest(
        [versions, settings.paths.base, as_of, metric_window]
    )
    used = snapshots.select("observation_id").distinct()
    valid_until = (
        read_delta(spark, path(settings, "admitted_threads_observations"))
        .join(used, "observation_id")
        .agg(F.min("retention_expires_at"))
        .first()[0]
    )
    eligible = snapshots.filter(
        (F.col("observed_at") <= as_of) & (F.col("snapshot_status") == "OBSERVED")
    )
    if metric_window == "First72H":
        eligible = eligible.filter(F.col("content_age_hours").between(0, 72))
    latest = (
        eligible.withColumn(
            "_rank",
            F.row_number().over(
                Window.partitionBy("content_id").orderBy(
                    F.col("observed_at").desc(), F.col("observation_id").desc()
                )
            ),
        )
        .filter("_rank = 1")
        .drop("_rank")
    )
    fields = [
        "views",
        "likes",
        "comments",
        "shares",
        "reposts",
        "quotes",
        "likes_delta",
        "comments_delta",
        "reposts_delta",
        "quotes_delta",
        "content_age_hours",
        "metrics_available",
        "metric_availability_json",
    ]
    selected = latest.select(
        "content_id", *fields, F.col("observed_at").alias("latest_observed_at")
    )
    stats = snapshots.groupBy("content_id").agg(
        F.countDistinct("observation_id").alias("observation_count"),
        F.countDistinct("observation_id").alias("snapshot_count"),
        F.min("observed_at").alias("first_observed_at"),
        F.collect_set("query_id").alias("query_ids"),
    )
    stats = stats.withColumn("query_ids", F.array_sort("query_ids"))
    mentions = frames[SILVER[3]]
    geo = mentions.groupBy("content_id").agg(
        F.collect_set("location_text_raw").alias("location_terms"),
        F.collect_set("admin_unit_id").alias("admin_unit_ids"),
        F.collect_set("admin_version").alias("admin_versions"),
        F.max(F.when(F.col("resolution_status") == "Resolved", 1).otherwise(0)).alias(
            "_resolved"
        ),
        F.max(F.when(F.col("resolution_status") != "Resolved", 1).otherwise(0)).alias(
            "_unresolved"
        ),
    )
    preview = (
        contents.drop("text_normalized", "comment_text_normalized")
        .join(selected, "content_id", "left")
        .join(stats, "content_id", "left")
        .join(geo, "content_id", "left")
    )
    preview = preview.withColumn(
        "geo_resolution",
        F.when(
            F.col("_resolved") == 1,
            F.when(F.col("_unresolved") == 1, "Mixed").otherwise("Resolved"),
        )
        .when(F.size("location_terms") > 0, "Unresolved")
        .otherwise("NoMention"),
    )
    preview = preview.withColumn(
        "week_start_vn",
        F.date_format(
            F.date_trunc("week", F.to_date("published_date_vn")), "yyyy-MM-dd"
        ),
    ).withColumn("month_vn", F.substring("published_date_vn", 1, 7))
    for name in (
        "location_terms",
        "admin_unit_ids",
        "admin_versions",
        "metrics_available",
    ):
        preview = preview.withColumn(
            name, F.array_sort(F.coalesce(F.col(name), F.array().cast("array<string>")))
        )
    gold = {
        PREVIEW[0]: _align(preview, PREVIEW[0]),
        PREVIEW[1]: spark.createDataFrame(collections, spark_schema(PREVIEW[1])),
    }
    # Official admin groupings require actual reviewed aliases. Legacy city keys
    # remain raw mentions and never become PRE_2025/POST_2025 IDs.
    resolved = (
        mentions.filter("resolution_status = 'Resolved'")
        .select("content_id", "admin_version", "admin_unit_id")
        .distinct()
    )
    located = (
        contents.join(resolved, "content_id")
        .filter("published_date_vn IS NOT NULL")
        .withColumnRenamed("published_date_vn", "activity_date")
    )
    dims = ["activity_date", "admin_version", "admin_unit_id", "platform"]
    p = located.filter("content_type = 'Post'").join(selected, "content_id", "left")
    aggregates = [
        F.countDistinct("content_id").alias("post_count"),
        F.countDistinct(
            F.when(F.size("metrics_available") > 0, F.col("content_id"))
        ).alias("posts_with_metrics_count"),
    ]
    mappings = {
        "views": "total_views",
        "likes": "total_likes",
        "comments": "total_comments_reported",
        "shares": "total_shares",
        "reposts": "total_reposts",
        "quotes": "total_quotes",
    }
    for metric, output in mappings.items():
        aggregates += [
            F.sum(metric).alias(output),
            F.count(metric).alias("posts_with_" + metric),
        ]
    aggregates += [
        F.avg("likes").alias("avg_likes_per_post"),
        F.avg("comments").alias("avg_comments_per_post"),
    ]
    engagement = (
        p.groupBy(*dims)
        .agg(*aggregates)
        .withColumn("median_engagement", F.lit(None).cast("double"))
        .withColumn("high_engagement_post_count", F.lit(None).cast("long"))
        .withColumn("snapshot_as_of", F.lit(as_of))
        .withColumn("metric_window", F.lit(metric_window))
    )
    gold["social_engagement_daily_v1"] = engagement
    gold["social_activity_by_admin_version_daily"] = located.groupBy(
        "activity_date", "admin_version", "admin_unit_id"
    ).agg(
        F.countDistinct(
            F.when(F.col("content_type") == "Post", F.col("content_id"))
        ).alias("post_count"),
        F.countDistinct(
            F.when(F.col("content_type") == "Comment", F.col("content_id"))
        ).alias("comment_count"),
        F.countDistinct("content_id").alias("unique_content_count"),
    )
    intent = frames[SILVER[4]].filter(
        "classification_status = 'EVALUATED_QUALIFIED' AND evaluation_reference IS NOT NULL"
    )
    intent = (
        intent.select("content_id", "intent_type", "intent_location_id", "evidence_id")
        .join(
            located.select("content_id", *dims, "content_type", "parent_comment_id"),
            "content_id",
        )
        .filter(F.col("intent_location_id") == F.col("admin_unit_id"))
    )
    intent_daily = intent.groupBy(*dims, "intent_type").agg(
        F.countDistinct(
            F.when(F.col("content_type") == "Post", F.col("content_id"))
        ).alias("post_count"),
        F.countDistinct(
            F.when(F.col("content_type") == "Comment", F.col("content_id"))
        ).alias("comment_count"),
        F.countDistinct(
            F.when(F.col("parent_comment_id").isNotNull(), F.col("content_id"))
        ).alias("reply_count"),
        F.countDistinct("evidence_id").alias("evidence_count"),
        F.countDistinct("content_id").alias("resolved_content_count"),
    )
    gold["social_intent_daily_v2"] = intent_daily.withColumn(
        "high_confidence_count", F.lit(None).cast("long")
    ).withColumn("generated_at", F.lit(as_of))
    collected = (
        located.filter("content_type = 'Comment'")
        .groupBy(*dims)
        .agg(
            F.countDistinct("content_id").alias("collected_comment_count"),
            F.countDistinct("content_id").alias("comments_with_resolved_geo"),
        )
    )
    for column in [
        f.name
        for f in spark_schema("social_comment_analytics_daily_v1")
        if f.name
        not in (
            *dims,
            "collected_comment_count",
            "comments_with_resolved_geo",
            "source_collection_status",
            "quality_status",
            "input_snapshot_id",
        )
    ]:
        collected = collected.withColumn(column, F.lit(None).cast("long"))
    gold["social_comment_analytics_daily_v1"] = collected.withColumn(
        "source_collection_status", F.lit("VISIBLE_PARTIAL_CLASSIFICATION_UNEVALUATED")
    )
    coverage = (
        located.withColumn(
            "period_start", F.concat(F.substring("activity_date", 1, 7), F.lit("-01"))
        )
        .withColumn(
            "period_end",
            F.date_format(F.last_day(F.to_date("activity_date")), "yyyy-MM-dd"),
        )
        .groupBy("admin_unit_id", "platform", "period_start", "period_end")
        .agg(
            F.countDistinct("content_id").alias("resolved_records"),
            F.max("observed_at").alias("last_success_at"),
        )
        .withColumnRenamed("platform", "source_platform")
    )
    for name in (
        "planned_queries",
        "successful_queries",
        "failed_queries",
        "records_returned",
        "records_accepted",
    ):
        coverage = coverage.withColumn(name, F.lit(None).cast("long"))
    gold["national_location_coverage_v1"] = coverage.withColumn(
        "collection_status", F.lit("Partial")
    )
    terms = (
        mentions.filter(
            "resolution_status = 'Resolved' AND published_date_vn IS NOT NULL"
        )
        .withColumn("month", F.substring("published_date_vn", 1, 7))
        .withColumn("location_term_raw_normalized", F.lower("location_text_raw"))
        .withColumnRenamed("admin_unit_id", "canonical_location_id")
        .withColumnRenamed("admin_version", "term_admin_version")
    )
    term_dims = GRAINS["location_term_usage_monthly_v1"]
    term_usage = terms.groupBy(*term_dims).agg(
        F.sum(F.when(F.col("content_type") == "Post", 1).otherwise(0)).alias(
            "post_mention_count"
        ),
        F.sum(F.when(F.col("content_type") == "Comment", 1).otherwise(0)).alias(
            "comment_mention_count"
        ),
        F.countDistinct("content_id").alias("unique_content_count"),
    )
    scope = Window.partitionBy(
        "month", "canonical_location_id", "term_admin_version", "platform"
    )
    term_usage = term_usage.withColumn(
        "term_usage_share",
        (F.col("post_mention_count") + F.col("comment_mention_count"))
        / F.sum(F.col("post_mention_count") + F.col("comment_mention_count")).over(
            scope
        ),
    ).withColumn("resolution_coverage", F.lit(None).cast("double"))
    gold["location_term_usage_monthly_v1"] = term_usage
    # Threads social-only monthly inputs; canonical supply and evaluated labels
    # are separate prerequisites, never filled from legacy market aliases.
    monthly = (
        located.withColumn("month", F.substring("activity_date", 1, 7))
        .join(selected, "content_id", "left")
        .groupBy("month", "admin_unit_id", "admin_version")
        .agg(
            F.sum(F.when(F.col("content_type") == "Post", F.col("views"))).alias(
                "total_views"
            ),
            F.sum(F.when(F.col("content_type") == "Post", F.col("likes"))).alias(
                "total_likes"
            ),
            F.sum(F.when(F.col("content_type") == "Post", F.col("shares"))).alias(
                "total_shares"
            ),
            F.countDistinct("platform").alias("source_count"),
        )
    )
    for field in spark_schema("location_market_research_monthly_v1"):
        if field.name not in monthly.columns:
            monthly = monthly.withColumn(field.name, F.lit(None).cast(field.dataType))
    gold["location_market_research_monthly_v1"] = (
        monthly.withColumn(
            "social_coverage_status", F.lit("THREADS_ONLY_CLASSIFICATION_UNEVALUATED")
        )
        .withColumn("supply_coverage_status", F.lit("MISSING_CANONICAL_SUPPLY"))
        .withColumn("generated_at", F.lit(as_of))
    )
    status = {}
    for name in RESEARCH:
        if name not in gold:
            gold[name] = spark.createDataFrame([], spark_schema(name))
            status[name] = "BLOCKED_DEPENDENCY"
        else:
            gold[name] = (
                gold[name]
                .withColumn("quality_status", F.lit("PARTIAL_COMPONENTS"))
                .withColumn("input_snapshot_id", F.lit(snapshot_id))
            )
            status[name] = "PARTIAL_COMPONENTS"
    counts = {}
    for name in RESEARCH + PREVIEW:
        frame = _align(gold[name], name).cache()
        import json

        from ..clean.contracts import contract_documents

        checked(frame, name, json.loads(contract_documents())[name]["x-grain"])
        replace_if_changed(frame, path(settings, name))
        counts[name] = frame.count()
        frame.unpersist()
        if not counts[name] and name in RESEARCH:
            status[name] = "BLOCKED_DEPENDENCY"
    return {
        "input_snapshot_id": snapshot_id,
        "as_of": as_of,
        "valid_until": valid_until,
        "metric_window": metric_window,
        "counts": counts,
        "dataset_status": status,
        "silver_versions": versions,
        "scope": "THREADS_EXPLORATORY",
        "dependencies": [
            "reviewed_versioned_admin_aliases",
            "evaluated_relevance_precision_0.90",
            "evaluated_intent_macro_f1_0.80",
            "evaluated_province_precision_0.90",
            "platform_engagement_definition_and_threshold",
            "canonical_venue_supply_and_discovery",
            "query_admin_scope_registry",
        ],
    }
