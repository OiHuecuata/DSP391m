"""Silver-only loading, cardinality-safe counts and a continuous VN calendar panel."""

from datetime import timedelta

from pyspark.sql import functions as F

from ...io.s3 import storage_uri
from ...processing.intelligence.catalog import contract
from ..config import fingerprint
from ..data import require_schema, unique_keys
from .config import CHANNELS, DIRECT_INTENTS, TABLES, InsufficientHistory

INVALID_GEO = ("UNRESOLVED", "REJECTED", "FAILED", "AMBIGUOUS", "NO_MATCH")


def normalized_inputs(inputs):
    if set(inputs["tables"]) != set(TABLES):
        raise ValueError(
            "provide exactly the four required Silver snapshots; no Bronze/Gold fallback"
        )
    if inputs.get("provenance") not in (
        "real",
        "synthetic",
        "mixed",
        "unknown",
    ) or not inputs.get("dataset_id"):
        raise ValueError("declare dataset_id and provenance")
    result = {**inputs, "tables": {}}
    for name, item in inputs["tables"].items():
        if (
            set(item) != {"path", "version"}
            or type(item["version"]) is not int
            or item["version"] < 0
        ):
            raise ValueError(
                "each Silver snapshot needs path and nonnegative integer Delta version"
            )
        result["tables"][name] = {
            "path": storage_uri(item["path"]),
            "version": item["version"],
        }
    return result


def read_sources(spark, inputs):
    inputs = normalized_inputs(inputs)
    sources, audit = {}, {}
    for name, item in inputs["tables"].items():
        frame = (
            spark.read.format("delta")
            .option("versionAsOf", item["version"])
            .load(item["path"])
        )
        require_schema(frame, {"schema_version": "string"})
        expected = contract(name)["properties"]["schema_version"]["const"]
        if frame.filter(~F.col("schema_version").eqNullSafe(expected)).limit(1).count():
            raise ValueError(f"unsupported Silver schema_version: {name}")
        sources[name] = frame
        audit[name] = {
            **item,
            "rows": frame.count(),
            "schema": frame.schema.jsonValue(),
        }
    return sources, {
        "inputs": inputs,
        "data_version": fingerprint(inputs),
        "tables": audit,
    }


def published_dates(posts):
    """UTC Spark session; prefer validated VN date, explicit-zone UTC fallback."""
    if posts.sparkSession.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError(
            "published date conversion requires the repository UTC Spark session"
        )
    for column in ("published_date_vn", "published_at"):
        if column not in posts.columns:
            raise ValueError(f"missing Silver publication field: {column}")
    kinds = {f.name: f.dataType.simpleString() for f in posts.schema}
    if kinds["published_date_vn"] not in ("string", "date") or kinds[
        "published_at"
    ] not in ("string", "timestamp"):
        raise ValueError("unsupported publication date/timestamp schema")
    vn_date = F.expr("try_cast(published_date_vn as date)")
    if (
        posts.filter(F.col("published_date_vn").isNotNull() & vn_date.isNull())
        .limit(1)
        .count()
    ):
        raise ValueError("invalid published_date_vn")
    if kinds["published_at"] == "string":
        fallback = F.try_to_timestamp("published_at")
        explicit_zone = F.col("published_at").rlike(r"(Z|[+-]\d{2}:?\d{2})$")
        if (
            posts.filter(
                vn_date.isNull()
                & F.col("published_at").isNotNull()
                & (~explicit_zone | fallback.isNull())
            )
            .limit(1)
            .count()
        ):
            raise ValueError(
                "fallback published_at must be valid with explicit timezone"
            )
    else:
        fallback = F.col("published_at")
    return posts.withColumn(
        "publication_date",
        F.coalesce(
            vn_date, F.to_date(F.from_utc_timestamp(fallback, "Asia/Ho_Chi_Minh"))
        ),
    )


def weekly_counts(sources, config):
    posts, geo, intents, admin = (sources[n] for n in TABLES)
    require_schema(
        posts,
        {
            "content_id": "string",
            "intent_labels": "array<string>",
            "is_commercial": "boolean",
        },
    )
    require_schema(
        geo,
        {
            "content_id": "string",
            "new_province_id": "string",
            "resolution_status": "string",
        },
    )
    require_schema(intents, {"content_id": "string", "intent_type": "string"})
    require_schema(
        admin,
        {
            "admin_unit_id": "string",
            "admin_name": "string",
            "admin_level": "string",
            "admin_version": "string",
        },
    )
    unique_keys(posts, ("content_id",))
    unique_keys(admin, ("admin_unit_id",))
    provinces = admin.filter(
        (F.col("admin_level") == "PROVINCE")
        & (F.col("admin_version") == config.admin_version)
    ).select(
        F.col("admin_unit_id").alias("province_id"),
        F.col("admin_name").alias("province_name"),
    )
    p = provinces.count()
    if (
        provinces.filter(
            F.col("province_name").isNull()
            | (F.trim("province_name") == "")
            | (F.trim("province_id") == "")
        )
        .limit(1)
        .count()
    ):
        raise ValueError("province IDs and display names must be nonempty")
    if not 1 <= p <= config.max_provinces:
        raise InsufficientHistory(
            "no matching admin provinces or province metadata bound exceeded"
        )
    intent_flags = intents.groupBy("content_id").agg(
        *[
            F.max(F.when(F.col("intent_type") == intent, 1).otherwise(0)).alias(
                f"e_{channel}"
            )
            for intent, channel in zip(DIRECT_INTENTS, CHANNELS[2:5])
        ]
    )
    posts = published_dates(posts).join(intent_flags, "content_id", "left")
    for intent, channel in zip(DIRECT_INTENTS, CHANNELS[2:5]):
        posts = posts.withColumn(
            channel,
            F.greatest(
                F.coalesce(
                    F.array_contains("intent_labels", intent).cast("int"), F.lit(0)
                ),
                F.coalesce(F.col(f"e_{channel}"), F.lit(0)),
            ),
        )
    posts = posts.withColumn(
        "target", F.greatest(*[F.col(c) for c in CHANNELS[2:5]])
    ).withColumn(
        "commercial_geo_posts", F.coalesce(F.col("is_commercial").cast("int"), F.lit(0))
    )
    pairs = (
        geo.filter(
            F.col("resolution_status").isNotNull()
            & (F.trim("resolution_status") != "")
            & ~F.upper(F.trim("resolution_status")).isin(*INVALID_GEO)
            & F.col("new_province_id").isNotNull()
        )
        .select("content_id", F.col("new_province_id").alias("province_id"))
        .dropDuplicates(["content_id", "province_id"])
        .join(provinces.select("province_id"), "province_id", "left_semi")
    )
    dated = posts.filter(
        F.col("publication_date")
        <= F.lit(config.last_complete_week + timedelta(days=6))
    )
    first = dated.agg(F.min("publication_date")).first()[0]
    if first is None:
        raise InsufficientHistory("no dated posts within completed observed weeks")
    joined = dated.join(pairs, "content_id", "inner").withColumn(
        "week_start",
        F.to_date(F.date_trunc("week", F.col("publication_date").cast("timestamp"))),
    )
    counts = joined.groupBy("province_id", "week_start").agg(
        F.count("*").cast("double").alias("geo_linked_posts"),
        *[
            F.sum(c).cast("double").alias(c)
            for c in CHANNELS
            if c != "geo_linked_posts"
        ],
    )
    audit = {
        "dated_posts_in_completed_range": dated.count(),
        "posts_missing_publication_date": posts.filter(
            F.col("publication_date").isNull()
        ).count(),
        "valid_distinct_geo_pairs": pairs.count(),
        "province_reference_count": p,
        "earliest_observed_publication_date": first.isoformat(),
        "last_complete_week": config.last_complete_week.isoformat(),
        "intent_policy": "union Silver intent_labels and intent evidence; max flag per post; target is OR of three direct intents",
        "geography_semantics": "mentioned province, not the author location",
        "zero_semantics": "no matching collected observation, not zero real-world demand",
    }
    return counts, provinces, first, audit


def make_panel(counts, provinces, first_date, config):
    from datetime import date

    start = (
        date.fromisoformat(config.panel_start)
        if config.panel_start
        else first_date - timedelta(days=first_date.weekday())
    )
    end = config.last_complete_week
    weeks = (end - start).days // 7 + 1
    if not 1 <= weeks <= config.max_panel_weeks:
        raise InsufficientHistory("invalid/excessive completed calendar range")
    calendar = counts.sparkSession.range(weeks).select(
        F.col("id").cast("int").alias("week_index"),
        F.date_add(F.lit(start), (F.col("id") * 7).cast("int")).alias("week_start"),
    )
    panel = (
        provinces.crossJoin(calendar)
        .join(counts, ["province_id", "week_start"], "left")
        .fillna(0.0, subset=list(CHANNELS))
    )
    unique_keys(panel, ("province_id", "week_start"))
    return panel, start, weeks


def eligibility(panel, folds, config):
    cutoff = folds[0]["validation_start_index"]
    history = panel.filter(F.col("week_index") < cutoff)
    audit = history.groupBy("province_id", "province_name").agg(
        F.count("*").alias("history_weeks"),
        F.sum("geo_linked_posts").alias("historical_geo_posts"),
        F.sum("target").alias("historical_direct_posts"),
    )
    audit = (
        audit.withColumn(
            "eligible",
            (F.col("historical_geo_posts") >= config.min_geo_posts_history)
            & (F.col("historical_direct_posts") >= config.min_direct_posts_history),
        )
        .withColumn(
            "reason",
            F.when(F.col("eligible"), "SELECTED_HISTORY_ONLY")
            .when(
                F.col("historical_geo_posts") < config.min_geo_posts_history,
                "INSUFFICIENT_GEO_HISTORY",
            )
            .otherwise("INSUFFICIENT_DIRECT_HISTORY"),
        )
        .withColumn("history_end_index_exclusive", F.lit(cutoff))
    )
    mapping = [
        r.asDict()
        for r in audit.filter("eligible")
        .select("province_id", "province_name")
        .orderBy("province_id")
        .limit(config.max_provinces + 1)
        .collect()
    ]
    return audit, mapping
