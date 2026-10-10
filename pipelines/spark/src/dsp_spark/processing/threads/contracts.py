"""Registered research grains and typed columns, shared by Delta and ClickHouse."""

from ..clean.contracts import CONTRACT_FILES

SILVER = [
    "silver_social_posts",
    "silver_social_comments",
    "silver_social_engagement_snapshots",
    "silver_social_geo_mentions",
    "silver_social_intent_evidence",
]
RESEARCH = [
    "court_supply_by_admin_version_v2",
    "social_engagement_daily_v1",
    "social_intent_daily_v2",
    "social_comment_analytics_daily_v1",
    "social_activity_by_admin_version_daily",
    "national_location_coverage_v1",
    "venue_community_feedback_weekly_v1",
    "location_term_usage_monthly_v1",
    "venue_discovery_trends_v1",
    "location_market_research_monthly_v1",
]
PREVIEW = ["threads_content_eda_v1", "threads_collection_eda_v1"]

GRAINS = {
    "silver_social_posts": ["content_id"],
    "silver_social_comments": ["content_id"],
    "silver_social_engagement_snapshots": ["observation_id"],
    "silver_social_geo_mentions": ["mention_id"],
    "silver_social_intent_evidence": ["evidence_id"],
    "social_engagement_daily_v1": [
        "activity_date",
        "admin_version",
        "admin_unit_id",
        "platform",
    ],
    "social_intent_daily_v2": [
        "activity_date",
        "admin_version",
        "admin_unit_id",
        "platform",
        "intent_type",
    ],
    "social_comment_analytics_daily_v1": [
        "activity_date",
        "admin_version",
        "admin_unit_id",
        "platform",
    ],
    "social_activity_by_admin_version_daily": [
        "activity_date",
        "admin_version",
        "admin_unit_id",
    ],
    "national_location_coverage_v1": [
        "admin_unit_id",
        "source_platform",
        "period_start",
        "period_end",
    ],
    "location_term_usage_monthly_v1": [
        "month",
        "location_term_raw_normalized",
        "canonical_location_id",
        "term_admin_version",
        "platform",
    ],
    "threads_content_eda_v1": ["content_id"],
    "threads_collection_eda_v1": ["collection_run_id"],
}

for name in SILVER + RESEARCH + PREVIEW:
    CONTRACT_FILES[name] = (
        f"data/{'silver' if name in SILVER else 'gold'}/{name}.schema.json"
    )


def path(settings, name):
    if name not in SILVER + RESEARCH + PREVIEW + [
        "admitted_threads_observations",
        "rejected_threads_events",
    ]:
        raise ValueError("Unregistered Threads dataset")
    layer = (
        "silver"
        if name in SILVER + ["admitted_threads_observations"]
        else "rejected"
        if name == "rejected_threads_events"
        else "gold"
    )
    return f"{settings.paths.base}/{layer}/{name}"
