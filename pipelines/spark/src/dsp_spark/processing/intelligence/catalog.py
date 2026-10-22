"""Dataset grains and schema specifications; JSON contracts are checked in.

All fields are required; a trailing ? means an explicitly nullable measurement.
Identifiers are internal keys, never invented official administrative codes.
"""

VERSION = "requirements-v1.0-r5"
COMMON = "schema_version:s processing_version:s lineage_ids:a"
GOLD_COMMON = (
    COMMON
    + " generated_at:t metric_definition_id:s completeness_status:s source_platforms:a"
)

# s=string, i=int64, n=number, b=boolean, a=array<string>, t=UTC timestamp,
# d=calendar date. Canonical timestamp strings follow the existing contracts.
SPECS = {
    "silver_venue_candidates": (
        "candidate_id",
        "evidence candidate",
        "candidate_id:s venue_id:s? venue_name:s? address_text:s? latitude:n? longitude:n? geocoding_precision:s court_count:i? price_min_vnd:n? price_max_vnd:n? price_unit:s? venue_status:s source_platform:s source_record_id:s evidence_id:s discovered_at:t match_status:s verification_status:s confidence_score:n? coordinate_source:s? conflicting_fields:a",
    ),
    "silver_court_venues": (
        "venue_id",
        "physical venue",
        "venue_id:s venue_name:s? address_normalized:s? latitude:n? longitude:n? coordinate_source:s? geocoding_precision:s old_province_id:s? old_district_id:s? old_ward_id:s? new_province_id:s? new_ward_id:s? court_count:i? court_count_status:s price_min_vnd:n? price_max_vnd:n? price_unit:s? is_indoor:b? has_lighting:b? has_parking:b? venue_status:s verification_status:s first_seen_at:t last_seen_at:t source_count:i conflicting_fields:a evidence_ids:a source_platforms:a",
    ),
    "silver_venue_evidence": (
        "evidence_id",
        "venue observation or social extraction",
        "evidence_id:s candidate_id:s venue_id:s? content_id:s? source_platform:s source_record_id:s source_url:s? observed_at:t evidence_type:s evidence_span:s? extraction_method:s attributes_json:s",
    ),
    "silver_venue_source_links": (
        "source_platform source_record_id record_type",
        "source entity to canonical venue",
        "source_platform:s source_record_id:s record_type:s venue_id:s first_seen_at:t last_seen_at:t match_method:s evidence_ids:a",
    ),
    "silver_social_posts": (
        "post_id",
        "current source post or video",
        "post_id:s content_id:s platform:s source_record_id:s source_url:s? content_type:s parent_post_id:s? text_normalized:s published_at:t? published_date_vn:d? language:s? is_pickleball_related:b? is_commercial:b? intent_labels:a topic_labels:a sentiment:s? relevance_confidence:n? classification_confidence:n? classification_status:s extraction_method:s is_duplicate:b duplicate_group_id:s? text_hash:s first_seen_at:t last_seen_at:t mentioned_venue_ids:a",
    ),
    "silver_social_comments": (
        "comment_id",
        "current source comment or reply",
        "comment_id:s content_id:s post_id:s parent_comment_id:s? platform:s source_record_id:s source_url:s? content_type:s comment_text_normalized:s published_at:t? published_date_vn:d? is_reply:b is_pickleball_related:b? is_commercial:b? intent_labels:a topic_labels:a sentiment:s? classification_confidence:n? classification_status:s extraction_method:s text_hash:s first_seen_at:t last_seen_at:t mentioned_venue_ids:a is_duplicate:b duplicate_group_id:s?",
    ),
    "silver_social_engagement_snapshots": (
        "snapshot_id",
        "content x observed instant (cumulative)",
        "snapshot_id:s content_id:s content_type:s platform:s observed_at:t observed_date_vn:d metric_scope:s views:i? likes:i? comments:i? shares:i? reposts:i? quotes:i? replies:i? content_age_hours:n? engagement_total:i? engagement_rate:n? engagement_rate_basis:s? metrics_available:a metric_availability_json:s snapshot_status:s previous_snapshot_id:s? elapsed_hours:n? delta_views:i? delta_likes:i? delta_comments:i? delta_shares:i? views_per_hour:n? delta_status:s",
    ),
    "silver_social_geo_mentions": (
        "mention_id",
        "content x text span x semantic role",
        "mention_id:s content_id:s content_type:s platform:s location_text_raw:s address_text_raw:s? location_role:s old_province_id:s? old_district_id:s? old_ward_id:s? new_province_id:s? new_ward_id:s? latitude:n? longitude:n? resolution_precision:s resolution_status:s confidence:n? evidence_span:s span_start:i span_end:i extraction_method:s raw_admin_unit_ids:a interpretation_json:s source_reference:s?",
    ),
    "silver_geocoding_attempts": (
        "attempt_id",
        "candidate x address x reference version",
        "attempt_id:s candidate_id:s query:s? address_hash:s provider:s geocoding_provider:s candidate_admin_unit_ids:a precision:s geocoding_precision:s resolution_status:s coordinate_source:s? latitude:n? longitude:n? geocoded_at:t source_reference:s? dependency_status:s",
    ),
    "silver_social_intent_evidence": (
        "evidence_id",
        "content x intent x explicit span x optional mention",
        "evidence_id:s content_id:s intent_type:s participant_count_mentioned:i? requested_duration_minutes:i? requested_start_time:t? requested_time_precision:s requested_time_text:s? price_budget_min_vnd:n? price_budget_max_vnd:n? intent_location_id:s? evidence_span:s span_start:i span_end:i confidence:n? extraction_method:s actual_booking_confirmed:b",
    ),
    "dim_admin_unit": (
        "admin_unit_id",
        "administrative unit x version",
        "admin_unit_id:s admin_code:s? admin_name:s admin_level:s admin_version:s parent_admin_unit_id:s? geometry:s? valid_from:d? valid_to:d? source_reference:s reference_status:s",
    ),
    "bridge_admin_crosswalk": (
        "old_admin_unit_id new_admin_unit_id",
        "old x new unit mapping",
        "old_admin_unit_id:s new_admin_unit_id:s relation_type:s mapping_method:s overlap_area_ratio:n? mapping_confidence:n? source_reference:s",
    ),
    "dim_location_alias": (
        "alias_id",
        "alias x unit x effective period",
        "alias_id:s alias_text:s alias_type:s admin_unit_id:s effective_from:d? effective_to:d? source_reference:s",
    ),
    "court_supply_by_admin_version_v2": (
        "snapshot_date admin_version admin_unit_id",
        "location x admin version x snapshot",
        "snapshot_date:d admin_version:s admin_unit_id:s venue_count:i known_court_count:i? known_count_venue_count:i unknown_count_venue_count:i conflicting_count_venue_count:i court_count_coverage_rate:n? verified_venue_count:i probable_venue_count:i price_min_vnd:n? price_median_vnd:n? priced_venue_count:i price_unit:s? venue_last_observed_at:t? unverified_candidate_count:i",
    ),
    "social_engagement_daily_v1": (
        "activity_date admin_version admin_unit_id platform",
        "location x version x platform x VN publication date",
        "activity_date:d admin_version:s admin_unit_id:s platform:s post_count:i total_views:i? total_likes:i? total_comments_reported:i? total_shares:i? avg_likes_per_post:n? avg_comments_per_post:n? median_engagement:n? high_engagement_post_count:i? posts_with_metrics_count:i posts_with_views:i posts_with_likes:i posts_with_comments:i posts_with_shares:i snapshot_as_of:t? metric_window:s high_engagement_threshold:n?",
    ),
    "social_intent_daily_v2": (
        "activity_date admin_unit_id admin_version platform intent_type",
        "location x version x platform x VN publication date x label",
        "activity_date:d admin_unit_id:s admin_version:s platform:s intent_type:s post_count:i comment_count:i reply_count:i evidence_count:i resolved_content_count:i high_confidence_count:i? unique_content_count:i",
    ),
    "social_comment_analytics_daily_v1": (
        "activity_date platform admin_unit_id admin_version",
        "comment geography x version x platform x VN comment date",
        "activity_date:d platform:s admin_unit_id:s admin_version:s collected_comment_count:i collected_reply_count:i reported_comment_count:i? relevant_comment_count:i positive_comment_count:i? negative_comment_count:i? neutral_comment_count:i? sentiment_classified_count:i court_seeking_comment_count:i booking_intent_comment_count:i price_inquiry_comment_count:i capacity_complaint_count:i comments_with_resolved_geo:i court_seeking_comment_share:n? source_collection_status:s",
    ),
    "social_activity_by_admin_version_daily": (
        "activity_date admin_version admin_unit_id",
        "location x version x VN publication date",
        "activity_date:d admin_version:s admin_unit_id:s post_count:i comment_count:i reply_count:i unique_content_count:i relevant_post_count:i relevant_comment_count:i classified_content_count:i classification_coverage_rate:n? geographic_interpretation:s",
    ),
    "national_location_coverage_v1": (
        "admin_unit_id admin_version source_platform period_start period_end",
        "location x version x source x observed collection day",
        "admin_unit_id:s admin_version:s source_platform:s period_start:d period_end:d planned_queries:i? successful_queries:i? failed_queries:i? records_returned:i? records_accepted:i? resolved_records:i? last_success_at:t? collection_status:s query_success_rate:n? geo_resolution_rate:n? query_evidence_status:s collection_run_ids:a",
    ),
    "venue_community_feedback_weekly_v1": (
        "venue_id week_start",
        "canonical venue x VN publication week",
        "venue_id:s week_start:d mentioning_post_count:i mentioning_comment_count:i positive_feedback_count:i? negative_feedback_count:i? price_complaint_count:i? facility_complaint_count:i? booking_difficulty_count:i service_complaint_count:i? source_count:i classified_evidence_count:i sentiment_coverage_rate:n?",
    ),
    "location_term_usage_monthly_v1": (
        "month location_term_raw_normalized canonical_location_id term_admin_version platform",
        "location x raw term x platform x month",
        "month:d location_term_raw_normalized:s canonical_location_id:s term_admin_version:s platform:s post_mention_count:i comment_mention_count:i unique_content_count:i term_usage_share:n? resolution_coverage:n? mention_count:i",
    ),
    "venue_discovery_trends_v1": (
        "admin_unit_id admin_version month",
        "location x version x first-observed month",
        "month:d admin_unit_id:s admin_version:s newly_discovered_venue_count:i verified_discovery_count:i probable_discovery_count:i unverified_candidate_count:i first_seen_min:t? first_seen_max:t? discovery_basis:s",
    ),
    "location_market_research_monthly_v1": (
        "month admin_unit_id admin_version",
        "location x version x VN publication month",
        "month:d admin_unit_id:s admin_version:s venue_count:i? known_court_count:i? court_count_coverage_rate:n? relevant_post_count:i relevant_comment_count:i total_views:i? total_likes:i? total_shares:i? court_seeking_post_count:i court_seeking_comment_count:i booking_intent_content_count:i capacity_complaint_count:i newly_discovered_venue_count:i? source_count:i social_coverage_status:s supply_coverage_status:s supply_as_of:d? classification_coverage_rate:n?",
    ),
}
SILVER = tuple(list(SPECS)[:13])
GOLD = tuple(list(SPECS)[13:])
KEYS = {name: tuple(spec[0].split()) for name, spec in SPECS.items()}


def contract(name):
    properties = {}
    common = GOLD_COMMON if name in GOLD else COMMON
    for token in (SPECS[name][2] + " " + common).split():
        field, code = token.split(":")
        nullable = code.endswith("?")
        code = code.rstrip("?")
        kind = {
            "s": "string",
            "i": "integer",
            "n": "number",
            "b": "boolean",
            "a": "array",
            "t": "string",
            "d": "string",
        }[code]
        node = {"type": [kind, "null"] if nullable else kind}
        if code == "a":
            node["items"] = {"type": "string"}
        if code in ("t", "d"):
            node["format"] = "date-time" if code == "t" else "date"
        if field.endswith("_vnd") and code == "n":
            node["x-dsp-decimal"] = [20, 2]
        if code == "i" and not field.startswith("delta_"):
            node["minimum"] = 0
        if field in ("latitude", "longitude"):
            bound = 90 if field == "latitude" else 180
            node.update(minimum=-bound, maximum=bound)
        if field.endswith(
            ("confidence", "confidence_score", "coverage_rate")
        ) or field in (
            "confidence",
            "term_usage_share",
            "resolution_coverage",
            "query_success_rate",
            "geo_resolution_rate",
            "overlap_area_ratio",
        ):
            node.update(minimum=0, maximum=1)
        if field == "schema_version":
            node["const"] = "3" if name in SILVER else "1"
        properties[field] = node
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": name,
        "description": "Grain: "
        + SPECS[name][1]
        + ". See docs/data/silver_gold_metric_definitions.md for lineage, denominators and completeness.",
        "type": "object",
        "additionalProperties": False,
        "required": list(properties),
        "properties": properties,
    }
