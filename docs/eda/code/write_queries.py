"""Write executable read-only ClickHouse chart queries and chart metadata."""
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[3]/'docs/eda'
QUERIES={
'01_supply_by_province': """WITH (SELECT max(snapshot_date) FROM dsp391m.court_supply_by_admin_version_v2) AS latest
SELECT admin_unit_id, snapshot_date, venue_count, verified_venue_count, probable_venue_count
FROM dsp391m.court_supply_by_admin_version_v2
WHERE snapshot_date = latest AND admin_version = 'VN_ADMIN_POST_2025'
ORDER BY venue_count DESC, admin_unit_id""",
'02_court_count_completeness': """WITH (SELECT max(snapshot_date) FROM dsp391m.court_supply_by_admin_version_v2) AS latest
SELECT admin_unit_id, snapshot_date, known_count_venue_count, unknown_count_venue_count,
       conflicting_count_venue_count, venue_count,
       toFloat64(known_count_venue_count) / nullIf(venue_count, 0) AS known_count_share
FROM dsp391m.court_supply_by_admin_version_v2
WHERE snapshot_date = latest AND admin_version = 'VN_ADMIN_POST_2025'
ORDER BY known_count_share, venue_count DESC""",
'03_collection_evidence_matrix': """SELECT period_start, source_platform, admin_unit_id, collection_status, query_evidence_status,
       successful_queries, failed_queries, records_accepted, last_success_at
FROM dsp391m.national_location_coverage_v1
WHERE admin_version = 'VN_ADMIN_POST_2025'
ORDER BY period_start, source_platform, admin_unit_id""",
'04_relevant_posts_by_platform': """SELECT platform, sum(post_count) AS relevant_posts,
       sum(posts_with_views) AS posts_with_views, sum(posts_with_likes) AS posts_with_likes,
       sum(posts_with_metrics_count) AS posts_with_any_metric
FROM dsp391m.social_engagement_daily_v1
WHERE admin_version = 'NATIONAL' AND admin_unit_id = 'NATIONAL'
GROUP BY platform ORDER BY relevant_posts DESC""",
'05_classification_coverage': """SELECT activity_date, sum(unique_content_count) AS collected_dated_content,
       sum(classified_content_count) AS relevance_classified_content,
       toFloat64(sum(classified_content_count)) / nullIf(sum(unique_content_count), 0) AS relevance_coverage,
       sum(relevant_post_count) AS relevant_posts, sum(relevant_comment_count) AS relevant_comments_and_replies
FROM dsp391m.social_activity_by_admin_version_daily
WHERE admin_version = 'NATIONAL' AND admin_unit_id = 'NATIONAL'
GROUP BY activity_date ORDER BY activity_date""",
'06_youtube_views_by_publication_cohort': """SELECT activity_date, sum(post_count) AS relevant_videos,
       sumOrNull(total_views) AS latest_observed_views, sum(posts_with_views) AS videos_with_views,
       sumOrNull(total_views) / nullIf(sum(posts_with_views), 0) AS available_views_per_video,
       min(snapshot_as_of) AS oldest_group_measurement, max(snapshot_as_of) AS newest_group_measurement
FROM dsp391m.social_engagement_daily_v1
WHERE admin_version = 'NATIONAL' AND admin_unit_id = 'NATIONAL' AND platform = 'youtube'
GROUP BY activity_date ORDER BY activity_date""",
'07_intents_by_platform': """SELECT platform, intent_type, sum(post_count) AS labeled_posts,
       sum(comment_count) AS labeled_comments, sum(reply_count) AS labeled_replies,
       sum(unique_content_count) AS content_with_label
FROM dsp391m.social_intent_daily_v2
WHERE admin_version = 'NATIONAL' AND admin_unit_id = 'NATIONAL'
GROUP BY platform, intent_type ORDER BY content_with_label DESC, platform""",
'08_geographic_activity': """SELECT admin_unit_id, sum(relevant_post_count) AS relevant_posts,
       sum(relevant_comment_count) AS relevant_comments_and_replies,
       sum(unique_content_count) AS dated_content_memberships,
       min(activity_date) AS first_publication_date, max(activity_date) AS last_publication_date
FROM dsp391m.social_activity_by_admin_version_daily
WHERE admin_version = 'VN_ADMIN_POST_2025'
GROUP BY admin_unit_id ORDER BY relevant_posts DESC, admin_unit_id""",
'09_location_term_usage': """SELECT month, canonical_location_id, platform, location_term_raw_normalized, term_admin_version,
       mention_count, unique_content_count, term_usage_share, resolution_coverage
FROM dsp391m.location_term_usage_monthly_v1
WHERE canonical_location_id != 'UNRESOLVED'
ORDER BY month, canonical_location_id, platform, mention_count DESC""",
'10_first_observed_venues': """SELECT month, admin_unit_id, newly_discovered_venue_count,
       verified_discovery_count, probable_discovery_count, unverified_candidate_count, discovery_basis
FROM dsp391m.venue_discovery_trends_v1
WHERE admin_version = 'VN_ADMIN_POST_2025'
ORDER BY month, newly_discovered_venue_count DESC, admin_unit_id""",
'11_supply_social_pairs': """SELECT month, admin_unit_id, supply_as_of, venue_count, known_court_count,
       court_count_coverage_rate, relevant_post_count, relevant_comment_count,
       court_seeking_post_count, court_seeking_comment_count, booking_intent_content_count,
       social_coverage_status, supply_coverage_status
FROM dsp391m.location_market_research_monthly_v1
WHERE admin_version = 'VN_ADMIN_POST_2025' AND supply_coverage_status = 'OBSERVED_AS_OF_SNAPSHOT'
      AND social_coverage_status = 'OBSERVED_SAMPLE_PARTIAL' AND venue_count IS NOT NULL
ORDER BY month, venue_count DESC""",
'12_collected_comment_availability': """SELECT platform, sum(collected_comment_count) AS collected_comments,
       sum(collected_reply_count) AS collected_replies, sum(relevant_comment_count) AS relevant_comments_and_replies,
       sum(sentiment_classified_count) AS sentiment_classified,
       sumOrNull(positive_comment_count) AS positive_classified,
       sumOrNull(negative_comment_count) AS negative_classified,
       sumOrNull(reported_comment_count) AS unallocatable_reported_comments
FROM dsp391m.social_comment_analytics_daily_v1
WHERE admin_version = 'NATIONAL' AND admin_unit_id = 'NATIONAL'
GROUP BY platform ORDER BY platform""",
'13_national_collection_geo_resolution': """SELECT period_start, source_platform, records_accepted, resolved_records, geo_resolution_rate,
       collection_status, planned_queries, query_success_rate
FROM dsp391m.national_location_coverage_v1
WHERE admin_version = 'NATIONAL' AND admin_unit_id = 'NATIONAL'
ORDER BY period_start, source_platform""",
'14_publication_month_inventory': """SELECT toStartOfMonth(activity_date) AS publication_month,
       sum(post_count) AS posts, sum(comment_count) AS comments, sum(reply_count) AS replies,
       sum(unique_content_count) AS dated_content
FROM dsp391m.social_activity_by_admin_version_daily
WHERE admin_version = 'NATIONAL' AND admin_unit_id = 'NATIONAL'
GROUP BY publication_month ORDER BY publication_month""",
}

def main():
    folder=ROOT/'queries'; folder.mkdir(parents=True,exist_ok=True)
    for n,sql in QUERIES.items():
        (folder/(n+'.sql')).write_text('-- Read-only EDA. Counts describe collected evidence, not the population.\n'+sql+';\n')
    print(f'Wrote {len(QUERIES)} analytical SQL queries.')

if __name__=='__main__':
    main()
