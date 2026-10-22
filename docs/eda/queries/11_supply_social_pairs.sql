-- Read-only EDA. Counts describe collected evidence, not the population.
SELECT month, admin_unit_id, supply_as_of, venue_count, known_court_count,
       court_count_coverage_rate, relevant_post_count, relevant_comment_count,
       court_seeking_post_count, court_seeking_comment_count, booking_intent_content_count,
       social_coverage_status, supply_coverage_status
FROM dsp391m.location_market_research_monthly_v1
WHERE admin_version = 'VN_ADMIN_POST_2025' AND supply_coverage_status = 'OBSERVED_AS_OF_SNAPSHOT'
      AND social_coverage_status = 'OBSERVED_SAMPLE_PARTIAL' AND venue_count IS NOT NULL
ORDER BY month, venue_count DESC;
