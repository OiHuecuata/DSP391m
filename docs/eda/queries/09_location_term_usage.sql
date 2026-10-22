-- Read-only EDA. Counts describe collected evidence, not the population.
SELECT month, canonical_location_id, platform, location_term_raw_normalized, term_admin_version,
       mention_count, unique_content_count, term_usage_share, resolution_coverage
FROM dsp391m.location_term_usage_monthly_v1
WHERE canonical_location_id != 'UNRESOLVED'
ORDER BY month, canonical_location_id, platform, mention_count DESC;
