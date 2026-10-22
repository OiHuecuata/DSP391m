-- Read-only EDA. Counts describe collected evidence, not the population.
SELECT period_start, source_platform, admin_unit_id, collection_status, query_evidence_status,
       successful_queries, failed_queries, records_accepted, last_success_at
FROM dsp391m.national_location_coverage_v1
WHERE admin_version = 'VN_ADMIN_POST_2025'
ORDER BY period_start, source_platform, admin_unit_id;
