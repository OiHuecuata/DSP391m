-- Read-only EDA. Counts describe collected evidence, not the population.
SELECT period_start, source_platform, records_accepted, resolved_records, geo_resolution_rate,
       collection_status, planned_queries, query_success_rate
FROM dsp391m.national_location_coverage_v1
WHERE admin_version = 'NATIONAL' AND admin_unit_id = 'NATIONAL'
ORDER BY period_start, source_platform;
