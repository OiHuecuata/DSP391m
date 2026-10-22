-- Read-only EDA. Counts describe collected evidence, not the population.
SELECT month, admin_unit_id, newly_discovered_venue_count,
       verified_discovery_count, probable_discovery_count, unverified_candidate_count, discovery_basis
FROM dsp391m.venue_discovery_trends_v1
WHERE admin_version = 'VN_ADMIN_POST_2025'
ORDER BY month, newly_discovered_venue_count DESC, admin_unit_id;
