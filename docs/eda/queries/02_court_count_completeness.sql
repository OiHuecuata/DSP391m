-- Read-only EDA. Counts describe collected evidence, not the population.
WITH (SELECT max(snapshot_date) FROM dsp391m.court_supply_by_admin_version_v2) AS latest
SELECT admin_unit_id, snapshot_date, known_count_venue_count, unknown_count_venue_count,
       conflicting_count_venue_count, venue_count,
       toFloat64(known_count_venue_count) / nullIf(venue_count, 0) AS known_count_share
FROM dsp391m.court_supply_by_admin_version_v2
WHERE snapshot_date = latest AND admin_version = 'VN_ADMIN_POST_2025'
ORDER BY known_count_share, venue_count DESC;
