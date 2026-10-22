-- Read-only EDA. Counts describe collected evidence, not the population.
WITH (SELECT max(snapshot_date) FROM dsp391m.court_supply_by_admin_version_v2) AS latest
SELECT admin_unit_id, snapshot_date, venue_count, verified_venue_count, probable_venue_count
FROM dsp391m.court_supply_by_admin_version_v2
WHERE snapshot_date = latest AND admin_version = 'VN_ADMIN_POST_2025'
ORDER BY venue_count DESC, admin_unit_id;
