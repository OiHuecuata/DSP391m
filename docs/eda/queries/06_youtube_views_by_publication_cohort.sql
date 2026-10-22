-- Read-only EDA. Counts describe collected evidence, not the population.
SELECT activity_date, sum(post_count) AS relevant_videos,
       sumOrNull(total_views) AS latest_observed_views, sum(posts_with_views) AS videos_with_views,
       sumOrNull(total_views) / nullIf(sum(posts_with_views), 0) AS available_views_per_video,
       min(snapshot_as_of) AS oldest_group_measurement, max(snapshot_as_of) AS newest_group_measurement
FROM dsp391m.social_engagement_daily_v1
WHERE admin_version = 'NATIONAL' AND admin_unit_id = 'NATIONAL' AND platform = 'youtube'
GROUP BY activity_date ORDER BY activity_date;
