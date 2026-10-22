-- Read-only EDA. Counts describe collected evidence, not the population.
SELECT platform, sum(post_count) AS relevant_posts,
       sum(posts_with_views) AS posts_with_views, sum(posts_with_likes) AS posts_with_likes,
       sum(posts_with_metrics_count) AS posts_with_any_metric
FROM dsp391m.social_engagement_daily_v1
WHERE admin_version = 'NATIONAL' AND admin_unit_id = 'NATIONAL'
GROUP BY platform ORDER BY relevant_posts DESC;
