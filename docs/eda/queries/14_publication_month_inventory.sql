-- Read-only EDA. Counts describe collected evidence, not the population.
SELECT toStartOfMonth(activity_date) AS publication_month,
       sum(post_count) AS posts, sum(comment_count) AS comments, sum(reply_count) AS replies,
       sum(unique_content_count) AS dated_content
FROM dsp391m.social_activity_by_admin_version_daily
WHERE admin_version = 'NATIONAL' AND admin_unit_id = 'NATIONAL'
GROUP BY publication_month ORDER BY publication_month;
