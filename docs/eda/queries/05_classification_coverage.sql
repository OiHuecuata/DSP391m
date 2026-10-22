-- Read-only EDA. Counts describe collected evidence, not the population.
SELECT activity_date, sum(unique_content_count) AS collected_dated_content,
       sum(classified_content_count) AS relevance_classified_content,
       toFloat64(sum(classified_content_count)) / nullIf(sum(unique_content_count), 0) AS relevance_coverage,
       sum(relevant_post_count) AS relevant_posts, sum(relevant_comment_count) AS relevant_comments_and_replies
FROM dsp391m.social_activity_by_admin_version_daily
WHERE admin_version = 'NATIONAL' AND admin_unit_id = 'NATIONAL'
GROUP BY activity_date ORDER BY activity_date;
