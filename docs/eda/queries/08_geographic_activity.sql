-- Read-only EDA. Counts describe collected evidence, not the population.
SELECT admin_unit_id, sum(relevant_post_count) AS relevant_posts,
       sum(relevant_comment_count) AS relevant_comments_and_replies,
       sum(unique_content_count) AS dated_content_memberships,
       min(activity_date) AS first_publication_date, max(activity_date) AS last_publication_date
FROM dsp391m.social_activity_by_admin_version_daily
WHERE admin_version = 'VN_ADMIN_POST_2025'
GROUP BY admin_unit_id ORDER BY relevant_posts DESC, admin_unit_id;
