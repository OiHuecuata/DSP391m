-- Read-only EDA. Counts describe collected evidence, not the population.
SELECT platform, intent_type, sum(post_count) AS labeled_posts,
       sum(comment_count) AS labeled_comments, sum(reply_count) AS labeled_replies,
       sum(unique_content_count) AS content_with_label
FROM dsp391m.social_intent_daily_v2
WHERE admin_version = 'NATIONAL' AND admin_unit_id = 'NATIONAL'
GROUP BY platform, intent_type ORDER BY content_with_label DESC, platform;
