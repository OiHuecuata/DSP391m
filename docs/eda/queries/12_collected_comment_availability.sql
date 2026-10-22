-- Read-only EDA. Counts describe collected evidence, not the population.
SELECT platform, sum(collected_comment_count) AS collected_comments,
       sum(collected_reply_count) AS collected_replies, sum(relevant_comment_count) AS relevant_comments_and_replies,
       sum(sentiment_classified_count) AS sentiment_classified,
       sumOrNull(positive_comment_count) AS positive_classified,
       sumOrNull(negative_comment_count) AS negative_classified,
       sumOrNull(reported_comment_count) AS unallocatable_reported_comments
FROM dsp391m.social_comment_analytics_daily_v1
WHERE admin_version = 'NATIONAL' AND admin_unit_id = 'NATIONAL'
GROUP BY platform ORDER BY platform;
