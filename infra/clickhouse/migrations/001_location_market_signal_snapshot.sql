CREATE DATABASE IF NOT EXISTS {database} ENGINE = Atomic;

CREATE TABLE IF NOT EXISTS {database}.location_market_signal_snapshot
(
    schema_version String,
    snapshot_date Date32,
    window_start_date Date32,
    window_end_date Date32,
    window_days Int64,
    location_id String,
    location_name String,
    social_post_count Int64,
    social_like_count Int64,
    social_reply_count Int64,
    social_repost_count Int64,
    social_quote_count Int64,
    social_engagement_total Int64,
    venue_count Nullable(Int64),
    known_court_count Nullable(Int64),
    venues_with_known_court_count Nullable(Int64),
    venues_without_known_court_count Nullable(Int64),
    court_count_coverage_ratio Nullable(Float64),
    has_social_signal Bool,
    has_supply_signal Bool,
    social_activity_per_venue Nullable(Float64),
    social_engagement_per_venue Nullable(Float64),
    gold_generated_at DateTime64(6, 'UTC')
)
ENGINE = MergeTree
ORDER BY (snapshot_date, location_id, window_start_date, window_end_date, window_days, has_social_signal, has_supply_signal);
