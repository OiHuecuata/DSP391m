package domain

import "errors"

var ErrSnapshotNotFound = errors.New("market signal snapshot not found")
var ErrInvalidSnapshotDate = errors.New("snapshot date must be YYYY-MM-DD")

// MarketSignal mirrors Gold v1. Social fields describe text-mentioned-place
// activity; supply is known source evidence with explicitly nullable totals.
type MarketSignal struct {
	SchemaVersion                string   `json:"schema_version"`
	SnapshotDate                 string   `json:"snapshot_date"`
	WindowStartDate              string   `json:"window_start_date"`
	WindowEndDate                string   `json:"window_end_date"`
	WindowDays                   int64    `json:"window_days"`
	LocationID                   string   `json:"location_id"`
	LocationName                 string   `json:"location_name"`
	SocialPostCount              int64    `json:"social_post_count"`
	SocialLikeCount              int64    `json:"social_like_count"`
	SocialReplyCount             int64    `json:"social_reply_count"`
	SocialRepostCount            int64    `json:"social_repost_count"`
	SocialQuoteCount             int64    `json:"social_quote_count"`
	SocialEngagementTotal        int64    `json:"social_engagement_total"`
	VenueCount                   *int64   `json:"venue_count"`
	KnownCourtCount              *int64   `json:"known_court_count"`
	VenuesWithKnownCourtCount    *int64   `json:"venues_with_known_court_count"`
	VenuesWithoutKnownCourtCount *int64   `json:"venues_without_known_court_count"`
	CourtCountCoverageRatio      *float64 `json:"court_count_coverage_ratio"`
	HasSocialSignal              bool     `json:"has_social_signal"`
	HasSupplySignal              bool     `json:"has_supply_signal"`
	SocialActivityPerVenue       *float64 `json:"social_activity_per_venue"`
	SocialEngagementPerVenue     *float64 `json:"social_engagement_per_venue"`
	GoldGeneratedAt              string   `json:"gold_generated_at"`
}

type MarketSignalSnapshot struct {
	SnapshotDate string         `json:"snapshot_date"`
	Rows         []MarketSignal `json:"rows"`
}
