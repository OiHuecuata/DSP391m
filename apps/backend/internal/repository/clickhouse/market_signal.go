package clickhouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/username/DSP391m/apps/backend/internal/config"
	"github.com/username/DSP391m/apps/backend/internal/domain"
)

const table = "location_market_signal_snapshot"
const columns = `schema_version, snapshot_date, window_start_date, window_end_date, window_days,
location_id, location_name, social_post_count, social_like_count, social_reply_count,
social_repost_count, social_quote_count, social_engagement_total, venue_count,
known_court_count, venues_with_known_court_count, venues_without_known_court_count,
court_count_coverage_ratio, has_social_signal, has_supply_signal, social_activity_per_venue,
social_engagement_per_venue,
formatDateTime(gold_generated_at, '%Y-%m-%dT%H:%i:%S.%fZ', 'UTC') AS gold_generated_at`

// MarketSignalRepository uses ClickHouse's HTTP typed JSONEachRow transport.
// SQL and parameter binding stay here; no extra driver or connection pool is needed.
type MarketSignalRepository struct {
	settings config.ClickHouse
	http     *http.Client
}

func NewMarketSignalRepository(settings config.ClickHouse) (*MarketSignalRepository, error) {
	if err := settings.Validate(); err != nil {
		return nil, err
	}
	return &MarketSignalRepository{settings: settings, http: &http.Client{Timeout: settings.Timeout}}, nil
}

func (r *MarketSignalRepository) Latest(ctx context.Context) ([]domain.MarketSignal, error) {
	qualified := r.settings.Database + "." + table
	// One statement observes one table generation even during atomic publication.
	return r.query(ctx, "SELECT "+columns+" FROM "+qualified+
		" WHERE snapshot_date = (SELECT max(snapshot_date) FROM "+qualified+") ORDER BY location_id", nil)
}

func (r *MarketSignalRepository) BySnapshot(ctx context.Context, date string) ([]domain.MarketSignal, error) {
	if parsed, err := time.Parse("2006-01-02", date); err != nil || parsed.Format("2006-01-02") != date {
		return nil, domain.ErrInvalidSnapshotDate
	}
	return r.query(ctx, "SELECT "+columns+" FROM "+r.settings.Database+"."+table+
		" WHERE snapshot_date = {snapshot:Date32} ORDER BY location_id", url.Values{"param_snapshot": {date}})
}

func (r *MarketSignalRepository) query(ctx context.Context, sql string, params url.Values) ([]domain.MarketSignal, error) {
	ctx, cancel := context.WithTimeout(ctx, r.settings.Timeout)
	defer cancel()
	if params == nil {
		params = url.Values{}
	}
	params.Set("wait_end_of_query", "1")
	params.Set("output_format_json_quote_64bit_integers", "0")
	params.Set("max_execution_time", fmt.Sprintf("%d", max(1, int(r.settings.Timeout.Seconds()))))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.settings.URL()+"/?"+params.Encode(), strings.NewReader(sql+" FORMAT JSONEachRow"))
	if err != nil {
		return nil, fmt.Errorf("create market signal query: %w", err)
	}
	req.SetBasicAuth(r.settings.User, r.settings.Password)
	response, err := r.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query market signals: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ClickHouse market signal query returned HTTP %d", response.StatusCode)
	}
	decoder := json.NewDecoder(response.Body)
	decoder.DisallowUnknownFields()
	rows := make([]domain.MarketSignal, 0)
	for {
		var row domain.MarketSignal
		if err := decoder.Decode(&row); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode ClickHouse market signal row: %w", err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}
