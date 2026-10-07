package clickhouse

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/username/DSP391m/apps/backend/internal/config"
	"github.com/username/DSP391m/apps/backend/internal/domain"
)

func repository(t *testing.T, handler http.HandlerFunc) *MarketSignalRepository {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	u, _ := url.Parse(server.URL)
	port, _ := strconv.Atoi(u.Port())
	repo, err := NewMarketSignalRepository(config.ClickHouse{Host: u.Hostname(), Port: port, Database: "dsp391m", User: "reader", Password: "fixture", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return repo
}

func TestMappingAndBoundDate(t *testing.T) {
	repo := repository(t, func(w http.ResponseWriter, r *http.Request) {
		user, password, _ := r.BasicAuth()
		if user != "reader" || password != "fixture" {
			t.Error("auth not supplied")
		}
		if r.URL.Query().Get("param_snapshot") != "2026-08-13" {
			t.Error("date not bound")
		}
		if r.URL.Query().Get("output_format_json_quote_64bit_integers") != "0" {
			t.Error("integer setting missing")
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), "{snapshot:Date32}") {
			t.Error("missing typed parameter")
		}
		io.WriteString(w, `{"schema_version":"1","snapshot_date":"2026-08-13","location_id":"vn-hanoi","location_name":"Hà Nội","social_post_count":9007199254740993,"venue_count":2,"known_court_count":null,"venues_with_known_court_count":0,"venues_without_known_court_count":2,"court_count_coverage_ratio":0,"has_social_signal":false,"has_supply_signal":true,"social_activity_per_venue":null,"social_engagement_per_venue":null,"gold_generated_at":"2026-10-06T04:18:12.123456Z"}`)
	})
	rows, err := repo.BySnapshot(context.Background(), "2026-08-13")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].SocialPostCount != 9007199254740993 || rows[0].KnownCourtCount != nil || rows[0].SocialActivityPerVenue != nil || *rows[0].CourtCountCoverageRatio != 0 || *rows[0].VenueCount != 2 || !rows[0].HasSupplySignal || rows[0].HasSocialSignal || rows[0].GoldGeneratedAt != "2026-10-06T04:18:12.123456Z" {
		t.Fatalf("bad typed mapping: %+v", rows)
	}
}

func TestLatestSingleQueryAndEmpty(t *testing.T) {
	repo := repository(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), "SELECT max(snapshot_date)") {
			t.Error("latest not selected in single query")
		}
	})
	rows, err := repo.Latest(context.Background())
	if err != nil || len(rows) != 0 {
		t.Fatalf("empty result: %v %v", rows, err)
	}
}

func TestQueryErrorsAndNoPartialRows(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		body   string
	}{
		{"server", 500, "failure"}, {"malformed", 200, "{bad"},
		{"partial", 200, "{\"snapshot_date\":\"2026-08-13\"}\n{bad"},
		{"null-to-number", 200, "{\"venue_count\":\"unknown\"}"},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo := repository(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(test.status); io.WriteString(w, test.body) })
			rows, err := repo.Latest(context.Background())
			if err == nil || rows != nil {
				t.Fatalf("error should discard partial rows: %v %v", rows, err)
			}
		})
	}
}

func TestCancellationAndInvalidDate(t *testing.T) {
	repo := repository(t, func(w http.ResponseWriter, r *http.Request) { t.Error("should not query") })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := repo.Latest(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if _, err := repo.BySnapshot(context.Background(), "2026-02-30"); !errors.Is(err, domain.ErrInvalidSnapshotDate) {
		t.Fatalf("date validation: %v", err)
	}
}
