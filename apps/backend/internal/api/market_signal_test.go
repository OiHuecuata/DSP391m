package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/username/DSP391m/apps/backend/internal/domain"
	"github.com/username/DSP391m/apps/backend/internal/services/analytics"
)

type marketReader struct {
	rows  []domain.MarketSignal
	err   error
	date  string
	calls int
}

func (r *marketReader) Latest(ctx context.Context) ([]domain.MarketSignal, error) {
	r.calls++
	return r.rows, r.err
}
func (r *marketReader) BySnapshot(ctx context.Context, date string) ([]domain.MarketSignal, error) {
	r.date = date
	return r.Latest(ctx)
}

func TestMarketSignalServiceAPI(t *testing.T) {
	reader := &marketReader{rows: []domain.MarketSignal{{SnapshotDate: "2026-08-13", LocationID: "vn-hanoi"}}}
	handler := NewMarketSignalHandler(analytics.NewMarketSignalService(reader))
	for _, path := range []string{"/", "/?snapshot_date=2026-08-13"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != 200 {
			t.Fatal(response.Body.String())
		}
		var payload map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
			t.Fatal(err)
		}
		row := payload["rows"].([]any)[0].(map[string]any)
		if value, present := row["known_court_count"]; !present || value != nil {
			t.Error("null count omitted or changed")
		}
		if value, present := row["social_activity_per_venue"]; !present || value != nil {
			t.Error("null ratio omitted or changed")
		}
	}
	if reader.date != "2026-08-13" {
		t.Error("date not passed through service")
	}
}

func TestMarketSignalErrors(t *testing.T) {
	for _, test := range []struct {
		path, method string
		err          error
		status       int
		noQuery      bool
	}{
		{"/?snapshot_date=bad", "GET", nil, 400, true},
		{"/?snapshot_date=", "GET", nil, 400, true},
		{"/?snapshot_date=2026-02-30", "GET", nil, 400, true},
		{"/?snapshot_date=2026-08-13&snapshot_date=2026-08-14", "GET", nil, 400, true},
		{"/", "POST", nil, 405, true},
		{"/", "GET", nil, 404, false},
		{"/", "GET", errors.New("private database error"), 500, false},
	} {
		t.Run(test.path+test.method, func(t *testing.T) {
			reader := &marketReader{err: test.err}
			response := httptest.NewRecorder()
			NewMarketSignalHandler(analytics.NewMarketSignalService(reader)).ServeHTTP(response, httptest.NewRequest(test.method, test.path, nil))
			if response.Code != test.status {
				t.Fatalf("status %d: %s", response.Code, response.Body.String())
			}
			if test.noQuery && reader.calls != 0 {
				t.Error("queried for invalid request")
			}
		})
	}
}
