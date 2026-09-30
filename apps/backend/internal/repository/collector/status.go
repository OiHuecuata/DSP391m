package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

var ErrStatusNotFound = errors.New("collector status file not found")

// QueryStatus mirrors the query-level status written by the collector runtime.
type QueryStatus struct {
	Query          string  `json:"query"`
	Kind           string  `json:"kind"`
	GeoName        *string `json:"geo_name"`
	Status         string  `json:"status"`
	RawCount       *int    `json:"raw_count"`
	AcceptedCount  *int    `json:"accepted_count"`
	TotalCollected *int    `json:"total_collected"`
	LandedURL      *string `json:"landed_url"`
	Error          *string `json:"error"`
}

// Metadata mirrors optional collector runtime metadata.
type Metadata struct {
	PerQueryCap int  `json:"per_query_cap"`
	Headless    bool `json:"headless"`
}

// CrawlerStatus mirrors apps/collector/.local/<source>/status.json.
//
// Keep this repository model aligned with the collector runtime status schema.
// Mapping to the backend's system domain model belongs in the service layer.
type CrawlerStatus struct {
	SchemaVersion string `json:"schema_version"`
	Source        string `json:"source"`
	RunID         string `json:"run_id"`
	Status        string `json:"status"`
	PID           int    `json:"pid"`

	SeedQuery string `json:"seed_query"`
	Sort      string `json:"sort"`

	TargetCount    int `json:"target_count"`
	CollectedCount int `json:"collected_count"`

	QueryPlanCount   int `json:"query_plan_count"`
	QueriesProcessed int `json:"queries_processed"`
	QueriesSucceeded int `json:"queries_succeeded"`
	QueriesFailed    int `json:"queries_failed"`

	RawCount int `json:"raw_count"`

	CurrentQuery *QueryStatus `json:"current_query"`
	LastQuery    *QueryStatus `json:"last_query"`

	StartedAt  *time.Time `json:"started_at"`
	UpdatedAt  *time.Time `json:"updated_at"`
	FinishedAt *time.Time `json:"finished_at"`

	OutputPath *string `json:"output_path"`
	Error      *string `json:"error"`

	Metadata Metadata `json:"metadata"`
}

// StatusRepository reads the latest status snapshot for one collector source.
type StatusRepository struct {
	statusPath string
}

// NewStatusRepository creates a repository for one collector status file.
//
// Example path:
//
//	apps/collector/.local/threads/status.json
func NewStatusRepository(statusPath string) *StatusRepository {
	return &StatusRepository{
		statusPath: filepath.Clean(statusPath),
	}
}

// Get reads and parses the latest crawler status snapshot.
func (r *StatusRepository) Get(ctx context.Context) (CrawlerStatus, error) {
	if err := ctx.Err(); err != nil {
		return CrawlerStatus{}, err
	}

	data, err := os.ReadFile(r.statusPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return CrawlerStatus{}, fmt.Errorf(
				"%w: %s",
				ErrStatusNotFound,
				r.statusPath,
			)
		}

		return CrawlerStatus{}, fmt.Errorf(
			"read collector status %q: %w",
			r.statusPath,
			err,
		)
	}

	var status CrawlerStatus
	if err := json.Unmarshal(data, &status); err != nil {
		return CrawlerStatus{}, fmt.Errorf(
			"decode collector status %q: %w",
			r.statusPath,
			err,
		)
	}

	return status, nil
}
