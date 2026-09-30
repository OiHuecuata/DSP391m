package system

import (
	"context"
	"errors"

	"github.com/username/DSP391m/apps/backend/internal/domain"
	collectorrepo "github.com/username/DSP391m/apps/backend/internal/repository/collector"
)

// CrawlerStatusReader is the minimal dependency required by the system status
// service. The collector repository satisfies this interface.
type CrawlerStatusReader interface {
	Get(ctx context.Context) (collectorrepo.CrawlerStatus, error)
}

// StatusService aggregates status for the components shown on the system
// dashboard.
type StatusService struct {
	crawlerStatus CrawlerStatusReader
}

// NewStatusService creates the system status service.
func NewStatusService(crawlerStatus CrawlerStatusReader) *StatusService {
	return &StatusService{
		crawlerStatus: crawlerStatus,
	}
}

// Get returns the current system status snapshot.
//
// The crawler status is read from the collector runtime snapshot. Data Lake
// and Data Warehouse remain explicit "not_configured" placeholders until
// real health checks are implemented.
func (s *StatusService) Get(ctx context.Context) (domain.SystemStatus, error) {
	if err := ctx.Err(); err != nil {
		return domain.SystemStatus{}, err
	}

	crawler := s.getCrawlerStatus(ctx)

	if err := ctx.Err(); err != nil {
		return domain.SystemStatus{}, err
	}

	return domain.SystemStatus{
		Components: []domain.ComponentStatus{
			crawler,
			{
				ID:     "data-lake",
				Type:   domain.ComponentTypeDataLake,
				Status: domain.StateNotConfigured,
			},
			{
				ID:     "clickhouse",
				Type:   domain.ComponentTypeDataWarehouse,
				Status: domain.StateNotConfigured,
			},
		},
	}, nil
}

func (s *StatusService) getCrawlerStatus(ctx context.Context) domain.ComponentStatus {
	status, err := s.crawlerStatus.Get(ctx)
	if err != nil {
		state := domain.StateUnavailable
		message := "collector status unavailable"

		if errors.Is(err, collectorrepo.ErrStatusNotFound) {
			state = domain.StateIdle
			message = "collector has not produced a status snapshot yet"
		}

		return domain.ComponentStatus{
			ID:     "threads",
			Type:   domain.ComponentTypeCrawler,
			Status: state,
			Details: map[string]any{
				"message": message,
				"error":   err.Error(),
			},
		}
	}

	details := map[string]any{
		"seed_query":        status.SeedQuery,
		"sort":              status.Sort,
		"target_count":      status.TargetCount,
		"collected_count":   status.CollectedCount,
		"query_plan_count":  status.QueryPlanCount,
		"queries_processed": status.QueriesProcessed,
		"queries_succeeded": status.QueriesSucceeded,
		"queries_failed":    status.QueriesFailed,
		"raw_count":         status.RawCount,
		"started_at":        status.StartedAt,
		"finished_at":       status.FinishedAt,
		"error":             status.Error,
	}

	if status.CurrentQuery != nil {
		details["current_query"] = mapQueryStatus(status.CurrentQuery)
	} else {
		details["current_query"] = nil
	}

	if status.LastQuery != nil {
		details["last_query"] = mapQueryStatus(status.LastQuery)
	} else {
		details["last_query"] = nil
	}

	return domain.ComponentStatus{
		ID:        status.Source,
		Type:      domain.ComponentTypeCrawler,
		Status:    mapCrawlerState(status.Status),
		UpdatedAt: status.UpdatedAt,
		Details:   details,
	}
}

func mapCrawlerState(status string) domain.ComponentState {
	switch status {
	case "idle":
		return domain.StateIdle
	case "running":
		return domain.StateRunning
	case "success":
		return domain.StateSuccess
	case "failed":
		return domain.StateFailed
	default:
		return domain.StateUnavailable
	}
}

func mapQueryStatus(status *collectorrepo.QueryStatus) map[string]any {
	return map[string]any{
		"query":           status.Query,
		"kind":            status.Kind,
		"geo_name":        status.GeoName,
		"status":          status.Status,
		"raw_count":       status.RawCount,
		"accepted_count":  status.AcceptedCount,
		"total_collected": status.TotalCollected,
		"error":           status.Error,
	}
}
