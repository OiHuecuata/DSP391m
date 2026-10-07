package analytics

import (
	"context"
	"time"

	"github.com/username/DSP391m/apps/backend/internal/domain"
)

type MarketSignalReader interface {
	Latest(context.Context) ([]domain.MarketSignal, error)
	BySnapshot(context.Context, string) ([]domain.MarketSignal, error)
}

type MarketSignalService struct{ repository MarketSignalReader }

func NewMarketSignalService(repository MarketSignalReader) *MarketSignalService {
	return &MarketSignalService{repository: repository}
}

// Get serves existing Gold computations; an empty date selects latest.
func (s *MarketSignalService) Get(ctx context.Context, date string) (domain.MarketSignalSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return domain.MarketSignalSnapshot{}, err
	}
	var rows []domain.MarketSignal
	var err error
	if date == "" {
		rows, err = s.repository.Latest(ctx)
	} else {
		parsed, parseErr := time.Parse("2006-01-02", date)
		if parseErr != nil || parsed.Format("2006-01-02") != date {
			return domain.MarketSignalSnapshot{}, domain.ErrInvalidSnapshotDate
		}
		rows, err = s.repository.BySnapshot(ctx, date)
	}
	if err != nil {
		return domain.MarketSignalSnapshot{}, err
	}
	if len(rows) == 0 {
		return domain.MarketSignalSnapshot{}, domain.ErrSnapshotNotFound
	}
	return domain.MarketSignalSnapshot{SnapshotDate: rows[0].SnapshotDate, Rows: rows}, nil
}
