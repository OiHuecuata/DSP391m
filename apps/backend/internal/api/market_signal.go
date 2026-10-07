package api

import (
	"errors"
	"net/http"

	"github.com/username/DSP391m/apps/backend/internal/domain"
	"github.com/username/DSP391m/apps/backend/internal/services/analytics"
)

type MarketSignalHandler struct {
	service *analytics.MarketSignalService
}

func NewMarketSignalHandler(service *analytics.MarketSignalService) *MarketSignalHandler {
	return &MarketSignalHandler{service: service}
}

// ServeHTTP handles /api/v1/analytics/location-market-signals? snapshot_date=YYYY-MM-DD.
// Omitting snapshot_date requests the latest currently served snapshot.
func (h *MarketSignalHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	query := r.URL.Query()
	date := query.Get("snapshot_date")
	if dates, present := query["snapshot_date"]; present && (len(dates) != 1 || date == "") {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": domain.ErrInvalidSnapshotDate.Error()})
		return
	}
	snapshot, err := h.service.Get(r.Context(), date)
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrInvalidSnapshotDate):
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		case errors.Is(err, domain.ErrSnapshotNotFound):
			writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
		default:
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "failed to get market signal snapshot"})
		}
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}
