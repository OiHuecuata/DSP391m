package api

import (
	"encoding/json"
	"net/http"

	systemservice "github.com/username/DSP391m/apps/backend/internal/services/system"
)

// StatusHandler serves the system status dashboard API.
type StatusHandler struct {
	service *systemservice.StatusService
}

// NewStatusHandler creates a system status HTTP handler.
func NewStatusHandler(service *systemservice.StatusService) *StatusHandler {
	return &StatusHandler{
		service: service,
	}
}

// ServeHTTP handles GET /api/v1/system/status.
func (h *StatusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	status, err := h.service.Get(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error": "failed to get system status",
		})
		return
	}

	writeJSON(w, http.StatusOK, status)
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(payload); err != nil {
		// Headers/status may already be written at this point, so there is
		// nothing useful left to send to the client.
		return
	}
}
