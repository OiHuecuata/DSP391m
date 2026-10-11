package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/username/DSP391m/apps/backend/internal/services/dataops"
)

type DataOpsHandler struct {
	Manager *dataops.Manager
	Status  *dataops.StatusService
}

func (h *DataOpsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/")
	if r.Method == http.MethodGet {
		switch path {
		case "data-ops/status":
			writeJSON(w, 200, h.Status.Status(r.Context()))
		case "data-ops/lakehouse":
			l, _ := h.Status.Infrastructure(r.Context())
			writeJSON(w, 200, l)
		case "data-ops/warehouse":
			_, v := h.Status.Infrastructure(r.Context())
			writeJSON(w, 200, v)
		case "data-ops/runs":
			writeJSON(w, 200, h.Manager.Snapshot().Runs)
		case "collection/status":
			writeJSON(w, 200, h.Manager.Snapshot())
		default:
			http.NotFound(w, r)
		}
		return
	}
	if r.Method != http.MethodPost && r.Method != http.MethodPatch {
		w.Header().Set("Allow", "GET, POST, PATCH")
		writeJSON(w, 405, map[string]string{"error": "method not allowed"})
		return
	}
	var err error
	if r.Method == http.MethodPatch && path != "collection/config" {
		writeJSON(w, 405, map[string]string{"error": "PATCH is supported for collection/config only"})
		return
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16384))
	decoder.DisallowUnknownFields()
	switch path {
	case "collection/config":
		if r.Method != http.MethodPatch {
			writeJSON(w, 405, map[string]string{"error": "PATCH required"})
			return
		}
		var body struct {
			Sources map[string]dataops.SourceConfig `json:"sources"`
		}
		if decoder.Decode(&body) != nil || len(body.Sources) == 0 {
			writeJSON(w, 400, map[string]string{"error": "sources configuration required"})
			return
		}
		err = h.Manager.Configure(body.Sources)
	case "collection/retry", "collection/replay", "collection/recover", "collection/continue":
		var body struct {
			RunID string `json:"run_id"`
		}
		if decoder.Decode(&body) != nil {
			writeJSON(w, 400, map[string]string{"error": "run_id required"})
			return
		}
		switch path {
		case "collection/recover":
			err = h.Manager.Recover(body.RunID)
		case "collection/continue":
			err = h.Manager.Continue(body.RunID)
		default:
			err = h.Manager.Replay(body.RunID)
		}
	case "collection/start":
		var body struct {
			Sources map[string]dataops.SourceConfig `json:"sources"`
		}
		if decoder.Decode(&body) != nil || len(body.Sources) == 0 {
			writeJSON(w, 400, map[string]string{"error": "sources configuration required"})
			return
		}
		err = h.Manager.Start(body.Sources)
	case "collection/stop":
		err = h.Manager.Stop()
	case "collection/run":
		var body struct {
			Sources []string `json:"sources"`
		}
		if decoder.Decode(&body) != nil {
			writeJSON(w, 400, map[string]string{"error": "sources required"})
			return
		}
		err = h.Manager.RunNow(body.Sources)
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		code := 400
		if errors.Is(err, dataops.ErrConflict) {
			code = 409
		}
		writeJSON(w, code, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 202, h.Manager.Snapshot())
}
