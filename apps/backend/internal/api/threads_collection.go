package api

import (
	"encoding/json"
	"errors"
	"github.com/username/DSP391m/apps/backend/internal/services/dataops"
	"net/http"
)

func ThreadsCollectionConfig(manager *dataops.Manager) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			value, err := manager.ThreadsConfig()
			if err != nil {
				writeJSON(w, 503, map[string]string{"error": "Threads configuration unavailable"})
				return
			}
			writeJSON(w, 200, value)
			return
		}
		if r.Method != http.MethodPatch {
			w.Header().Set("Allow", "GET, PATCH")
			w.WriteHeader(405)
			return
		}
		var value dataops.ThreadsAcquisitionConfig
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16384))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&value) != nil {
			writeJSON(w, 400, map[string]string{"error": "Invalid Threads acquisition configuration"})
			return
		}
		if err := manager.ConfigureThreads(value); err != nil {
			status := 400
			if errors.Is(err, dataops.ErrConflict) {
				status = 409
			}
			writeJSON(w, status, map[string]string{"error": "Threads configuration rejected"})
			return
		}
		writeJSON(w, 200, value)
	})
}
