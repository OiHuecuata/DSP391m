package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/username/DSP391m/apps/backend/internal/config"
	"github.com/username/DSP391m/apps/backend/internal/services/dataops"
)

func operations(t *testing.T, configured bool) *DataOpsHandler {
	t.Helper()
	dir := t.TempDir()
	m, err := dataops.NewManager(dataops.Options{Directory: dir, YouTubeConfigured: configured, Runner: func(string, string) (*exec.Cmd, error) { return exec.Command("sleep", "0.05"), nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	return &DataOpsHandler{Manager: m, Status: dataops.NewStatusService("file://"+filepath.Join(dir, "lake"), config.ClickHouse{Host: "127.0.0.1", Port: 1, Database: "dsp391m", User: "test", Password: "fixture-private"}, m)}
}
func call(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(method, "/api/v1/"+path, strings.NewReader(body)))
	return w
}
func TestOperationalGETEndpoints(t *testing.T) {
	h := operations(t, true)
	for _, path := range []string{"data-ops/status", "data-ops/lakehouse", "data-ops/warehouse", "data-ops/runs", "collection/status"} {
		t.Run(path, func(t *testing.T) {
			w := call(h, "GET", path, "")
			if w.Code != 200 || !json.Valid(w.Body.Bytes()) {
				t.Fatal(w.Code, w.Body.String())
			}
			if strings.Contains(w.Body.String(), "fixture-private") || strings.Contains(w.Body.String(), "YOUTUBE_API_KEY") {
				t.Fatal("secret exposed")
			}
		})
	}
}
func TestCollectionStartStopAPI(t *testing.T) {
	h := operations(t, true)
	start := call(h, "POST", "collection/start", `{"sources":{"threads":{"enabled":true,"interval_seconds":43200},"youtube":{"enabled":false,"interval_seconds":21600},"pbplus":{"enabled":false,"interval_seconds":86400}}}`)
	if start.Code != 202 {
		t.Fatal(start.Code, start.Body.String())
	}
	stop := call(h, "POST", "collection/stop", `{}`)
	if stop.Code != 202 || h.Manager.Snapshot().Continuous {
		t.Fatal(stop.Code)
	}
}
func TestRunNowDuplicateAPI(t *testing.T) {
	h := operations(t, true)
	if r := call(h, "POST", "collection/run", `{"sources":["threads"]}`); r.Code != 202 {
		t.Fatal(r.Code)
	}
	if r := call(h, "POST", "collection/run", `{"sources":["threads"]}`); r.Code != 409 {
		t.Fatal(r.Code)
	}
}
func TestYouTubeNotConfiguredAPI(t *testing.T) {
	h := operations(t, false)
	if r := call(h, "POST", "collection/run", `{"sources":["youtube"]}`); r.Code != 400 {
		t.Fatal(r.Code)
	}
	r := call(h, "GET", "collection/status", "")
	if !strings.Contains(r.Body.String(), `"configured":false`) {
		t.Fatal(r.Body.String())
	}
}
func TestYouTubeConfiguredAPI(t *testing.T) {
	h := operations(t, true)
	if r := call(h, "POST", "collection/run", `{"sources":["youtube"]}`); r.Code != 202 {
		t.Fatal(r.Code)
	}
}
func TestCollectionMalformedUnknownAndMethod(t *testing.T) {
	h := operations(t, true)
	for _, body := range []string{`{}`, `invalid`, `{"sources":["other"]}`, `{"sources":["threads"],"key":"forbidden"}`} {
		if r := call(h, "POST", "collection/run", body); r.Code != 400 {
			t.Fatal(r.Code, body)
		}
	}
	if r := call(h, "PUT", "collection/status", ""); r.Code != 405 {
		t.Fatal(r.Code)
	}
	if r := call(h, "GET", "unknown", ""); r.Code != 404 {
		t.Fatal(r.Code)
	}
}
