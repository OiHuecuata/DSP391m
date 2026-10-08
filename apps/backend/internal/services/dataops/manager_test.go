package dataops

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/username/DSP391m/apps/backend/internal/config"
)

func newTestManager(t *testing.T, configured bool, status string, delay string) *Manager {
	t.Helper()
	dir := t.TempDir()
	m, err := NewManager(Options{Directory: dir, YouTubeConfigured: configured, Runner: func(source, id string) (*exec.Cmd, error) {
		path := filepath.Join(dir, "runs", id)
		if err := os.MkdirAll(path, 0700); err != nil {
			return nil, err
		}
		summary := fmt.Sprintf(`{"status":%q,"metrics":{"accepted":2,"new_identities":1},"stages":{"kafka":{"result":{"published_count":2}}}}`, status)
		return exec.Command("sh", "-c", `sleep "$1"; printf '%s' "$2" > "$3"`, "worker", delay, summary, filepath.Join(path, "summary.json")), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	return m
}
func await(t *testing.T, check func() bool) {
	t.Helper()
	end := time.Now().Add(4 * time.Second)
	for time.Now().Before(end) {
		if check() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition timed out")
}
func selection(name string, enabled bool) map[string]SourceConfig {
	return map[string]SourceConfig{"threads": {Enabled: name == "threads" && enabled, Interval: 43200}, "youtube": {Enabled: name == "youtube" && enabled, Interval: 21600}, "pbplus": {Enabled: name == "pbplus" && enabled, Interval: 86400}}
}

func TestStartReturnsImmediatelyAndSchedules(t *testing.T) {
	m := newTestManager(t, true, "SUCCESS", "0.1")
	start := time.Now()
	if err := m.Start(selection("threads", true)); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 90*time.Millisecond {
		t.Fatal("Start waited on worker")
	}
	await(t, func() bool { return len(m.Snapshot().Runs) == 1 })
	await(t, func() bool { return m.Snapshot().Active == "" })
	s := m.Snapshot()
	if !s.Continuous || s.Sources["threads"].Next == nil || s.Runs[0].Status != "SUCCESS" {
		t.Fatalf("unexpected state %+v", s)
	}
	_ = m.Stop()
}
func TestStopAfterCurrentFinishesAndPreventsNext(t *testing.T) {
	m := newTestManager(t, true, "SUCCESS", "0.15")
	_ = m.Start(selection("threads", true))
	await(t, func() bool { return m.Snapshot().Active != "" })
	_ = m.Stop()
	if s := m.Snapshot(); s.Status != "STOPPING" || s.Continuous {
		t.Fatal(s)
	}
	await(t, func() bool { return m.Snapshot().Active == "" })
	m.tick()
	s := m.Snapshot()
	if len(s.Runs) != 1 || s.Runs[0].Status != "SUCCESS" || s.Sources["threads"].Next != nil {
		t.Fatal(s)
	}
}
func TestRunNowWithoutContinuous(t *testing.T) {
	m := newTestManager(t, true, "SUCCESS", "0.03")
	if err := m.RunNow([]string{"youtube"}); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return len(m.Snapshot().Runs) == 1 && m.Snapshot().Active == "" })
	s := m.Snapshot()
	if s.Continuous || s.Sources["youtube"].Next != nil {
		t.Fatal(s)
	}
}
func TestDuplicateRunningAndQueuedRejected(t *testing.T) {
	m := newTestManager(t, true, "SUCCESS", "0.15")
	_ = m.RunNow([]string{"threads"})
	if !errors.Is(m.RunNow([]string{"threads"}), ErrConflict) {
		t.Fatal("duplicate accepted")
	}
	await(t, func() bool { return m.Snapshot().Active != "" })
	if !errors.Is(m.RunNow([]string{"threads"}), ErrConflict) {
		t.Fatal("overlap accepted")
	}
}
func TestDifferentSourcesSerialize(t *testing.T) {
	m := newTestManager(t, true, "SUCCESS", "0.05")
	_ = m.RunNow([]string{"threads", "youtube", "pbplus"})
	await(t, func() bool { return len(m.Snapshot().Runs) == 3 && m.Snapshot().Active == "" })
	s := m.Snapshot()
	for i := 0; i < 2; i++ {
		if s.Runs[i].Started.Before(*s.Runs[i+1].Completed) {
			t.Fatal("source cycles overlapped")
		}
	}
}
func TestTwoScheduledObservationsHaveDistinctRuns(t *testing.T) {
	m := newTestManager(t, true, "SUCCESS", "0.02")
	_ = m.Start(selection("threads", true))
	await(t, func() bool { return len(m.Snapshot().Runs) == 1 && m.Snapshot().Active == "" })
	m.mu.Lock()
	past := time.Now().Add(-time.Second)
	m.state.Sources["threads"].Next = &past
	m.mu.Unlock()
	m.tick()
	await(t, func() bool { return len(m.Snapshot().Runs) == 2 && m.Snapshot().Active == "" })
	_ = m.Stop()
	s := m.Snapshot()
	if s.Runs[0].ID == s.Runs[1].ID {
		t.Fatal("run identity reused")
	}
}
func TestPartialAndFailedStatesAndIndependentSource(t *testing.T) {
	for _, status := range []string{"PARTIAL", "FAILED"} {
		t.Run(status, func(t *testing.T) {
			m := newTestManager(t, true, status, "0.01")
			_ = m.RunNow([]string{"threads"})
			await(t, func() bool { return len(m.Snapshot().Runs) == 1 && m.Snapshot().Active == "" })
			if m.Snapshot().Sources["threads"].Status != status {
				t.Fatal(m.Snapshot())
			}
			if err := m.RunNow([]string{"youtube"}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestYouTubeConfigurationGate(t *testing.T) {
	for _, configured := range []bool{false, true} {
		t.Run(fmt.Sprint(configured), func(t *testing.T) {
			m := newTestManager(t, configured, "SUCCESS", "0.01")
			err := m.Start(selection("youtube", true))
			if configured != (err == nil) {
				t.Fatalf("configured=%v error=%v", configured, err)
			}
			if !configured && m.RunNow([]string{"youtube"}) == nil {
				t.Fatal("unconfigured source ran")
			}
			_ = m.Stop()
		})
	}
}
func TestInvalidInputDoesNotQueueSource(t *testing.T) {
	m := newTestManager(t, true, "SUCCESS", "0.01")
	for _, names := range [][]string{nil, {"unknown"}, {"threads", "unknown"}} {
		if m.RunNow(names) == nil {
			t.Fatal("invalid source accepted")
		}
	}
	for _, interval := range []int{0, 9, 604801} {
		if m.Start(map[string]SourceConfig{"threads": {true, interval}}) == nil {
			t.Fatal("invalid interval accepted")
		}
	}
	if m.Snapshot().Sources["threads"].Pending {
		t.Fatal("partial queue mutation")
	}
}
func TestPersistenceAndRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(Options{Directory: dir, YouTubeConfigured: true})
	if err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	now := time.Now()
	m.state.Continuous = true
	m.state.Sources["threads"].Enabled = false
	m.state.Sources["youtube"].Enabled = false
	m.state.Sources["pbplus"].Enabled = false
	m.state.Sources["threads"].LastRun = "interrupted"
	m.state.Sources["threads"].Status = "RUNNING"
	m.state.Active = "interrupted"
	m.state.Runs = []*Run{{ID: "interrupted", Source: "threads", Status: "RUNNING", Started: now, PID: 99999999}}
	m.saveLocked()
	m.state.Active = ""
	m.mu.Unlock()
	m.Close()
	recovered, err := NewManager(Options{Directory: dir, YouTubeConfigured: false})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	s := recovered.Snapshot()
	if s.Active != "" || s.Sources["threads"].Status == "RUNNING" || s.Runs[0].Status != "FAILED" || s.Sources["threads"].Enabled {
		t.Fatal(s)
	}
}
func TestControlLockRejectsSecondBackend(t *testing.T) {
	m := newTestManager(t, true, "SUCCESS", "0.01")
	if second, err := NewManager(m.options); err == nil {
		second.Close()
		t.Fatal("second controller accepted")
	}
}
func TestCorruptPersistentStateFailsClosed(t *testing.T) {
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, "control.json"), []byte("bad"), 0600)
	if m, err := NewManager(Options{Directory: dir}); err == nil {
		m.Close()
		t.Fatal("corrupt state ignored")
	}
}

func TestConfigurationPersistsWithoutStarting(t *testing.T) {
	m := newTestManager(t, true, "SUCCESS", "0.01")
	if err := m.Configure(selection("youtube", true)); err != nil {
		t.Fatal(err)
	}
	s := m.Snapshot()
	if s.Continuous || s.Sources["youtube"].Next != nil || len(s.Runs) != 0 {
		t.Fatal(s)
	}
	var persisted State
	b, _ := os.ReadFile(filepath.Join(m.options.Directory, "control.json"))
	_ = json.Unmarshal(b, &persisted)
	if !persisted.Sources["youtube"].Enabled || persisted.Sources["threads"].Enabled {
		t.Fatal(persisted)
	}
}

func TestRetrySavedArtifactsPreservesIdentity(t *testing.T) {
	m := newTestManager(t, true, "FAILED", "0.01")
	_ = m.RunNow([]string{"threads"})
	await(t, func() bool { return len(m.Snapshot().Runs) == 1 && m.Snapshot().Active == "" })
	id := m.Snapshot().Runs[0].ID
	if m.Retry(id) == nil {
		t.Fatal("retry without artifacts accepted")
	}
	_ = os.WriteFile(filepath.Join(m.options.Directory, "runs", id, "collection.json"), []byte(`{}`), 0600)
	if err := m.Retry(id); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return len(m.Snapshot().Runs) == 2 && m.Snapshot().Active == "" })
	s := m.Snapshot()
	if s.Runs[0].ID != id || s.Runs[0].Attempt != 2 || s.Runs[1].Status != "FAILED" {
		t.Fatal(s)
	}
}

func TestGracefulRestartRestoresDesiredSchedule(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(Options{Directory: dir, YouTubeConfigured: true})
	if err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.state.Continuous = true
	for _, s := range m.state.Sources {
		s.Enabled = false
	}
	s := m.state.Sources["threads"]
	s.Enabled = true
	s.Status = "SUCCESS"
	completed := time.Now().UTC()
	s.Completed = &completed
	s.Next = nil
	m.saveLocked()
	m.mu.Unlock()
	m.Close()
	restarted, err := NewManager(Options{Directory: dir, YouTubeConfigured: true})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	state := restarted.Snapshot()
	if !state.Continuous || state.Sources["threads"].Next == nil || state.Sources["threads"].Status == "RUNNING" {
		t.Fatal(state)
	}
}

func TestSurvivingWorkerRestartReconcilesCompletion(t *testing.T) {
	dir := t.TempDir()
	id := "recover-worker"
	path := filepath.Join(dir, "runs", id)
	_ = os.MkdirAll(path, 0700)
	cmd := exec.Command("sh", "-c", `sleep 0.3; printf '%s' '{"status":"SUCCESS","metrics":{"accepted":2}}' > "$1"`, "collection_worker.py", filepath.Join(path, "summary.json"))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Wait()
	now := time.Now().UTC()
	state := State{Sources: map[string]*Source{"threads": {Enabled: true, Configured: true, Interval: 43200, Status: "RUNNING"}}, Active: id, Runs: []*Run{{ID: id, Source: "threads", Status: "RUNNING", Started: now, PID: cmd.Process.Pid}}}
	b, _ := json.Marshal(state)
	_ = os.WriteFile(filepath.Join(dir, "control.json"), b, 0600)
	m, err := NewManager(Options{Directory: dir, YouTubeConfigured: true})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if m.Snapshot().Sources["threads"].Status != "RECOVERING" {
		t.Fatal(m.Snapshot())
	}
	await(t, func() bool { return m.Snapshot().Active == "" })
	if m.Snapshot().Runs[0].Status != "SUCCESS" {
		t.Fatal(m.Snapshot())
	}
}

func TestQuotaExhaustionBackoff(t *testing.T) {
	m := newTestManager(t, true, "FAILED", "0.01")
	now := time.Now().UTC()
	id := "quota"
	_ = os.MkdirAll(filepath.Join(m.options.Directory, "runs", id), 0700)
	_ = os.WriteFile(filepath.Join(m.options.Directory, "runs", id, "summary.json"), []byte(`{"status":"PARTIAL","metrics":{"stopping_reason":"quotaExceeded"}}`), 0600)
	m.mu.Lock()
	m.state.Continuous = true
	m.state.Sources["youtube"].Enabled = true
	m.state.Sources["youtube"].Interval = 10
	r := &Run{ID: id, Source: "youtube", Started: now}
	m.finishLocked(r, "")
	next := m.state.Sources["youtube"].Next
	m.mu.Unlock()
	if next == nil || next.Sub(now) < 24*time.Hour {
		t.Fatal("quota exhaustion retried too early")
	}
	_ = m.Stop()
}

func TestManualRunAfterStopIsRunning(t *testing.T) {
	m := newTestManager(t, true, "SUCCESS", "0.1")
	m.mu.Lock()
	m.state.StopRequested = true
	m.mu.Unlock()
	if err := m.RunNow([]string{"threads"}); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return m.Snapshot().Active != "" })
	if m.Snapshot().Status != "RUNNING" {
		t.Fatal(m.Snapshot())
	}
}

func TestInvalidLakeURIIsSanitized(t *testing.T) {
	m := newTestManager(t, true, "SUCCESS", "0.01")
	s := NewStatusService("file:///lake?credential=fixture-secret", config.ClickHouse{}, m)
	lake := s.inspectLake()
	b, _ := json.Marshal(lake)
	if strings.Contains(string(b), "fixture-secret") || lake.Status != "FAILED" {
		t.Fatalf("invalid URI leaked: %s", b)
	}
}
func TestLakehouseRealDeltaLogStatistics(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "silver/social_posts/_delta_log")
	_ = os.MkdirAll(log, 0700)
	_ = os.WriteFile(filepath.Join(log, "00000000000000000000.json"), []byte("{\"add\":{\"path\":\"a\",\"stats\":\"{\\\"numRecords\\\":3}\"}}\n"), 0600)
	_ = os.WriteFile(filepath.Join(log, "00000000000000000001.json"), []byte("{\"remove\":{\"path\":\"a\"}}\n{\"add\":{\"path\":\"b\",\"stats\":\"{\\\"numRecords\\\":5}\"}}\n"), 0600)
	result := inspectTable("silver/social_posts", (&urlForTest{dir}).uri()+"/silver/social_posts")
	if !result.Exists || result.Rows == nil || *result.Rows != 5 || *result.Version != 1 {
		t.Fatal(result)
	}
}

type urlForTest struct{ path string }

func (u *urlForTest) uri() string { return "file://" + u.path }
func TestMissingAndUnreadableLakehouse(t *testing.T) {
	m := newTestManager(t, true, "SUCCESS", "0.01")
	s := NewStatusService("file:///nonexistent/dsp-test", config.ClickHouse{}, m)
	lake := s.inspectLake()
	if lake.Status != "FAILED" || lake.Readable {
		t.Fatal(lake)
	}
	if inspectTable("missing", "file:///nonexistent").Exists {
		t.Fatal("missing Delta exists")
	}
}
func TestIncompleteDeltaLogDoesNotInventRowCount(t *testing.T) {
	dir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dir, "_delta_log"), 0700)
	_ = os.WriteFile(filepath.Join(dir, "_delta_log/00000000000000000005.json"), []byte(`{"add":{"path":"a","stats":"{\"numRecords\":3}"}}`), 0600)
	r := inspectTable("t", "file://"+dir)
	if !r.Exists || r.Rows != nil {
		t.Fatal(r)
	}
}
func TestWarehouseHealthAndSanitizedConfiguration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if user, password, ok := r.BasicAuth(); !ok || user != "user" || password != "fixture-secret" {
			t.Error("auth missing")
		}
		if strings.Contains(readBody(r), "system.tables") {
			fmt.Fprintln(w, `{"n":1}`)
		} else {
			fmt.Fprintln(w, `{"rows":8,"locations":8,"snapshot":"2026-10-07","generated":"2026-10-08 00:00:00"}`)
		}
	}))
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "http://")
	parts := strings.Split(host, ":")
	var port int
	fmt.Sscan(parts[1], &port)
	m := newTestManager(t, true, "SUCCESS", "0.01")
	s := NewStatusService("file://"+t.TempDir(), config.ClickHouse{Host: parts[0], Port: port, Database: "db", User: "user", Password: "fixture-secret"}, m)
	w := s.inspectWarehouse(t.Context())
	b, _ := json.Marshal(w)
	if w.Status != "HEALTHY" || *w.Rows != 8 || strings.Contains(string(b), "fixture-secret") {
		t.Fatalf("%s", b)
	}
}
func readBody(r *http.Request) string { b, _ := io.ReadAll(r.Body); return string(b) }
func TestWarehouseUnavailableAndMissingTable(t *testing.T) {
	for _, response := range []string{`{"n":0}`, "unavailable"} {
		t.Run(response, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if response == "unavailable" {
					w.WriteHeader(503)
				} else {
					fmt.Fprintln(w, response)
				}
			}))
			defer server.Close()
			parts := strings.Split(strings.TrimPrefix(server.URL, "http://"), ":")
			var port int
			fmt.Sscan(parts[1], &port)
			m := newTestManager(t, true, "SUCCESS", "0.01")
			s := NewStatusService("file://"+t.TempDir(), config.ClickHouse{Host: parts[0], Port: port, Database: "db"}, m)
			w := s.inspectWarehouse(t.Context())
			if w.Status == "HEALTHY" {
				t.Fatal(w)
			}
		})
	}
}

func TestStartUpdatesScheduledCadence(t *testing.T) {
	m := newTestManager(t, true, "SUCCESS", "0.01")
	_ = m.Start(selection("threads", true))
	await(t, func() bool { return len(m.Snapshot().Runs) == 1 && m.Snapshot().Active == "" })
	if err := m.Start(map[string]SourceConfig{"threads": {Enabled: true, Interval: 86400}}); err != nil {
		t.Fatal(err)
	}
	source := m.Snapshot().Sources["threads"]
	if source.Next.Sub(*source.Completed) != 24*time.Hour {
		t.Fatal("next run did not adopt configured cadence")
	}
	_ = m.Stop()
}
