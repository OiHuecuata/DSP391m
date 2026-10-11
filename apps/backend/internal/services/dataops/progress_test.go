package dataops

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func confirmedSurvey(completed int, plan string) map[string]any {
	return map[string]any{"survey_id": "survey", "plan_id": plan, "planner_version": "3", "candidate_queries": 117528, "completed_queries": completed, "remaining_queries": 117528 - completed, "deferred_queries": 0, "retry_pending_queries": 0, "updated_at": fmt.Sprintf("2026-10-11T00:%02d:00Z", completed/32)}
}

func writeProgress(t *testing.T, m *Manager, id string, survey map[string]any) {
	t.Helper()
	path := filepath.Join(m.options.Directory, "runs", id)
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(map[string]any{"run_id": id, "batch": map[string]any{"queries_executed": 0, "requests_used": 0, "max_requests": 32}, "survey": survey})
	if err := os.WriteFile(filepath.Join(path, "progress.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestSurveyPersistsAcrossBatchesAndMissingProgress(t *testing.T) {
	m := newTestManager(t, true, "PARTIAL", "0")
	m.mu.Lock()
	m.state.Sources["threads"].Metrics = map[string]any{"discovery": confirmedSurvey(188, "plan")}
	m.mu.Unlock()
	for _, n := range []int{188, 220, 252} {
		id := fmt.Sprintf("batch-%d", n)
		m.mu.Lock()
		m.state.Active = id
		m.state.Runs = append([]*Run{{ID: id, Source: "threads", Status: "RUNNING", Started: time.Now()}}, m.state.Runs...)
		m.state.Sources["threads"].LastRun = id
		m.mu.Unlock()
		// A new progress file is not yet available. Keep confirmed coverage.
		before := m.Snapshot().Sources["threads"].Survey["completed_queries"]
		if before == nil {
			t.Fatal("coverage disappeared at batch start")
		}
		writeProgress(t, m, id, confirmedSurvey(n, "plan"))
		s := m.Snapshot()
		if s.Sources["threads"].Survey["completed_queries"] != float64(n) {
			t.Fatal(s)
		}
		if s.Progress["batch"].(map[string]any)["requests_used"] != float64(0) {
			t.Fatal("batch counter not independent")
		}
		// Repeated status snapshots must never add local attempts to coverage.
		if m.Snapshot().Sources["threads"].Survey["completed_queries"] != float64(n) {
			t.Fatal("double counted")
		}
		m.mu.Lock()
		m.state.Active = ""
		m.mu.Unlock()
		if m.Snapshot().Sources["threads"].Survey["completed_queries"] != float64(n) {
			t.Fatal("waiting reset")
		}
	}
}

func TestSurveyReplaySourceIsolationAndPlanTransition(t *testing.T) {
	m := newTestManager(t, true, "PARTIAL", "0")
	m.mu.Lock()
	m.setSurveyLocked(confirmedSurvey(252, "old"))
	m.state.Active = "replay"
	m.state.Runs = []*Run{{ID: "replay", Source: "threads", Status: "RUNNING", Operation: "processing_replay"}}
	m.mu.Unlock()
	writeProgress(t, m, "replay", confirmedSurvey(999, "old"))
	if m.Snapshot().Sources["threads"].Survey["completed_queries"] != float64(252) {
		t.Fatal("replay changed coverage")
	}
	m.mu.Lock()
	m.state.Runs[0].Source = "youtube"
	m.state.Runs[0].Operation = ""
	m.mu.Unlock()
	if m.Snapshot().Sources["threads"].Survey["completed_queries"] != float64(252) {
		t.Fatal("source switch changed coverage")
	}
	m.mu.Lock()
	m.state.Active = ""
	m.setSurveyLocked(confirmedSurvey(12, "new"))
	m.mu.Unlock()
	s := m.Snapshot().Sources["threads"].Survey
	if s["completed_queries"] != float64(12) || s["plan_transition"] != true {
		t.Fatal(s)
	}
}

func TestFinishedPartialBatchAndCoverageRestoreOnRestart(t *testing.T) {
	m := newTestManager(t, true, "PARTIAL", "0")
	id := "finished"
	writeProgress(t, m, id, confirmedSurvey(252, "plan"))
	path := filepath.Join(m.options.Directory, "runs", id, "summary.json")
	if err := os.WriteFile(path, []byte(`{"status":"PARTIAL","metrics":{"accepted":0,"discovered":0,"stopping_reason":"request_budget"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	r := &Run{ID: id, Source: "threads", Started: time.Now()}
	m.state.Runs = []*Run{r}
	m.finishLocked(r, "")
	m.saveLocked()
	m.mu.Unlock()
	b, err := os.ReadFile(filepath.Join(m.options.Directory, "control.json"))
	if err != nil {
		t.Fatal(err)
	}
	// Decode exactly the persisted contract used by a fresh manager/browser.
	var restored State
	if err := json.Unmarshal(b, &restored); err != nil {
		t.Fatal(err)
	}
	s := restored.Sources["threads"]
	if s.Survey["completed_queries"] != float64(252) || s.LastBatch.Status != "PARTIAL" || s.LastBatch.Metrics["stopping_reason"] != "request_budget" {
		t.Fatal(s)
	}
	restoredDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(restoredDir, "control.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewManager(Options{Directory: restoredDir})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if restarted.Snapshot().Sources["threads"].Survey["completed_queries"] != float64(252) {
		t.Fatal("manager restart lost coverage")
	}
}

func TestLegacyReplayRestoresLastAcquisitionCoverage(t *testing.T) {
	m := newTestManager(t, true, "PARTIAL", "0")
	m.mu.Lock()
	completed := time.Now().UTC()
	m.state.Sources["threads"].Metrics = map[string]any{"operation": "processing_replay", "discovery": confirmedSurvey(12, "plan")}
	m.state.Runs = []*Run{{ID: "acquisition", Source: "threads", Completed: &completed, Metrics: map[string]any{"discovery": confirmedSurvey(252, "plan")}}}
	m.mu.Unlock()
	if m.Snapshot().Sources["threads"].Survey["completed_queries"] != float64(252) {
		t.Fatal("legacy replay masked last acquisition")
	}
}

func TestMissingAndOutdatedCoverageNeverBecomesZero(t *testing.T) {
	m := newTestManager(t, true, "PARTIAL", "0")
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setSurveyLocked(confirmedSurvey(252, "plan"))
	m.setSurveyLocked(surveyFromMetrics(map[string]any{"discovery": map[string]any{"executed_queries": 32}}, nil))
	m.setSurveyLocked(map[string]any{"survey_id": "survey", "plan_id": "plan"})
	m.setSurveyLocked(map[string]any{"completed_queries": 0})
	m.setSurveyLocked(confirmedSurvey(188, "plan"))
	if m.state.Sources["threads"].Survey["completed_queries"] != 252 {
		t.Fatal("missing/older coverage replaced latest snapshot")
	}
}
