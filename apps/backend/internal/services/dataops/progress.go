package dataops

import (
	"encoding/json"
	"time"
)

// surveyFromMetrics uses published coverage only, never per-attempt query counts.
func surveyFromMetrics(metrics map[string]any, completed *time.Time) map[string]any {
	if metrics["operation"] == "processing_replay" {
		return nil
	}
	d, ok := metrics["discovery"].(map[string]any)
	if !ok || d["completed_queries"] == nil {
		return nil
	}
	survey := map[string]any{}
	for _, key := range []string{"survey_id", "plan_id", "planner_version", "candidate_queries", "completed_queries", "remaining_queries", "eligible_queries", "deferred_queries", "retry_pending_queries", "has_more", "updated_at"} {
		if d[key] != nil {
			survey[key] = d[key]
		}
	}
	if survey["candidate_queries"] == nil {
		survey["candidate_queries"] = d["planned_queries"]
	}
	if survey["updated_at"] == nil && completed != nil {
		survey["updated_at"] = completed.UTC().Format(time.RFC3339Nano)
	}
	return survey
}

func (m *Manager) projectSurveyLocked() bool {
	s := m.state.Sources["threads"]
	if s == nil {
		return false
	}
	var next map[string]any
	if s.Survey == nil {
		next = surveyFromMetrics(s.Metrics, s.Completed)
		if next == nil {
			// Legacy controllers may have most recently replayed an older batch.
			// Restore coverage from saved acquisition summaries, never replay counts.
			for _, r := range m.state.Runs {
				if r.Source == "threads" && r.Completed != nil && r.Operation != "processing_replay" {
					if next = surveyFromMetrics(r.Metrics, r.Completed); next != nil {
						break
					}
				}
			}
		}
	}
	if r := m.findRun(m.state.Active); r != nil && r.Source == "threads" && r.Operation != "processing_replay" {
		if survey, ok := m.state.Progress["survey"].(map[string]any); ok && survey["completed_queries"] != nil {
			next = survey
		}
	}
	if next == nil {
		return false
	}
	return m.setSurveyLocked(next)
}

func (m *Manager) setSurveyLocked(next map[string]any) bool {
	if next == nil || next["completed_queries"] == nil {
		return false
	}
	s := m.state.Sources["threads"]
	old := s.Survey
	// An incomplete identity cannot replace a known survey with unscoped counts.
	if old != nil && ((old["survey_id"] != nil && next["survey_id"] == nil) || (old["plan_id"] != nil && next["plan_id"] == nil)) {
		return false
	}
	if old != nil && old["survey_id"] == next["survey_id"] && old["plan_id"] == next["plan_id"] {
		oldTime, _ := time.Parse(time.RFC3339Nano, stringValue(old["updated_at"]))
		nextTime, _ := time.Parse(time.RFC3339Nano, stringValue(next["updated_at"]))
		if !oldTime.IsZero() && (nextTime.IsZero() || nextTime.Before(oldTime)) {
			return false
		}
		if old["plan_transition"] == true {
			next["plan_transition"] = true
		}
	} else if old != nil {
		next["plan_transition"] = true
	}
	a, _ := json.Marshal(old)
	b, _ := json.Marshal(next)
	if string(a) == string(b) {
		return false
	}
	s.Survey = next
	return true
}

func stringValue(value any) string {
	s, _ := value.(string)
	return s
}
