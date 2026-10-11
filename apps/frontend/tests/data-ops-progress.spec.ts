import { test, expect } from "@playwright/test";
import { activeSource, continuousLabel, countdown, retainSurvey, threadsSurvey } from "../src/features/data-ops/progress";
import type { Collection, Survey } from "../src/features/data-ops/types";

const coverage = (completed_queries = 188): Survey => ({ survey_id: "survey", plan_id: "plan", planner_version: "3", completed_queries, candidate_queries: 117528, remaining_queries: 117528 - completed_queries, updated_at: "2026-10-11T00:00:00Z" });
function collection(): Collection {
  const s = { enabled: true, configured: true, interval_seconds: 43200, status: "PARTIAL", queued: false };
  return { continuous: true, status: "IDLE", sources: { threads: { ...s, survey: coverage(), next_run_at: "2026-10-11T12:00:00Z" }, youtube: { ...s }, pbplus: { ...s } }, runs: [] };
}
function running(): Collection {
  const c = collection();
  c.active_run_id = "batch";
  c.sources.threads.last_run_id = "batch";
  c.sources.threads.status = "RUNNING";
  c.runs = [{ run_id: "batch", source: "threads", status: "RUNNING", operation: "auto", started_at: "2026-10-11T00:00:00Z", duration_seconds: 0 }];
  c.progress = { batch: { requests_used: 0, max_requests: 32, queries_executed: 0 }, survey: coverage() };
  return c;
}

test("new batch resets navigation only and preserves 188 confirmed queries", () => {
  const c = running();
  expect(c.progress?.batch?.requests_used).toBe(0);
  expect(threadsSurvey(c)?.completed_queries).toBe(188);
});
test("authoritative coverage advances 188 to 220 to 252 without addition", () => {
  let previous = running();
  for (const completed of [188, 220, 252, 252]) {
    const next = running(); next.progress!.survey = coverage(completed);
    previous = retainSurvey(next, previous);
    expect(threadsSurvey(previous)?.completed_queries).toBe(completed);
  }
});
test("query attempts and navigation requests have independent units", () => {
  const c = running(); c.progress!.batch = { queries_executed: 12, requests_used: 25, max_requests: 32 };
  expect(c.progress?.batch?.queries_executed).toBe(12);
  expect(c.progress?.batch?.requests_used).toBe(25);
  expect(c.progress?.batch?.max_queries).toBeUndefined();
  expect(threadsSurvey(c)?.completed_queries).toBe(188);
});
test("continuous enabled IDLE means WAITING and carries the next timestamp", () => {
  const c = collection(); expect(continuousLabel(c)).toBe("CONTINUOUS ENABLED — WAITING");
  expect(c.sources.threads.next_run_at).toBe("2026-10-11T12:00:00Z");
  expect(countdown(c.sources.threads.next_run_at, Date.parse("2026-10-11T11:59:00Z"))).toBe("0h 1m 0s");
});
test("processing replay ignores saved discovery counters even in stale progress files", () => {
  const c = running(); c.runs[0].operation = "processing_replay"; c.progress!.survey = coverage(999);
  expect(threadsSurvey(c)?.completed_queries).toBe(188);
});
test("recovery keeps the original run identity and confirmed coverage", () => {
  const c = running(); c.runs[0].operation = "interrupted_recovery"; c.sources.threads.status = "RECOVERING";
  expect(activeSource(c)).toBe("threads"); expect(c.active_run_id).toBe("batch");
  expect(continuousLabel(c, false, "threads")).toBe("RECOVERING");
  expect(threadsSurvey(c)?.completed_queries).toBe(188);
});
test("missing coverage uses a compatible prior snapshot or unavailable", () => {
  const next = running(); delete next.sources.threads.survey; delete next.progress!.survey;
  expect(threadsSurvey(next)).toBeUndefined();
  expect(threadsSurvey(retainSurvey(next, running()))?.completed_queries).toBe(188);
});
test("browser refresh restores coverage entirely from serialized API state", () => {
  const restored = JSON.parse(JSON.stringify(collection())) as Collection;
  expect(threadsSurvey(restored)?.completed_queries).toBe(188);
});
test("older live progress cannot override a newer persisted backend snapshot", () => {
  const c = running(); c.sources.threads.survey = { ...coverage(252), updated_at: "2026-10-11T00:03:00Z" };
  expect(threadsSurvey(c)?.completed_queries).toBe(252);
});
test("plan transition replaces coverage and does not combine counts", () => {
  const next = running(); next.progress!.survey = { ...coverage(12), plan_id: "new-plan" };
  const merged = retainSurvey(next, running());
  expect(threadsSurvey(merged)?.completed_queries).toBe(12);
  expect(threadsSurvey(merged)?.plan_transition).toBe(true);
});
test("identity change without counts cannot reuse incompatible coverage", () => {
  const next = running(); next.progress!.survey = { survey_id: "new-survey", plan_id: "new-plan" };
  const result = threadsSurvey(retainSurvey(next, running()));
  expect(result?.completed_queries).toBeUndefined();
  expect(result?.plan_transition).toBe(true);
});
test("PARTIAL batch keeps survey progress while waiting", () => {
  const c = collection(); expect(threadsSurvey(c)?.completed_queries).toBe(188);
  expect(continuousLabel(c, false, "threads")).toContain("WAITING");
});
test("source switch leaves Threads coverage in its own card", () => {
  const c = running(); c.runs[0].source = "youtube"; c.progress!.survey = coverage(999);
  expect(activeSource(c)).toBe("youtube"); expect(threadsSurvey(c)?.completed_queries).toBe(188);
  expect(continuousLabel(c, false, "threads")).toContain("WAITING");
});
test("unavailable, disabled, stopping and failed controller presentation", () => {
  const c = running(); expect(continuousLabel(c, true)).toBe("UNAVAILABLE");
  c.continuous = false; expect(continuousLabel(c)).toBe("CONTINUOUS DISABLED");
  c.status = "STOPPING"; expect(continuousLabel(c)).toBe("STOPPING");
  c.status = "FAILED"; expect(continuousLabel(c)).toBe("FAILED");
});
