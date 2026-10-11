import type { Collection, Survey } from "./types";

export function activeSource(c: Collection) {
  if (!c.active_run_id) return undefined;
  return c.runs.find(r => r.run_id === c.active_run_id && !r.completed_at)?.source
    ?? (Object.keys(c.sources) as (keyof Collection["sources"])[]).find(id => c.sources[id].last_run_id === c.active_run_id);
}

export function continuousLabel(c: Collection, unavailable = false, source?: keyof Collection["sources"]) {
  if (unavailable) return "UNAVAILABLE";
  const s = source ? c.sources[source] : undefined;
  if (c.status === "FAILED") return "FAILED";
  if (c.status === "STOPPING" || (c.stop_requested && c.active_run_id)) return "STOPPING";
  if (s?.status === "RECOVERING" || (!source && Object.values(c.sources).some(s => s.status === "RECOVERING"))) return "RECOVERING";
  if (!c.continuous) return "CONTINUOUS DISABLED";
  if (s && (!s.enabled || !s.configured)) return "CONTINUOUS ENABLED · SOURCE DISABLED";
  const running = source ? activeSource(c) === source : !!c.active_run_id;
  return `CONTINUOUS ENABLED — ${running ? "RUNNING" : "WAITING"}`;
}

export function threadsSurvey(c: Collection): Survey | undefined {
  const operation = c.runs.find(r => r.run_id === c.active_run_id && !r.completed_at)?.operation ?? c.progress?.operation ?? c.progress?.metrics?.operation;
  if (activeSource(c) === "threads" && operation !== "processing_replay") {
    const live = c.progress?.survey ?? c.progress?.metrics?.discovery;
    // An identity change without coverage must not borrow counts from the old plan.
    if (live && (live.completed_queries != null || live.survey_id || live.plan_id)) {
      const saved = c.sources.threads.survey;
      const compatible = saved && (!live.survey_id || saved.survey_id === live.survey_id) && (!live.plan_id || saved.plan_id === live.plan_id);
      if (compatible && saved.updated_at && (!live.updated_at || Date.parse(saved.updated_at) > Date.parse(live.updated_at))) return saved;
      return compatible ? { ...saved, ...live } : live;
    }
  }
  const s = c.sources.threads;
  if (s.survey) return s.survey;
  const discovery = s.metrics?.discovery;
  if (discovery && s.metrics?.operation !== "processing_replay") return { ...discovery, candidate_queries: discovery.candidate_queries ?? discovery.planned_queries, updated_at: discovery.updated_at ?? s.last_completed_at };
}

// Keep only confirmed coverage across a temporarily incomplete response. Backend
// persistence supplies the same snapshot after reload; React is not its owner.
export function retainSurvey(next: Collection, previous?: Collection): Collection {
  const incoming = threadsSurvey(next);
  const old = previous && threadsSurvey(previous);
  const compatible = old && (!incoming?.survey_id || incoming.survey_id === old.survey_id) && (!incoming?.plan_id || incoming.plan_id === old.plan_id);
  const transition = incoming?.plan_transition || (compatible && old?.plan_transition) || !!(old && !compatible);
  const survey = incoming?.completed_queries != null ? { ...incoming, plan_transition: transition } : compatible ? old : incoming ? { ...incoming, plan_transition: transition } : undefined;
  return { ...next, sources: { ...next.sources, threads: { ...next.sources.threads, survey } } };
}

export function countdown(next: string | undefined, now: number) {
  if (!now) return "Updating…";
  if (!next || !Number.isFinite(Date.parse(next))) return "Unavailable";
  const seconds = Math.ceil((Date.parse(next) - now) / 1000);
  if (seconds <= 0) return "Due; awaiting controller";
  return `${Math.floor(seconds / 3600)}h ${Math.floor(seconds % 3600 / 60)}m ${seconds % 60}s`;
}
