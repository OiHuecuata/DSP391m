import { activeSource, continuousLabel, countdown, threadsSurvey } from "./progress";
import type { Collection } from "./types";

function date(value?: string) { return value ? new Date(value).toLocaleString() : "Unavailable"; }
function number(value?: number) { return value == null ? "Unavailable" : value.toLocaleString("en-US"); }

export function ThreadsProgress({ collection: c, unavailable, now }: { collection: Collection; unavailable: boolean; now: number }) {
  const s = c.sources.threads;
  const active = activeSource(c) === "threads";
  const run = active ? c.runs.find(r => r.run_id === c.active_run_id && !r.completed_at) : undefined;
  const last = s.last_batch ?? c.runs.find(r => r.source === "threads" && r.completed_at);
  const p = active ? c.progress : undefined;
  const m = active ? p?.metrics : last?.metrics ?? s.metrics;
  const operation = run?.operation ?? p?.operation ?? m?.operation;
  const replay = operation === "processing_replay";
  const d = m?.discovery;
  const requests = replay ? 0 : p?.batch?.requests_used ?? m?.api_requests_this_attempt ?? d?.requests_this_invocation;
  const executed = replay ? 0 : p?.batch?.queries_executed ?? d?.executed_queries;
  const budget = p?.batch?.max_requests ?? d?.max_requests;
  const survey = threadsSurvey(c);
  const total = survey?.candidate_queries ?? (survey && "planned_queries" in survey ? survey.planned_queries as number : undefined);
  const completed = survey?.completed_queries;
  const percentage = completed != null && total != null && total > 0 ? completed / total * 100 : undefined;
  const status = active ? run?.status ?? s.status : last?.status ?? s.status;
  return <>
    <h3>Continuous status</h3>
    <p className="runtime-message">{continuousLabel(c, unavailable, "threads")}</p>
    <p className="muted">Source {s.enabled ? "enabled" : "disabled"} · Interval {s.interval_seconds.toLocaleString("en-US")} seconds</p>
    <h3>{active ? "Current batch" : "Latest batch"} — {status}</h3>
    <dl className="ops-details">
      <dt>Run ID</dt><dd>{active ? c.active_run_id : last?.run_id ?? s.last_run_id ?? "Unavailable"}</dd>
      <dt>Operation</dt><dd>{operation || "Unavailable"}</dd>
      <dt>Stage</dt><dd>{active ? p?.stage ?? "Awaiting progress" : s.last_completed_at ? "Finished" : "Unavailable"}</dd>
      <dt>Queries executed in batch</dt><dd>{number(executed)}{p?.batch?.max_queries != null && ` / ${number(p.batch.max_queries)}`}</dd>
      <dt>Navigation requests / budget</dt><dd>{number(requests)} / {number(budget)}</dd>
      <dt>Current query</dt><dd>{active ? p?.query ?? "Unavailable" : "—"}</dd>
      <dt>Discovered source records</dt><dd>{number(p?.discovered ?? m?.discovered)}</dd>
      <dt>Accepted records</dt><dd>{number(p?.accepted ?? m?.accepted)}</dd>
      <dt>Technical errors</dt><dd>{number(m?.acquisition?.technical_failures ?? p?.batch?.technical_errors)}</dd>
      <dt>Stopping reason</dt><dd>{m?.stopping_reason ?? "Unavailable"}</dd>
    </dl>
    {requests != null && budget != null && budget > 0 && <progress aria-label="Batch navigation requests" value={requests} max={budget} />}
    {replay && <p className="muted">Processing replay uses saved records; no new discovery completion is counted.</p>}
    <section aria-label="Threads Discovery — Entire Survey">
      <h3>Threads Discovery — Entire Survey</h3>
      {survey?.plan_transition && <p className="notice">Plan transition: showing coverage confirmed for the current plan.</p>}
      <dl className="ops-details">
        <dt>Survey ID</dt><dd>{survey?.survey_id ?? "Unavailable"}</dd>
        <dt>Plan ID / version</dt><dd>{survey?.plan_id ?? "Unavailable"} / {survey?.planner_version ?? "Unavailable"}</dd>
        <dt>Completed queries / unique candidates</dt><dd>{number(completed)} / {number(total)}</dd>
        <dt>Remaining queries / eligible now</dt><dd>{number(survey?.remaining_queries)} / {number(survey?.eligible_queries)}</dd>
        <dt>Deferred / retry pending</dt><dd>{number(survey?.deferred_queries)} / {number(survey?.retry_pending_queries)}</dd>
        <dt>Completion</dt><dd>{percentage == null ? "Unavailable" : `${percentage.toFixed(3)}%`}</dd>
        <dt>Survey status</dt><dd>{completed == null || total == null ? "UNAVAILABLE" : completed >= total ? "COMPLETE" : "IN PROGRESS"}</dd>
        <dt>Last confirmed update</dt><dd>{date(survey?.updated_at)}</dd>
      </dl>
      {percentage != null && <progress aria-label="Entire survey completion" value={completed} max={total} />}
      <p className="muted">{unavailable ? "Disconnected; showing last confirmed coverage." : "Latest confirmed checkpoint coverage; updates after persistence."}{survey?.updated_at && now - Date.parse(survey.updated_at) > 60_000 && " No coverage update in the last minute."}</p>
    </section>
    <h3>Scheduling</h3>
    <dl className="ops-details">
      <dt>Next scheduled execution</dt><dd>{date(s.next_run_at)}</dd>
      <dt>Time remaining</dt><dd>{unavailable ? "Unavailable" : countdown(s.next_run_at, now)}</dd>
      <dt>Latest completed run / status</dt><dd>{last?.run_id ?? (s.last_completed_at ? s.last_run_id : undefined) ?? "Unavailable"} / {last?.status ?? (s.last_completed_at && !active ? s.status : "Unavailable")}</dd>
      <dt>Latest completion time</dt><dd>{date(last?.completed_at ?? s.last_completed_at)}</dd>
      <dt>Last batch discovered / accepted</dt><dd>{number(last?.metrics?.discovered ?? (s.last_completed_at ? s.metrics?.discovered : undefined))} / {number(last?.metrics?.accepted ?? (s.last_completed_at ? s.metrics?.accepted : undefined))}</dd>
      <dt>Last batch stopping reason</dt><dd>{last?.metrics?.stopping_reason ?? s.metrics?.stopping_reason ?? "Unavailable"}</dd>
    </dl>
    <details>
      <summary>Threads diagnostics</summary>
      <dl className="ops-details">
        <dt>Acquisition / processing / Bronze ingestion</dt><dd>{m?.acquisition?.status ?? "Unavailable"} / {m?.processing_status ?? "Unavailable"} / {m?.bronze_ingestion_status ?? "Unavailable"}</dd>
        <dt>Recovered navigation failures</dt><dd>{number(d?.transient_failures)}</dd>
        <dt>Processing and collection errors</dt><dd>{number(m?.errors)}</dd>
        <dt>New / existing identities</dt><dd>{number(m?.new_identities)} / {number(m?.existing_identities)}</dd>
        <dt>Distinct IDs / duplicate encounters / relevance rejected</dt><dd>{number(d?.unique_discovered_count)} / {number(d?.duplicate_discovered_count)} / {number(d?.relevance_rejected_count)}</dd>
        <dt>Geographic coverage</dt><dd>{Object.entries(d?.geographic_coverage ?? {}).map(([version, coverage]) => `${version}: ${coverage.geographic_references} references; ${coverage.attempted} query attempts`).join("; ") || "Unavailable"}</dd>
        <dt>Semantic coverage</dt><dd>{Object.entries(d?.semantic_coverage ?? {}).map(([category, coverage]) => `${category}: ${coverage.attempted}/${coverage.planned} attempted`).join("; ") || "Unavailable"}</dd>
      </dl>
    </details>
  </>;
}
