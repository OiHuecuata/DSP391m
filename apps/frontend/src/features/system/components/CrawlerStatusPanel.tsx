import type { CrawlerComponentStatus, CrawlerSource, QueryStatus } from "../types";
import { StatusBadge } from "./StatusBadge";

function count(value: number | null | undefined) {
  return value == null ? "—" : value.toLocaleString("en-US");
}

function Timestamp({ value }: { value?: string | null }) {
  if (!value || Number.isNaN(Date.parse(value))) return <span>—</span>;
  return <time dateTime={value}>{new Date(value).toLocaleString(undefined, { timeZoneName: "short" })}</time>;
}

function QueryDetails({ title, query }: { title: string; query?: QueryStatus | null }) {
  return <section className="query-card">
    <h3>{title}</h3><p className="query-text">{query?.query || "No query recorded"}</p>
    {query && <>
      <p className="muted">{[query.kind, query.geo_name, query.status].filter(Boolean).join(" · ")}</p>
      {(query.raw_count != null || query.accepted_count != null) &&
        <p className="query-result">{count(query.raw_count)} raw → {count(query.accepted_count)} accepted</p>}
      {query.error && <p className="error-text">{query.error}</p>}
    </>}
  </section>;
}

export function CrawlerStatusPanel({ source, component }: { source: CrawlerSource; component?: CrawlerComponentStatus }) {
  if (!component) return <div className="empty-state card">
    <span className="empty-icon" aria-hidden="true">◎</span><h2>{source.name}</h2>
    <p>No runtime status available</p><p className="muted">This source has not published a runtime snapshot.</p>
  </div>;

  const details = component.details;
  const collected = details?.collected_count;
  const target = details?.target_count;
  const progress = collected != null && target != null && target > 0
    ? Math.min(100, Math.max(0, (collected / target) * 100)) : null;
  const metrics = [
    { label: "Raw scanned", value: count(details?.raw_count) },
    { label: "Queries processed", value: `${count(details?.queries_processed)} / ${count(details?.query_plan_count)}` },
    { label: "Succeeded", value: count(details?.queries_succeeded) },
    { label: "Failed", value: count(details?.queries_failed) },
  ];

  return <article className="card crawler-panel">
    <header className="panel-header">
      <div><span className="eyebrow">Crawler runtime</span><h2>{source.name}</h2></div>
      <StatusBadge status={component.status} />
    </header>
    {details?.message && <p className="runtime-message">{details.message}</p>}
    {(component.status === "failed" || details?.error) &&
      <div className="notice notice-error" role="alert">
        <strong>{component.status === "failed" ? "Crawler failed" : "Runtime error"}</strong>
        <p>{details?.error || "The crawler reported a failure without an error message."}</p>
      </div>}
    <section className="collection-progress" aria-label="Collection progress">
      <div className="progress-heading">
        <div><h3>Collected items</h3><p className="collection-count">{count(collected)} <span>/ {count(target)}</span></p></div>
        <span className="progress-percent">{progress === null ? "—" : `${Math.round(progress)}%`}</span>
      </div>
      {progress !== null ? <div className="progress-track" role="progressbar"
        aria-label="Collected items" aria-valuemin={0} aria-valuemax={100} aria-valuenow={progress}
        aria-valuetext={`${count(collected)} of ${count(target)} items collected`} data-state={component.status}>
        <div className="progress-fill" style={{ width: `${progress}%` }} />
      </div> : <p className="muted">Collection progress is not available.</p>}
    </section>
    <dl className="metric-grid">{metrics.map((metric) =>
      <div key={metric.label}><dt>{metric.label}</dt><dd>{metric.value}</dd></div>)}
    </dl>
    <div className="query-grid">
      <QueryDetails title="Current query" query={details?.current_query} />
      <QueryDetails title="Last query" query={details?.last_query} />
    </div>
    <section className="run-details"><h3>Run details</h3>
      <dl>
        <div><dt>Seed query</dt><dd>{details?.seed_query || "—"}</dd></div>
        <div><dt>Sort</dt><dd>{details?.sort || "—"}</dd></div>
        <div><dt>Started</dt><dd><Timestamp value={details?.started_at} /></dd></div>
        <div><dt>Finished</dt><dd><Timestamp value={details?.finished_at} /></dd></div>
        <div><dt>Snapshot updated</dt><dd><Timestamp value={component.updated_at} /></dd></div>
      </dl>
    </section>
  </article>;
}
