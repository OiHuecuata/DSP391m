"use client";

import { useEffect, useState } from "react";
import { useWorkspaceStatus } from "@/features/home/AppShell";
import { controlCollection } from "@/services/data-ops";
import { OperationsGuide } from "./OperationsGuide";
import plain from "@/features/collectors/collectors.module.css";
import { readableState } from "@/features/collectors/CollectorOverview";
import { ThreadsProgress } from "./ThreadsProgress";
import { activeSource, continuousLabel, retainSurvey } from "./progress";
import { formatPhysicalSize } from "./format";
import type { SourceConfig, SourceName } from "./types";

const sources: { id: SourceName; label: string }[] = [{ id: "threads", label: "Threads" }, { id: "youtube", label: "YouTube" }, { id: "pbplus", label: "PBPlus" }];
function date(value: string | null | undefined) { return value ? new Date(value).toLocaleString() : "—"; }
function Badge({ value }: { value: string }) {
  const tone = ["HEALTHY", "SUCCESS"].includes(value) ? "green" : ["FAILED", "INTERRUPTED"].includes(value) ? "red" : ["PARTIAL", "STOPPING", "RECOVERING"].includes(value) ? "amber" : value === "RUNNING" ? "blue" : "neutral";
  return <span className={`status-badge status-${tone}`}><span className="status-dot" />{readableState(value)}</span>;
}

export function DataOperations() {
  const [now, setNow] = useState(0);
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1_000);
    return () => clearInterval(timer);
  }, []);
  const { data, setData, error } = useWorkspaceStatus();
  const [draftOverride, setDraft] = useState<SourceConfig | null>(null);
  const draft = draftOverride ?? (data ? Object.fromEntries(sources.map(({ id }) => [id, {
    enabled: data.collection.sources[id].enabled && data.collection.sources[id].configured,
    interval_seconds: data.collection.sources[id].interval_seconds,
  }])) as SourceConfig : null);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState("");

  async function control(action: "start" | "stop" | "run" | "config" | "retry" | "replay" | "continue" | "recover", selected?: SourceName[], retry?: string) {
    setBusy(true); setNotice("");
    try {
      const collection = await controlCollection(action, ["retry", "replay", "continue", "recover"].includes(action) ? { run_id: retry! } : (action === "start" || action === "config") && draft ? { sources: draft } : action === "run" ? { sources: selected ?? sources.filter(({ id }) => draft?.[id].enabled).map(({ id }) => id) } : undefined);
      setData((old) => old ? { ...old, collection: retainSurvey(collection, old.collection) } : old);
      setNotice(action === "config" ? "Source settings saved." : ["retry", "replay"].includes(action) ? "Saved data is queued to be processed again. This will not download new platform content." : action === "recover" ? "The interrupted collection is queued to resume using its original reference." : action === "continue" ? "More searches are queued as a new collection attempt." : action === "stop" ? "Future runs stopped. The current cycle will finish safely." : action === "start" ? "Continuous collection enabled. Start enables scheduled execution; an existing schedule may wait until its next run. You can close this browser." : "Run queued. Collection continues in the background.");
    } catch (e) { setNotice(e instanceof Error ? e.message : "Control failed."); }
    finally { setBusy(false); }
  }
  const rows = (name: string) => data?.lakehouse.tables.find((t) => t.name === name)?.rows ?? "—";
  const c = data?.collection;
  const progress = c?.progress;
  return <div className={`tracker-shell ${plain.operations}`}>
    <main className="tracker-main ops-main">
      <div className="page-heading"><span className="eyebrow">Platform operations</span><h1>Data Operations</h1></div>
      <OperationsGuide />
      {error && <div role="alert" className="notice notice-error">{error}{data && " Showing the previous snapshot; it may be stale."}</div>}
      {!data && !error && <div role="status">Loading data operations…</div>}
      {data && draft && <>
        <section aria-label="Pipeline" className="card ops-panel"><div className="panel-header"><h2>Overall data flow</h2><Badge value={data.pipeline.status} /></div><p className="muted">{data.pipeline.basis}</p></section>
        <section aria-label="Collection" className="card ops-panel"><div className="panel-header"><h2>Collection</h2><Badge value={continuousLabel(c!, !!error)} /></div>
          <div className="ops-actions"><button disabled={busy || !sources.some(({ id }) => draft[id].enabled)} onClick={() => control("start")}>Start continuous collection</button><button disabled={busy || (!c!.continuous && !c!.active_run_id)} onClick={() => control("stop")}>Stop after current run</button><button disabled={busy || !sources.some(({ id }) => draft[id].enabled)} onClick={() => control("run")}>Run all now</button><button disabled={busy} onClick={() => control("config")}>Save source settings</button></div>
          {notice && <p role="status" className="notice ops-notice">{notice}</p>}
          {progress && <details className={plain.technical}><summary>Live processing details</summary><p role="status" className="runtime-message">Stage: {progress.stage ?? "scheduled"}{activeSource(c!) === "youtube" && progress.requests_used != null && ` · YouTube API requests ${progress.requests_used}`}{progress.videos != null && ` · Videos ${progress.videos}`}{progress.comments != null && ` · Comments ${progress.comments}`}{progress.accepted != null && ` · Accepted ${progress.accepted}`}{progress.metrics?.acknowledged != null && ` · Kafka acknowledged ${progress.metrics.acknowledged}`}{progress.metrics?.consumed != null && ` · Spark consumed ${progress.metrics.consumed}`}{progress.query && ` · ${progress.query}`}</p></details>}
          <div className="ops-source-grid">{sources.map(({ id, label }) => {
            const s = c!.sources[id]; const m = activeSource(c!) === id ? progress?.metrics : s.metrics;
            return <article key={id} aria-label={`${label} collection`} className="query-card"><div className="panel-header"><h2>{label}</h2><Badge value={!s.configured ? "NOT_CONFIGURED" : s.queued ? "QUEUED" : s.status} /></div>
              <label className="ops-toggle"><input type="checkbox" checked={draft[id].enabled} disabled={!s.configured || busy} onChange={(e) => setDraft({ ...draft, [id]: { ...draft[id], enabled: e.target.checked } })} />Enable {label}</label>
              <label className="ops-interval">Time between collections (seconds)<input aria-label={`${label} interval seconds`} type="number" min={10} max={604800} value={draft[id].interval_seconds} onChange={(e) => setDraft({ ...draft, [id]: { ...draft[id], interval_seconds: Number(e.target.value) } })} /></label>
              {id === "youtube" && <p className="muted">YouTube connection ready: {s.configured ? "Yes" : "No"}</p>}
              <div className={plain.sourceSummary}><div><span>Items checked</span><strong>{m?.discovered == null ? "Not reported" : m.discovered.toLocaleString()}</strong></div><div><span>Items accepted</span><strong>{m?.accepted == null ? "Not reported" : m.accepted.toLocaleString()}</strong></div></div>
              
              {!s.configured && <p className="muted">Setup is needed before this collector can run.</p>}
              <details className={plain.technical}><summary>Schedule, counts and technical details</summary>
              {id === "threads" && <ThreadsProgress collection={c!} unavailable={!!error} now={now} />}
              {id !== "threads" && <dl className="ops-details"><dt>Last started</dt><dd>{date(s.last_started_at)}</dd><dt>Last completed</dt><dd>{date(s.last_completed_at)}</dd><dt>Next run</dt><dd>{date(s.next_run_at)}</dd><dt>Collection reference</dt><dd>{s.last_run_id ?? "—"}</dd><dt>Items checked (including repeats)</dt><dd>{m?.discovered ?? "—"}</dd><dt>Items accepted by the rules</dt><dd>{m?.accepted ?? "—"}</dd><dt>New items / items seen before</dt><dd>{m ? `${m.new_identities ?? "—"} / ${m.existing_identities ?? "—"}` : "—"}</dd><dt>Errors</dt><dd>{m?.errors ?? "—"}</dd>{id === "youtube" && <><dt>Videos / comments</dt><dd>{m ? `${m.videos ?? "—"} / ${m.comments ?? "—"}` : "—"}</dd><dt>Platform requests / allowance</dt><dd>{m?.requests ? `${m.requests.used} / ${m.requests.budget}` : "—"}</dd><dt>Stopping reason</dt><dd>{m?.stopping_reason ?? "—"}</dd></>}</dl>}
              </details>
              {s.last_error && <p className="error-text">{s.last_error}</p>}<button disabled={busy || !s.configured || s.queued || ["RUNNING", "STOPPING", "RECOVERING"].includes(s.status)} onClick={() => control("run", [id])}>Run {label} now</button>{s.last_run_id && ["FAILED", "PARTIAL"].includes(s.status) && <button disabled={busy || s.queued} onClick={() => control("replay", undefined, s.last_run_id)}>Process saved {label} data again</button>}{id === "threads" && s.pending_acquisition_run_id && <button disabled={busy || s.queued || activeSource(c!) === id} onClick={() => control("recover", undefined, s.pending_acquisition_run_id)}>Resume interrupted Threads collection</button>}{id === "threads" && s.last_run_id && (c!.sources.threads.survey?.has_more ?? m?.discovery?.has_more) && !s.pending_acquisition_run_id && <button disabled={busy || s.queued || activeSource(c!) === id} onClick={() => control("continue", undefined, s.last_run_id)}>Continue searching Threads</button>}

            </article>;
          })}</div>
        </section>
        <div className="ops-infra-grid"><section aria-label="Data Lake" className="card ops-panel"><div className="panel-header"><h2>Data Lake</h2><Badge value={data.lakehouse.status} /></div><dl className="ops-details"><dt>Storage backend</dt><dd>{data.lakehouse.backend}</dd><dt>Storage address</dt><dd>{data.lakehouse.uri}</dd><dt>Can read / can save</dt><dd>{data.lakehouse.readable ? "Yes" : "No"} / {data.lakehouse.writable ? "Yes" : "No"}</dd><dt>Physical size</dt><dd>{formatPhysicalSize(data.lakehouse.physical_bytes)}</dd><dt>Tables</dt><dd>{data.lakehouse.table_count}</dd><dt>Cleaned social records</dt><dd>{rows("silver/social_posts")}</dd><dt>Cleaned court records</dt><dd>{rows("silver/courts")}</dd><dt>Prepared market records</dt><dd>{rows("gold/location_market_signal_snapshot")}</dd><dt>Last update</dt><dd>{date(data.lakehouse.updated_at)}</dd></dl></section>
        <section aria-label="Data Warehouse" className="card ops-panel"><div className="panel-header"><h2>Data Warehouse</h2><Badge value={data.warehouse.status} /></div><dl className="ops-details"><dt>Backend</dt><dd>{data.warehouse.backend}</dd><dt>Host / database</dt><dd>{data.warehouse.host} / {data.warehouse.database}</dd><dt>Serving table</dt><dd>{data.warehouse.table}</dd><dt>Records available for reporting</dt><dd>{data.warehouse.rows ?? "—"}</dd><dt>Source activity rows</dt><dd>{data.warehouse.source_activity_rows ?? "—"}</dd><dt>Different location labels</dt><dd>{data.warehouse.distinct_locations ?? "—"}</dd><dt>Latest snapshot</dt><dd>{data.warehouse.latest_snapshot_date ?? "—"}</dd><dt>Last successful load</dt><dd>{date(data.warehouse.last_successful_load_at)}</dd></dl>{data.warehouse.error && <p className="error-text">{data.warehouse.error}</p>}</section></div>
        <section aria-label="Delta tables" className="card ops-panel"><h2>Stored datasets</h2><div className="ops-table-scroll"><table><thead><tr><th>Dataset</th><th>Exists</th><th>Records</th><th>Version</th><th>Last update</th></tr></thead><tbody>{data.lakehouse.tables.map((t) => <tr key={t.name}><td>{t.name}{t.error && <p className="muted">{t.error}</p>}</td><td>{t.exists ? "Yes" : "No"}</td><td>{t.rows ?? "—"}</td><td>{t.version ?? "—"}</td><td>{date(t.updated_at)}</td></tr>)}</tbody></table></div></section>
        <section aria-label="Run history" className="card ops-panel"><h2>Recent runs</h2><div className="ops-table-scroll"><table><thead><tr><th>Run</th><th>Source</th><th>Started</th><th>Duration</th><th>Status</th><th>Items accepted</th><th>New item identifiers</th><th>Errors</th></tr></thead><tbody>{c!.runs.map((r) => <tr key={`${r.run_id}-${r.started_at}`}><td>{r.run_id}{r.error && <p className="error-text">{r.error}</p>}</td><td>{r.source}</td><td>{date(r.started_at)}</td><td>{r.completed_at ? `${r.duration_seconds.toFixed(1)}s` : "In progress"}</td><td><Badge value={r.status} /></td><td>{r.metrics?.accepted ?? "—"}</td><td>{r.metrics?.new_identities ?? "—"}</td><td>{r.metrics?.errors ?? "—"}</td></tr>)}</tbody></table>{!c!.runs.length && <p className="muted">No collection runs yet.</p>}</div></section>
        
      </>}
    </main>
  </div>;
}
