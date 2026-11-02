"use client";

import Image from "next/image";
import Link from "next/link";
import { time, useWorkspaceStatus } from "./AppShell";
import { activeSource, threadsSurvey } from "@/features/data-ops/progress";
import { CollectionChart, platforms } from "./CollectionChart";
import styles from "./dashboard.module.css";

function Badge({ value, running = false }: { value?: string; running?: boolean }) {
  const label = value ? value.toLowerCase().replaceAll("_", " ") : "Unavailable";
  const tone = /healthy|success|connected/.test(label) ? "green" : /running|recovering/.test(label) ? "orange" : /failed|error|partial|interrupted/.test(label) ? "red" : "neutral";
  return <span className={`${styles.badge} ${styles[tone]}`}><i className={running ? styles.pulseDot : ""} />{label}</span>;
}
const number = (value?: number | null) => value == null ? "—" : value.toLocaleString();
const completedAt = (value?: string | null) => value ? new Date(value).toLocaleString("en-GB", { timeZone: "Asia/Ho_Chi_Minh", day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit" }) + " ICT" : "—";
function relative(value: string, now?: string) {
  if (!now) return time(value);
  const minutes = Math.max(0, Math.floor((Date.parse(now) - Date.parse(value)) / 60000));
  if (minutes < 1) return "Just now";
  if (minutes < 60) return `${minutes}m ago`;
  if (minutes < 1440) return `${Math.floor(minutes / 60)}h ago`;
  return `${Math.floor(minutes / 1440)}d ago`;
}
export function HomePage() {
  const { data, error, updated } = useWorkspaceStatus();
  const allRuns = [...(data?.collection.runs ?? [])].sort((a, b) => (b.completed_at ?? b.started_at).localeCompare(a.completed_at ?? a.started_at));
  const runs = allRuns.slice(0, 5);
  const nextRun = data ? Object.values(data.collection.sources).filter(s => s.enabled && s.configured && s.next_run_at).map(s => s.next_run_at!).sort()[0] : undefined;
  const currentSource = data ? activeSource(data.collection) : undefined;
  const running = data ? Object.values(data.collection.sources).filter(s => /^(RUNNING|RECOVERING|STOPPING)$/.test(s.status)).length : 0;
  const needsAttention = data ? Object.values(data.collection.sources).filter(s => /^(FAILED|INTERRUPTED|PARTIAL)$/.test(s.status)).length : 0;
  return <main id="system-status" className={styles.dashboard}>
    <div className={styles.pageHeading}><div><span className={styles.eyebrow}>WORKSPACE / OVERVIEW</span><h1>System status<span className={styles.headingDot} aria-hidden="true">.</span></h1></div><div className={styles.toolbar}><span className={styles.today}>{updated ? new Date(updated).toLocaleDateString("en-GB", { timeZone: "Asia/Ho_Chi_Minh", weekday: "short", day: "numeric", month: "short" }) : "Local workspace"}</span><Link href="/data-operations" aria-label="Open operations">Data Operations <span aria-hidden="true">↗</span></Link></div></div>
    {error && <div role="alert" className={styles.alert}>{data ? "Connection lost. Showing the last successful snapshot. " : "Unable to load system status. "}{error} Retrying automatically.</div>}
    <div className={styles.overviewGrid}>
      <CollectionChart runs={allRuns} updated={updated} unavailable={!!error} />
      <section className={`${styles.panel} ${styles.collectorPanel}`} aria-labelledby="collectors-title">
        <div className={styles.panelHeading}><div><span className={styles.eyebrow}>YOUR SOURCES</span><h2 id="collectors-title">Collectors <span className={styles.count}>03</span></h2></div>{running > 0 && !error ? <span className={styles.runningLabel}><i className={styles.pulseDot} />{running} running</span> : <span className={styles.quietLabel}>{error ? "Snapshot offline" : needsAttention ? `${needsAttention} need attention` : data ? "Standing by" : "Connecting"}</span>}</div>
        <div className={styles.collectorList}>{platforms.map(collector => {
          const source = data?.collection.sources[collector.id];
          const sourceRuns = allRuns.filter(run => run.source === collector.id);
          const latestRun = sourceRuns.find(run => run.run_id === source?.last_run_id) ?? [...sourceRuns].sort((a,b) => b.started_at.localeCompare(a.started_at))[0];
          const accepted = latestRun ? latestRun.metrics?.accepted : source?.metrics?.accepted;
          const completed = source?.last_completed_at ?? sourceRuns.find(run => run.completed_at)?.completed_at;
          const isRunning = !!source && /^(RUNNING|RECOVERING|STOPPING)$/.test(source.status);
          const survey = collector.id === "threads" && data ? threadsSurvey(data.collection) : undefined;
          const requestMetrics = currentSource === collector.id ? data?.collection.progress?.metrics?.requests : undefined;
          const done = collector.id === "threads" ? survey?.completed_queries : collector.id === "youtube" ? requestMetrics?.used : undefined;
          const target = collector.id === "threads" ? survey?.candidate_queries : collector.id === "youtube" ? requestMetrics?.budget : undefined;
          const percent = done != null && target != null && target > 0 ? Math.min(100, Math.max(0, done / target * 100)) : undefined;
          return <Link key={collector.id} href={collector.href} aria-label={`Open ${collector.name} collector`} className={styles.collectorRow}>
            <div className={`${styles.platformIcon} ${styles[collector.id + "Tint"]}`}><Image src={`/brands/${collector.logo}`} alt="" width={22} height={22} unoptimized /></div>
            <div className={styles.sourceInfo}><div><h3>{collector.name}</h3><Badge value={source ? !source.configured ? "not_configured" : source.queued ? "queued" : source.status : undefined} running={isRunning && !error} /></div><span>{collector.description}</span>{isRunning && <div className={styles.progressBlock}>{percent != null ? <><div className={styles.progressTrack} role="progressbar" aria-label={`${collector.name} ${collector.id === "threads" ? "query" : "request budget"} progress`} aria-valuemin={0} aria-valuemax={100} aria-valuenow={percent}><span style={{ width: `${percent}%` }} /></div><small>{number(done)} / {number(target)} {collector.id === "threads" ? "queries" : "requests"}</small></> : <span className={styles.stage}>{data?.collection.progress?.source === collector.id ? data.collection.progress.stage?.replaceAll("_", " ") ?? "Collection in progress" : "Collection in progress"}</span>}</div>}</div>
            <div className={styles.sourceMetric}><strong key={accepted} className={accepted != null ? styles.metricUpdate : ""}>{accepted == null ? "—" : number(accepted)}</strong><span>{accepted == null ? "No runtime data yet" : "latest run · records"}</span><small title="Last completed">{completedAt(completed)}</small></div><span className={styles.rowArrow} aria-hidden="true">↗</span>
          </Link>;
        })}</div>
        <div className={styles.collectorFooter}><span className={styles.planned}>TikTok <span>Coming soon</span></span></div>
      </section>
    </div>
    <section className={styles.infrastructure} aria-labelledby="infra-title">
      <div className={styles.infraLabel}><span className={styles.eyebrow}>UNDER THE HOOD</span><h2 id="infra-title">Infrastructure</h2></div>
      <article><div><h3>Scheduler</h3><Badge value={data?.collection.status} /></div><p>{data ? data.collection.continuous ? "Continuous collection enabled" : "Scheduling paused" : "Waiting for status"}<span>Next run: {time(nextRun)} ICT</span></p></article>
      <article><div><h3>Data Lake</h3><Badge value={data?.lakehouse.status} /></div><p>{number(data?.lakehouse.table_count)} tables<span>Updated {time(data?.lakehouse.updated_at)} ICT</span></p></article>
      <article><div><h3>ClickHouse</h3><Badge value={data?.warehouse.status} /></div><p>{number(data?.warehouse.rows)} serving rows<span>Last sync {time(data?.warehouse.last_successful_load_at)} ICT</span></p></article>
    </section>
    <section className={`${styles.panel} ${styles.feedPanel}`} aria-labelledby="activity-title">
      <div className={styles.panelHeading}><div><span className={styles.eyebrow}>THE LATEST</span><h2 id="activity-title">Recent activity</h2></div><span className={styles.quietLabel}>Collection run history · ICT</span></div>
      {runs.length ? <ol className={styles.feed}>{runs.map(run => {
        const platform = platforms.find(p => p.id === run.source)!;
        const failed = /FAILED|INTERRUPTED/.test(run.status);
        const title = run.operation === "processing_replay" ? "saved processing replay" : "collection";
        return <li key={run.run_id + run.started_at}><span className={`${styles.eventIcon} ${styles[run.source + "Tint"]}`}><Image src={`/brands/${platform.logo}`} alt="" width={18} height={18} unoptimized /></span><div className={styles.eventDetails}><p><strong>{platform.name}</strong> {title} {failed ? "failed" : run.completed_at ? "finished" : "started"}<Badge value={run.status} running={!run.completed_at && !error} /></p><span>{run.error ?? `${number(run.metrics?.accepted)} accepted records`}</span><small title={run.run_id}>{run.run_id}</small></div><time dateTime={run.completed_at ?? run.started_at} title={completedAt(run.completed_at ?? run.started_at)}>{relative(run.completed_at ?? run.started_at, updated)}</time></li>;
      })}</ol> : <div className={styles.feedEmpty}><div className={styles.signalArt} aria-hidden="true"><span /><span /><i>↗</i></div><div><h3>{error ? "Waiting to reconnect" : data ? "Ready for your first collection" : "Loading recent activity…"}</h3><p>{error ? "Your activity will return when the connection does." : "Pick a source, start a run, and watch your workspace come to life."}</p></div><Link href="/data-operations">Open Data Operations <span aria-hidden="true">→</span></Link></div>}
    </section>
    <footer className={styles.footer}><span><i className={error ? styles.offlineDot : ""} />{error ? "Connection interrupted" : data ? "Connected to your workspace" : "Connecting to your workspace"}</span><span>Last snapshot {time(updated)} ICT · Auto-refresh 5s</span></footer>
  </main>;
}
