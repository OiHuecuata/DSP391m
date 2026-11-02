import type { Source } from "@/features/data-ops/types";
import type { CrawlerComponentStatus } from "@/features/system/types";
import styles from "./collectors.module.css";

export function readableState(value?: string) {
  const key = value?.toUpperCase();
  if (key === "CONTINUOUS DISABLED") return "Automatic collection is off";
  if (key === "CONTINUOUS ENABLED — WAITING") return "Scheduled · waiting for the next collection";
  if (key === "CONTINUOUS ENABLED — RUNNING") return "Automatic collection is working";
  if (key === "CONTINUOUS ENABLED · SOURCE DISABLED") return "Schedule on · this source is switched off";
  return ({ IDLE: "Waiting for the next collection", RUNNING: "Collecting now", SUCCESS: "Collection finished", HEALTHY: "Working normally", FAILED: "Needs attention", INTERRUPTED: "Collection was interrupted", PARTIAL: "Some steps need attention", RECOVERING: "Resuming interrupted work", STOPPING: "Finishing the current collection", QUEUED: "Waiting to start", NOT_CONFIGURED: "Setup needed", UNAVAILABLE: "Status unavailable" } as Record<string,string>)[key ?? ""] ?? (value ? value.replaceAll("_", " ").toLowerCase() : "Waiting for an update");
}
const count = (value?: number | null) => value == null ? "Not reported" : value.toLocaleString();
export function MetricCards({ items }: { items: { label: string; value?: number | null; explanation: string }[] }) {
  return <div className={styles.metrics}>{items.map(item => <article key={item.label}><span title={item.explanation}>{item.label}</span><strong>{count(item.value)}</strong></article>)}</div>;
}
export function CollectorOverview({ name, status, runtime, offline = false }: { name: string; status?: Source; runtime?: CrawlerComponentStatus; offline?: boolean }) {
  const details = runtime?.details;
  const state = offline ? "Status connection lost" : status && !status.configured ? "Setup needed" : readableState(status?.queued ? "QUEUED" : runtime?.status ?? status?.status);
  const failed = /failed|interrupted|partial/i.test(runtime?.status ?? status?.status ?? "");
  const running = /running|recovering/i.test(runtime?.status ?? status?.status ?? "");
  const accepted = details?.collected_count ?? status?.metrics?.accepted;
  const scanned = details?.raw_count ?? status?.metrics?.discovered;
  const queryDone = details?.queries_processed;
  const queryTotal = details?.query_plan_count;
  const progress = queryDone != null && queryTotal != null && queryTotal > 0 ? Math.min(100, Math.max(0, queryDone / queryTotal * 100)) : null;
  return <section className={styles.overview} aria-label={`${name} at a glance`}>
    <div className={styles.story}><span className={styles.eyebrow}>{offline ? "LAST KNOWN INFORMATION" : "AT A GLANCE"}</span><h2><i data-running={running && !offline} />{state}</h2>{(offline || (!status && !runtime) || (status && !status.configured) || failed) && <p>{offline ? "We cannot check the latest status. Any numbers below may be out of date; we’ll retry automatically." : !status && !runtime ? "The collector has not shared its status yet. Missing numbers mean unavailable information, not zero activity." : status && !status.configured ? `Connect ${name} in Data Operations before starting a collection.` : failed ? "A collection did not finish cleanly. Review the issue in the details before trying again." : running ? `Tracker is checking ${name} for matching content. These numbers may change while it works.` : ""}</p>}</div>
    <MetricCards items={[
      { label: "Items checked", value: scanned, explanation: "Results examined before filtering. The same item can appear more than once." },
      { label: "Items accepted", value: accepted, explanation: "Records kept by the collection rules. This is not a count of unique people." },
      ...(name === "YouTube" ? [{ label: "Videos collected", value: status?.metrics?.videos, explanation: "Video records reported by the collector." }, { label: "Comments collected", value: status?.metrics?.comments, explanation: "Comment records reported separately from videos." }] : [{ label: "Searches completed", value: queryDone, explanation: queryTotal == null ? "Search tasks processed; the total plan has not been reported." : `Out of ${queryTotal.toLocaleString()} planned searches. This measures search coverage, not records collected.` }, { label: "Searches with issues", value: details?.queries_failed, explanation: "Search tasks that failed, rather than a count of missing posts." }]),
    ]} />
    {progress != null && <div className={styles.coverage}><span>Search coverage <strong>{Math.round(progress)}%</strong></span><div role="progressbar" aria-label="Search coverage" aria-valuemin={0} aria-valuemax={100} aria-valuenow={progress}><i style={{width:progress+"%"}} /></div><p>{queryDone?.toLocaleString()} of {queryTotal?.toLocaleString()} planned searches processed.</p></div>}
  </section>;
}
