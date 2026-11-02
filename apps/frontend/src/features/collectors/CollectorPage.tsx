"use client";

import Image from "next/image";
import { VenueSnapshot } from "@/features/courts/VenueSnapshot";
import Link from "next/link";
import { useWorkspaceStatus, time } from "@/features/home/AppShell";
import { useSystemStatus } from "@/features/system/hooks/useSystemStatus";
import { CrawlerStatusPanel } from "@/features/system/components/CrawlerStatusPanel";
import type { CrawlerComponentStatus } from "@/features/system/types";
import type { SourceName } from "@/features/data-ops/types";
import { CollectorOverview, readableState } from "./CollectorOverview";
import presentation from "./collectors.module.css";
import styles from "@/features/home/home.module.css";

const collectors = {
  threads: { name: "Threads", logo: "threads.svg" },
  youtube: { name: "YouTube", logo: "youtube.png" },
  pbplus: { name: "PickleballPlus", logo: "pickleballplus.png" },
};

function ThreadsMonitor({ status, offline }: { status?: import("@/features/data-ops/types").Source; offline: boolean }) {
  const { data, loading, error } = useSystemStatus();
  const component = data?.components.find((item): item is CrawlerComponentStatus =>
    item.type === "crawler" && item.id === "threads");
  return <>
    <CollectorOverview name="Threads" status={status} runtime={component} offline={offline || !!error} />
    <details className={presentation.technical}><summary>Technical details · search tasks and collection diagnostics</summary>
    {error && <div role="alert" className="notice notice-error"><strong>Backend unavailable</strong><p>{error}</p>{data && <p>Showing the previous snapshot; it may be stale.</p>}</div>}
    {loading ? <div role="status" className="empty-state card">Loading Threads status…</div> :
      data ? <CrawlerStatusPanel source={{ id: "threads", name: "Threads" }} component={component} /> :
        <div className="empty-state card"><h2>Waiting for Threads status</h2><p>Retrying automatically every five seconds.</p></div>}
    </details>
  </>;
}

export function CollectorPage({ source }: { source: SourceName }) {
  const { data, error } = useWorkspaceStatus();
  const collector = collectors[source];
  const status = data?.collection.sources[source];
  const runs = (data?.collection.runs ?? []).filter(run => run.source === source)
    .sort((a, b) => b.started_at.localeCompare(a.started_at)).slice(0, 10);
  return <main className={styles.main}>
    <div className={styles.collectorHeading}>
      <div><h1><Image src={`/brands/${collector.logo}`} alt="" width={28} height={28} className={styles.collectorLogo} unoptimized />{collector.name}</h1></div>
      <Link className={styles.manageLink} href="/data-operations">Manage collection</Link>
    </div>
    {source === "pbplus" && <VenueSnapshot />}
    {source !== "pbplus" && <>
    {source === "threads" ? <ThreadsMonitor status={status} offline={!!error} /> : <>
      {error && <div role="alert" className={styles.alert}>{error} {data ? "Showing the last successful snapshot; it may be stale." : "Retrying automatically."}</div>}
      {!status ? <div role="status" className={styles.empty}>{error ? "Collector status is unavailable until the backend reconnects." : `Loading ${collector.name} status…`}</div> :
        <>

          {source === "youtube" && <CollectorOverview name={collector.name} status={status} offline={!!error} />}
          <details className={presentation.technical}><summary>Collection details · schedule and troubleshooting</summary>
            <p>Current state: {readableState(status.status)}</p>
            <div className={styles.collectorTiming}><p>Last started: {time(status.last_started_at)} ICT</p><p>Last finished: {time(status.last_completed_at)} ICT</p><p>Next scheduled collection: {time(status.next_run_at)} ICT</p></div>
            {source === "youtube" && <p>Requests sent to YouTube: {status.metrics?.requests?.used ?? "Not reported"} / {status.metrics?.requests?.budget ?? "Not reported"} allowed in this collection.</p>}
            <p>Collection issues reported: {status.metrics?.errors ?? "Not reported"}.</p>
            {status.last_error && <div role="alert" className={styles.alert}>{status.last_error}</div>}
          </details>
        </>}
    </>}
    <section aria-labelledby="collector-history"><div className={styles.sectionHeading}><h2 id="collector-history">Recent {collector.name} runs</h2><span>ICT</span></div>
      {runs.length ? <ul className={presentation.history}>{runs.map(run => <li key={run.run_id + run.started_at}><div><h3>{readableState(run.status)}</h3><p>{run.metrics?.accepted == null ? "The accepted-item count was not reported." : `${run.metrics.accepted.toLocaleString()} records accepted by the collection rules.`} {run.operation === "processing_replay" ? "Previously saved data was processed again; this did not fetch new platform content." : ""}</p><details><summary>View collection reference and any reported issue</summary><p>{run.run_id}</p>{run.error && <p>{run.error}</p>}</details></div><time dateTime={run.started_at}>{new Date(run.started_at).toLocaleString("en-GB", {timeZone:"Asia/Ho_Chi_Minh"})}</time></li>)}</ul> :
        <p className={presentation.empty}>{data ? `No ${collector.name} collection attempts have been reported yet. Start a collection in Data Operations to see results here.` : "Waiting for collection history. No totals are available yet."}</p>}
    </section>
    </>}
  </main>;
}
