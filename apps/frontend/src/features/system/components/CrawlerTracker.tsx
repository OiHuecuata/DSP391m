"use client";

import { useState } from "react";
import { crawlerSources } from "../config/crawlerSources";
import { useSystemStatus } from "../hooks/useSystemStatus";
import type { CrawlerComponentStatus } from "../types";
import { CrawlerSourceTabs } from "./CrawlerSourceTabs";
import { CrawlerStatusPanel } from "./CrawlerStatusPanel";

export function CrawlerTracker() {
  const [selectedId, setSelectedId] = useState(crawlerSources[0]?.id ?? "");
  const { data, loading, error } = useSystemStatus();
  const source = crawlerSources.find((item) => item.id === selectedId) ?? crawlerSources[0];
  const component = data?.components.find((item): item is CrawlerComponentStatus =>
    item.type === "crawler" && item.id === source?.id);
  if (!source) return <div className="empty-state card"><h2>No crawler sources configured</h2></div>;

  return <>
    <div className="crawler-toolbar">
      <CrawlerSourceTabs sources={crawlerSources} selected={source.id} onSelect={setSelectedId} />
      <p className="poll-note"><span className="poll-dot" aria-hidden="true" />Auto-refresh · 5s</p>
    </div>
    <div role="tabpanel" id={`crawler-panel-${source.id}`} aria-labelledby={`crawler-tab-${source.id}`} tabIndex={0}>
      {error && <div className="notice notice-error" role="alert">
        <strong>Backend unavailable</strong><p>{error}</p>
        <p>{data ? "Showing the last received snapshot; it may be out of date. " : ""}Retrying automatically every 5 seconds.</p>
      </div>}
      {loading ? <div className="empty-state card" role="status"><span className="loading-ring" aria-hidden="true" />Loading crawler status…</div>
        : data ? <CrawlerStatusPanel source={source} component={component} />
        : <div className="empty-state card"><h2>{source.name}</h2><p>Waiting for a connection to the backend.</p></div>}
    </div>
  </>;
}
