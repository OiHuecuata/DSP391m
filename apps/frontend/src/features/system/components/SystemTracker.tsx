"use client";

import { useState } from "react";
import Link from "next/link";
import { ComingSoon } from "./ComingSoon";
import { CrawlerTracker } from "./CrawlerTracker";
import { TrackerTabs } from "./TrackerTabs";
import { ThreadsEDA } from "@/features/research/ThreadsEDA";

const mainTabs = [{ id: "analysis", name: "Analysis" }, { id: "system", name: "System" }];
const systemTabs = [{ id: "api", name: "API" }, { id: "crawler", name: "Crawler" }];

export function SystemTracker() {
  const [mainTab, setMainTab] = useState("system");
  const [systemTab, setSystemTab] = useState("crawler");
  return <div className="tracker-shell">
    <header className="site-header"><div className="brand"><span className="brand-mark" aria-hidden="true">D</span>DSP391m<span className="brand-divider" />Data platform<Link href="/data-operations">Data Operations</Link></div></header>
    <main className="tracker-main">
      <div className="page-heading"><span className="eyebrow">Platform operations</span><h1>Tracker</h1><p>Follow your data collection, from query to completion.</p></div>
      <TrackerTabs id="main" label="Tracker sections" options={mainTabs} selected={mainTab} onSelect={setMainTab} />
      <div role="tabpanel" id={`main-panel-${mainTab}`} aria-labelledby={`main-tab-${mainTab}`} tabIndex={0}>
        {mainTab === "analysis" ? <ThreadsEDA /> : <>
          <div className="section-heading"><h2>System</h2><p>Runtime visibility for your data platform.</p></div>
          <TrackerTabs id="system" label="System trackers" options={systemTabs} selected={systemTab} onSelect={setSystemTab} variant="secondary" />
          <div role="tabpanel" id={`system-panel-${systemTab}`} aria-labelledby={`system-tab-${systemTab}`} tabIndex={0}>
            {systemTab === "api" ? <ComingSoon title="API tracker" description="API monitoring will be available in a future release." /> : <CrawlerTracker />}
          </div>
        </>}
      </div>
    </main>
    <footer className="site-footer">DSP391m · System tracker</footer>
  </div>;
}
