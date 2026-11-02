"use client";

import { useState } from "react";
import type { Run } from "@/features/data-ops/types";
import { collectionHistory, type HistoryRange } from "./history";
import styles from "./dashboard.module.css";

export const platforms = [
  { id: "threads", name: "Threads", logo: "threads.svg", href: "/collectors/threads", description: "Social posts" },
  { id: "youtube", name: "YouTube", logo: "youtube.png", href: "/collectors/youtube", description: "Videos & comments" },
  { id: "pbplus", name: "PickleballPlus", logo: "pickleballplus.png", href: "/collectors/pickleballplus", description: "Court listings" },
] as const;
const label = (timestamp: number, range: HistoryRange) => new Date(timestamp).toLocaleString("en-GB", {
  timeZone: "Asia/Ho_Chi_Minh",
  ...(range === "24h" ? { hour: "2-digit" as const, minute: "2-digit" as const } : { day: "2-digit" as const, month: "short" as const }),
});
export function CollectionChart({ runs, updated, unavailable }: { runs: Run[]; updated?: string; unavailable: boolean }) {
  const [range, setRange] = useState<HistoryRange>("24h");
  const [hovered, setHovered] = useState<number | null>(null);
  const now = updated ? Date.parse(updated) : 0;
  const history = collectionHistory(runs, range, now);
  const totals = history.bins.map(bin => Object.values(bin.counts).reduce((a, b) => a + b, 0));
  const max = Math.max(1, ...totals);
  const ceiling = max <= 10 ? 10 : Math.ceil(max / 10 ** Math.floor(Math.log10(max))) * 10 ** Math.floor(Math.log10(max));
  const active = hovered == null ? null : history.bins[hovered];
  return <section className={`${styles.panel} ${styles.chartPanel}`} aria-labelledby="volume-title">
    <div className={styles.panelHeading}><div><span className={styles.eyebrow}>THE BIG PICTURE</span><h2 id="volume-title">Collection volume</h2></div><div className={styles.rangeSwitch} role="group" aria-label="Chart time range">
      {(["24h", "7d"] as const).map(value => <button key={value} aria-pressed={range === value} onClick={() => { setRange(value); setHovered(null); }}>{value === "24h" ? "24 hours" : "7 days"}</button>)}
    </div></div>
    <div className={styles.chartHeadline}><strong key={history.total} className={styles.metricUpdate}>{updated ? history.total.toLocaleString() : "—"}</strong><div><span>accepted records</span><small>{updated ? `${history.countedRuns} completed ${history.countedRuns === 1 ? "run" : "runs"} in this period` : "Waiting for a live snapshot"}</small></div></div>
    <div className={styles.chartWrap}>
      <div className={styles.axis}><span>{ceiling.toLocaleString()}</span><span>{(ceiling / 2).toLocaleString()}</span><span>0</span></div>
      <div className={styles.plot}>
        <div className={styles.gridLines} aria-hidden="true"><span /><span /><span /></div>
        <div className={styles.bars} style={{ gridTemplateColumns: `repeat(${history.bins.length}, minmax(0, 1fr))` }}>
          {history.bins.map((bin, i) => <button key={i} className={styles.barButton} onMouseEnter={() => setHovered(i)} onMouseLeave={() => setHovered(null)} onFocus={() => setHovered(i)} onBlur={() => setHovered(null)} aria-label={`${label(bin.start, range)} to ${label(bin.end, range)} ICT: ${platforms.map(p => p.name + " " + bin.counts[p.id]).join(", ")} accepted records in available history`}>
            <span className={styles.barStack} style={{ height: `${totals[i] / ceiling * 100}%` }}>{platforms.map(p => bin.counts[p.id] > 0 && <span key={p.id} className={styles[p.id]} style={{ height: `${bin.counts[p.id] / totals[i] * 100}%` }} />)}</span>
          </button>)}
        </div>
        {history.countedRuns === 0 && <div className={styles.chartEmpty}><span className={styles.emptyOrbit} aria-hidden="true">↗</span><strong>{unavailable ? "History is unavailable" : !updated ? "Connecting your workspace" : history.missingCounts ? "Record counts aren’t available yet" : "A little quiet. Ready when you are."}</strong><span>{unavailable ? "The last available snapshot is shown." : "Completed collection runs will appear here."}</span></div>}
      </div>
    </div>
    <div className={styles.timeAxis}><span>{label(history.bins[0].start, range)}</span><span>{label(history.bins[Math.floor(history.bins.length / 2)].start, range)}</span><span>{updated ? label(now, range) : "Now"} ICT</span></div>
    <div className={styles.chartLegend}>{platforms.map(p => <span key={p.id}><i className={styles[p.id]} />{p.name}</span>)}</div>
    <p className={styles.chartReadout} role="status">{active && history.countedRuns > 0 ? `${label(active.start, range)}–${label(active.end, range)} ICT · ${platforms.map(p => `${p.name}: ${active.counts[p.id].toLocaleString()}`).join(" · ")}` : ""}</p>
    <p className={styles.chartNote}>Available history: latest 100 runs. Replays excluded; accepted records may include existing items.{history.missingCounts > 0 && ` Counts missing for ${history.missingCounts} completed runs.`}</p>
    {history.countedRuns > 0 && <details className={styles.chartData}><summary>View chart data</summary><div><table><caption>Accepted records in available run history · ICT</caption><thead><tr><th>Period starts</th>{platforms.map(p => <th key={p.id}>{p.name}</th>)}</tr></thead><tbody>{history.bins.map(bin => <tr key={bin.start}><td>{label(bin.start, range)}</td>{platforms.map(p => <td key={p.id}>{bin.counts[p.id].toLocaleString()}</td>)}</tr>)}</tbody></table></div></details>}
  </section>;
}
