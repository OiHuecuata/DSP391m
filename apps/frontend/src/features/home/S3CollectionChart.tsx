"use client";
import { useState } from "react";
import type { CollectionHistory } from "@/features/s3/status";
import { platforms } from "./CollectionChart";
import styles from "./dashboard.module.css";
const total = (counts: { threads: number; youtube: number; pbplus: number }) => counts.threads + counts.youtube + counts.pbplus;
const label = (date: string) => new Date(date + "T00:00:00Z").toLocaleDateString("en-GB", { day: "numeric", month: "short", timeZone: "UTC" });
export function S3CollectionChart({ history, loaded, source }: { history?: CollectionHistory; loaded: boolean; source?: "threads" | "youtube" }) {
 const [range, setRange] = useState<"all" | "7d">("all");
 const [hovered, setHovered] = useState<string | null>(null);
 const visiblePlatforms = source ? platforms.filter(p => p.id === source) : platforms;
 const observed = (history?.days ?? []).map(d => ({...d, counts: { threads: !source || source === "threads" ? d.counts.threads : 0, youtube: !source || source === "youtube" ? d.counts.youtube : 0, pbplus: source ? 0 : d.counts.pbplus }})).filter(d => !source || total(d.counts) > 0);
 const latest = observed.at(-1)?.date;
 const earliest = observed[0]?.date;
 const end = latest ? Date.parse(latest + "T00:00:00Z") : 0;
 const start = earliest ? range === "7d" ? Math.max(Date.parse(earliest + "T00:00:00Z"), end - 6 * 86400000) : Date.parse(earliest + "T00:00:00Z") : 0;
 const byDate = new Map(observed.map(d => [d.date, d]));
 const bins = [];
 // Protect the chart from unexpectedly large historical ranges.
 if (observed.length && (end - start) / 86400000 <= 3660) for (let t = start; t <= end; t += 86400000) {
   const date = new Date(t).toISOString().slice(0,10);
   bins.push(byDate.get(date) ?? { date, counts: { threads: 0, youtube: 0, pbplus: 0 } });
 }
 const max = Math.max(1, ...bins.map(b => total(b.counts)));
 const ceiling = Math.ceil(max / 100) * 100;
 const count = bins.reduce((n,b) => n + total(b.counts),0);
 const active = bins.find(b => b.date === hovered);
 return <section className={`${styles.panel} ${styles.chartPanel}`} aria-labelledby="volume-title">
 <div className={styles.panelHeading}><div><h2 id="volume-title" title="Collected observations, including repeated collections. YouTube includes videos and comments.">Collection volume</h2></div><div className={styles.rangeSwitch} role="group" aria-label="Chart time range">{(["all","7d"] as const).map(r => <button key={r} aria-pressed={range === r} onClick={() => {setRange(r);setHovered(null);}}>{r === "all" ? "All dates" : "Latest 7 days"}</button>)}</div></div>
 <div className={styles.chartHeadline}><strong>{bins.length ? count.toLocaleString() : "—"}</strong><div><span>collected observations</span><small>{bins.length ? `${label(bins[0].date)} – ${label(bins.at(-1)!.date)} ${latest!.slice(0,4)} · ICT` : loaded ? "History not available in this export" : "Loading history…"}</small></div></div>
 {bins.length > 0 && <><div className={styles.chartWrap}><div className={styles.axis}><span>{ceiling.toLocaleString()}</span><span>{(ceiling/2).toLocaleString()}</span><span>0</span></div><div className={styles.plot}><div className={styles.gridLines} aria-hidden="true"><span/><span/><span/></div><div className={styles.bars} style={{gridTemplateColumns:`repeat(${bins.length}, minmax(0, 1fr))`}}>{bins.map(b => <button key={b.date} className={styles.barButton} onMouseEnter={() => setHovered(b.date)} onMouseLeave={() => setHovered(null)} onFocus={() => setHovered(b.date)} onBlur={() => setHovered(null)} onClick={() => setHovered(b.date)} aria-label={`${label(b.date)} ICT: ${visiblePlatforms.map(p => `${p.name} ${b.counts[p.id]}`).join(", ")} observations`}><span className={styles.barStack} style={{height:`${total(b.counts)/ceiling*100}%`}}>{visiblePlatforms.map(p => b.counts[p.id] > 0 && <span key={p.id} className={styles[p.id]} style={{height:`${b.counts[p.id]/total(b.counts)*100}%`}}/>)}</span></button>)}</div></div></div>
 <div className={styles.timeAxis}><span>{label(bins[0].date)}</span><span>{label(bins[Math.floor(bins.length/2)].date)}</span><span>{label(bins.at(-1)!.date)} ICT</span></div>
 <div className={styles.chartLegend}>{visiblePlatforms.map(p => <span key={p.id}><i className={styles[p.id]}/>{p.name}</span>)}</div>
 <p className={styles.chartReadout} role="status">{active ? `${label(active.date)} ICT · ${visiblePlatforms.map(p => `${p.name}: ${active.counts[p.id].toLocaleString()}`).join(" · ")}` : ""}</p>
 <details className={styles.chartData}><summary>View daily counts</summary><div><table><caption>Collected observations per day · ICT</caption><thead><tr><th>Date</th>{visiblePlatforms.map(p => <th key={p.id}>{p.name}</th>)}</tr></thead><tbody>{bins.map(b => <tr key={b.date}><td>{b.date}</td>{visiblePlatforms.map(p => <td key={p.id}>{b.counts[p.id].toLocaleString()}</td>)}</tr>)}</tbody></table></div></details></>}
 {!!history?.skippedUndated&&<p className={styles.chartNote}>{history.skippedUndated} observations lack a valid collection time.</p>}
 </section>;
}
