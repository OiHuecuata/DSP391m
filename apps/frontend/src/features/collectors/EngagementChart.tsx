"use client";
import { useId, useState } from "react";
import type { EngagementPoint } from "@/features/s3/snapshot";
import styles from "./collectors.module.css";
export const engagementLines=[{key:"likes",label:"Likes",color:"#008568"},{key:"replies",label:"Replies",color:"#d76ba4"},{key:"reposts",label:"Reposts",color:"#147dcc"},{key:"quotes",label:"Quotes",color:"#cd740c"}] as const;
const dateLabel=(value:string)=>new Date(value+"T00:00:00Z").toLocaleDateString("en-GB",{day:"numeric",month:"short",timeZone:"UTC"});
export function MiniChart({values,color}:{values:(number|null)[];color:string}) {
 const valid=values.filter((v):v is number=>v!=null);
 if(valid.length<2)return <span className={styles.noTrend}>No trend</span>;
 const max=Math.max(1,...valid),min=Math.min(0,...valid);
 const points=values.map((v,i)=>v==null?null:`${i/Math.max(1,values.length-1)*84},${28-(v-min)/(max-min)*24}`);
 let path="";let drawing=false;for(const point of points){if(point){path+=`${drawing?" L":" M"}${point}`;drawing=true;}else drawing=false;}
 return <svg viewBox="0 0 88 34" className={styles.miniChart} aria-hidden="true"><path d={path} fill="none" stroke={color} strokeWidth="2.3" strokeLinecap="round" strokeLinejoin="round"/></svg>;
}
export function MetricIcon({kind}:{kind:string}) {
 const paths:Record<string,string>={posts:"M5 3h10l4 4v14H5z M14 3v5h5 M8 12h8 M8 16h6",likes:"M12 20S3 14 3 8a5 5 0 0 1 9-3 5 5 0 0 1 9 3c0 6-9 12-9 12z",replies:"M4 4h16v12H9l-5 4z",reposts:"M4 8h15l-4-4 M20 16H5l4 4 M19 8v4 M5 16v-4",quotes:"M4 5h7v8H7l-3 5v-5z M14 5h7v8h-4l-3 5v-5z",media:"M3 4h18v16H3z M3 16l5-5 4 4 4-6 5 7 M7 8h.01",views:"M2 12s4-7 10-7 10 7 10 7-4 7-10 7S2 12 2 12z M15 12a3 3 0 1 1-6 0 3 3 0 0 1 6 0",duration:"M12 3a9 9 0 1 1-.01 0 M12 7v6l4 2"};
 return <svg width="23" height="23" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d={paths[kind]??paths.posts}/></svg>;
}
export function EngagementChart({points,source}:{points:EngagementPoint[];source:"threads"|"youtube"}) {
 const id=useId().replace(/:/g,"");const [range,setRange]=useState<"all"|"7d">("all");const [selected,setSelected]=useState<string|null>(null);
 const end=points.length?Date.parse(points.at(-1)!.date):0;
 const rows=range==="all"?points:points.filter(p=>Date.parse(p.date)>=end-6*86400000);
 const lines=engagementLines.filter(l=>(source==="threads"||l.key==="likes"||l.key==="replies")&&rows.some(p=>p[l.key]!=null));
 const max=Math.max(1,...rows.flatMap(p=>lines.map(l=>p[l.key]??0)));const ceiling=Math.ceil(max/10**Math.floor(Math.log10(max)))*10**Math.floor(Math.log10(max));
 const start=rows.length?Date.parse(rows[0].date):0;const last=rows.length?Date.parse(rows.at(-1)!.date):start;
 const x=(p:EngagementPoint)=>60+(last===start?310:(Date.parse(p.date)-start)/(last-start)*620);
 const y=(value:number)=>185-value/ceiling*155;
 const active=rows.find(p=>p.date===selected)??rows.at(-1);
 return <section className={styles.dataPanel} aria-labelledby="engagement-title"><div className={styles.dataHeading}><h2 id="engagement-title" title="Recorded totals for content collected each day; samples may differ. These are not newly gained interactions.">Engagement over time</h2><div className={styles.dataTabs}>{(["all","7d"]as const).map(r=><button key={r} aria-pressed={range===r} onClick={()=>{setRange(r);setSelected(null);}}>{r==="all"?"All dates":"Latest 7 days"}</button>)}</div></div>
 {!lines.length?<p className={styles.dataMeta}>Engagement history is unavailable in this snapshot.</p>:<>
 <svg viewBox="0 0 710 225" className={styles.engagementSvg} role="img" aria-label="Reported engagement for content collected on each date"><defs>{lines.map(l=><linearGradient id={`${id}-${l.key}`} key={l.key} x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={l.color} stopOpacity=".16"/><stop offset="100%" stopColor={l.color} stopOpacity="0"/></linearGradient>)}</defs>
 {[0,.25,.5,.75,1].map(t=><g key={t}><line x1="60" x2="680" y1={y(t*ceiling)} y2={y(t*ceiling)} stroke="#dfe3da" strokeDasharray="4 5"/><text x="48" y={y(t*ceiling)+4} textAnchor="end">{Intl.NumberFormat("en",{notation:"compact"}).format(t*ceiling)}</text></g>)}
 {lines.map(l=>{let path="";let drawing=false;for(const p of rows){const value=p[l.key];if(value==null){drawing=false;continue;}path+=`${drawing?" L":" M"}${x(p)},${y(value)}`;drawing=true;}
 const full=rows.every(p=>p[l.key]!=null);return <g key={l.key}>{full&&rows.length>1&&<path d={`${path} L${x(rows.at(-1)!)},185 L${x(rows[0])},185 Z`} fill={`url(#${id}-${l.key})`}/>}<path d={path} fill="none" stroke={l.color} strokeWidth="2.5" strokeLinejoin="round" strokeLinecap="round"/>{rows.filter(p=>p[l.key]!=null).map(p=><circle key={p.date} cx={x(p)} cy={y(p[l.key]!)} r="4" fill={l.color}><title>{dateLabel(p.date)} · {l.label}: {p[l.key]!.toLocaleString()} · {p.items} items</title></circle>)}</g>;})}
 {[...new Set([0,Math.floor((rows.length-1)/2),rows.length-1])].filter(i=>i>=0).map(i=><text key={i} x={x(rows[i])} y="211" textAnchor="middle">{dateLabel(rows[i].date)}</text>)}
 </svg><div className={styles.engagementFooter}><div className={styles.engagementLegend}>{lines.map(l=><span key={l.key}><i style={{background:l.color}}/>{l.label}</span>)}</div>
 <label className={styles.engagementDate}>Collection date<select value={active?.date??""} onChange={e=>setSelected(e.target.value)}>{rows.map(p=><option key={p.date} value={p.date}>{dateLabel(p.date)} · {p.date.slice(0,4)}</option>)}</select></label></div>
 <details className={styles.engagementTable}><summary>View engagement counts</summary><div><table><thead><tr><th>Collection date · ICT</th><th>Items</th>{lines.map(l=><th key={l.key}>{l.label}</th>)}</tr></thead><tbody>{rows.map(p=><tr key={p.date}><td>{p.date}</td><td>{p.items}</td>{lines.map(l=><td key={l.key}>{p[l.key]?.toLocaleString()??"—"} <small>({p[`${l.key}Reported`]} reported)</small></td>)}</tr>)}</tbody></table></div></details>
 </>}
 </section>;
}
