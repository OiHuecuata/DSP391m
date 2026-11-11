"use client";
import { useEffect, useState } from "react";
import type { EngagementPoint } from "@/features/s3/snapshot";
import { EngagementChart, MetricIcon, MiniChart } from "./EngagementChart";
import type { CollectionHistory } from "@/features/s3/status";
import { useS3Status, snapshotDate } from "@/features/s3/status";
import { S3CollectionChart } from "@/features/home/S3CollectionChart";
import type { LocationMentions } from "./location-mentions";
import styles from "./collectors.module.css";
type Metric = {total:number|null;reported:number;records:number};
type Overview = {engagement:EngagementPoint[];reposts:Metric;quotes:Metric;media:Metric;duration:Metric;tags:{name:string;count:number}[];locations:LocationMentions|null;syncedAt:string;posts:number;comments:number|null;firstCollected:string|null;lastCollected:string|null;likes:Metric;views:Metric;replies:Metric;history?:CollectionHistory};
const number = (value:number|null) => value == null ? "—" : value.toLocaleString();
export function CollectedData({source}:{source:"threads"|"youtube"}) {
 const {data:snapshotStatus}=useS3Status();
 const [data,setData]=useState<Overview|null>(null),[error,setError]=useState("");
 const [showAll,setShowAll]=useState(false);
 const [reload,setReload]=useState(0),[loading,setLoading]=useState(true);
 useEffect(()=>{const abort=new AbortController();
 async function load(){try{const response=await fetch(`/api/collected/${source}/overview`,{cache:"no-store",signal:abort.signal});const result=await response.json();if(!response.ok)throw Error(result.error ?? "Overview unavailable");if(!abort.signal.aborted)setData(result);}catch(e){if(!abort.signal.aborted)setError(e instanceof Error?e.message:"Overview unavailable");}finally{if(!abort.signal.aborted)setLoading(false);}}
 void load();return()=>abort.abort();},[source,reload,snapshotStatus?.syncedAt]);
 const name=source === "threads"?"Threads":"YouTube";
 const series=data?.engagement??[];
 const metric=(key:"likes"|"replies"|"views"|"reposts"|"quotes",label:string,color:string)=>({key,label,value:data![key].total,coverage:data![key],color,trend:series.map(p=>p[key]),suffix:""});
 const metrics=data ? [{key:"posts",label:source === "threads"?"Saved posts":"Saved videos",value:data.posts,coverage:null,color:"#147dcc",trend:series.map(p=>p.items),suffix:""},
 metric("likes","Reported likes","#008568"),metric("replies","Reported replies","#d76ba4"),
 ...(source === "threads"?[metric("reposts","Reported reposts","#147dcc"),metric("quotes","Reported quotes","#cd740c"),{key:"media",label:"Posts with media",value:data.media.total,coverage:data.media,color:"#008568",trend:[],suffix:""}]:
 [metric("views","Reported video views","#147dcc"),{key:"replies",label:"Saved comments",value:data.comments,coverage:null,color:"#cd740c",trend:[],suffix:""},{key:"duration",label:"Median video length",value:data.duration.total==null?null:Math.round(data.duration.total/60*10)/10,coverage:data.duration,color:"#008568",trend:[],suffix:" min"}])]:[];
 const primaryMetrics=metrics.filter(m=>source==="threads"?["posts","likes","replies"].includes(m.key):m.label!=="Reported replies"&&m.key!=="duration");
 const extraMetrics=metrics.filter(m=>!primaryMetrics.includes(m));
 return <section aria-label={`${name} overview`} className={styles.collectorOverview}>
 <div className={styles.dataPanel}><div className={styles.dataHeading}><h2>{name} overview</h2><button onClick={()=>{setData(null);setLoading(true);setError("");setReload(v=>v+1);}} disabled={loading}>Refresh</button></div>
 {loading?<p role="status">Loading overview…</p>:error?<p role="alert">{error}</p>:data&&<>
 <div className={styles.illustratedMetrics}>{primaryMetrics.map(m=><article key={m.label} className={styles.illustratedCard}><div className={styles.cardVisual}><MetricIcon kind={m.key}/>{m.trend.length>0&&<MiniChart values={m.trend} color={m.color}/>}</div><span>{m.label}</span><strong>{number(m.value)}{m.value!=null&&m.suffix}</strong></article>)}</div>
 {extraMetrics.length>0&&<details className={styles.overviewDatesDetails}><summary>More metrics</summary><dl>{extraMetrics.map(m=><div key={m.label}><dt>{m.label}</dt><dd>{number(m.value)}{m.value!=null&&m.suffix}</dd></div>)}</dl></details>}
 {source==="youtube"&&data.tags.length>0&&<div className={styles.tagSummary}><span>Common video tags</span><div>{data.tags.map(t=><span key={t.name}>{t.name} <small>{t.count}</small></span>)}</div></div>}

 <details className={styles.overviewDatesDetails}><summary>Collection dates & coverage</summary><div className={styles.overviewDates}><div><span>First collected</span><strong>{snapshotDate(data.firstCollected)}</strong></div><div><span>Latest collection</span><strong>{snapshotDate(data.lastCollected)}</strong></div><div><span>Last sync</span><strong>{snapshotDate(data.syncedAt)}</strong></div></div><ul>{metrics.filter(m=>m.coverage).map(m=><li key={m.label}>{m.label}: {m.coverage!.reported} / {m.coverage!.records} items report a value</li>)}</ul></details>
 </>}
 </div>
 {data&&<div className={styles.overviewRow}><EngagementChart points={data.engagement??[]} source={source}/><section className={styles.dataPanel} aria-labelledby="location-mentions-title"><div className={styles.dataHeading}><h2 id="location-mentions-title" title="Place-name matches in saved text, not verified court locations. Each item counts once per location; multiple locations may be mentioned. YouTube comments are excluded.">Locations mentioned</h2><span>{source === "threads"?"Post text":"Video text"}</span></div>
 {data.locations == null ? <p role="status" className={styles.dataMeta}>Location matching is unavailable.</p> : <>
 <p className={styles.dataMeta}>{data.locations.matched.toLocaleString()} / {data.locations.total.toLocaleString()} {source === "threads"?"posts":"videos"} located</p>
 {data.locations.locations.length ? <ol className={styles.mentionList}>{data.locations.locations.slice(0,showAll?undefined:8).map(location=><li key={location.id}><div><strong>{location.name}</strong><span>{location.count.toLocaleString()} {source === "threads"?"posts":"videos"}</span></div><div className={styles.mentionTrack} aria-hidden="true"><i style={{width:`${location.count / data.locations!.locations[0].count * 100}%`,background:source === "threads"?"#d97706":"#2563eb"}}/></div></li>)}</ol> : <p className={styles.dataMeta}>No recognized locations mentioned in the saved text.</p>}
 {data.locations.locations.length>8&&<button aria-expanded={showAll} onClick={()=>setShowAll(v=>!v)}>{showAll?"Show fewer locations":`Show all ${data.locations.locations.length} locations`}</button>}
 </>}
 </section></div>}
 {data&&<S3CollectionChart history={data.history} loaded={true} source={source}/>}
 </section>;
}
