"use client";
import {useState} from 'react';
import type {MapData,Source} from './types';
import styles from './dashboard-charts.module.css';
const labels:Record<Source,string>={threads:'Threads posts',youtube:'YouTube videos',pickleballplus:'Court listings'};
const colors:Record<Source,string>={threads:'#d87409',youtube:'#2876d0',pickleballplus:'#238b6c'};
const historyKeys={threads:'threads',youtube:'youtube',pickleballplus:'pbplus'} as const;
const compact=(n:number)=>Intl.NumberFormat('en',{notation:'compact',maximumFractionDigits:1}).format(n);
const date=(s:string)=>new Date(s+'T00:00:00Z').toLocaleDateString('en-GB',{month:'short',day:'numeric',timeZone:'UTC'});
export function DashboardCharts({data,sources}:{data:MapData;sources:Source[]}){
 const [range,setRange]=useState<'all'|'7d'>('all');
 const [selectedDate,setSelectedDate]=useState<string|null>(null);
 const days=[...(data.history?.days??[])].sort((a,b)=>a.date.localeCompare(b.date));
 const end=days.length?Date.parse(days.at(-1)!.date):0;
 const observed=days.filter(d=>range==='all'||Date.parse(d.date)>=end-6*86400000);
 const start=observed.length?Date.parse(observed[0].date):end;
 const byDate=new Map(observed.map(d=>[d.date,d]));
 const rows=observed.length&&(end-start)/86400000<=3660?Array.from({length:Math.floor((end-start)/86400000)+1},(_,i)=>{const day=new Date(start+i*86400000).toISOString().slice(0,10);return byDate.get(day)??{date:day,counts:{threads:0,youtube:0,pbplus:0}};}):observed;
 const max=Math.max(1,...rows.flatMap(d=>sources.map(s=>d.counts[historyKeys[s]])));
 const step=Math.max(1,Math.ceil(max/4/10**Math.floor(Math.log10(max/4)))*10**Math.floor(Math.log10(max/4)));
 const ceiling=step*4;
 const x=(d:string)=>60+(end===start?450:(Date.parse(d)-start)/(end-start)*910);
 const y=(n:number)=>215-n/ceiling*175;
 const total=sources.reduce((n,s)=>n+data.totals[s],0);
 const active=rows.find(d=>d.date===selectedDate);
 const social=sources.filter((s):s is 'threads'|'youtube'=>s!=='pickleballplus');
 const metricLabels={likes:'Likes',replies:'Replies / comments',reposts:'Reposts',quotes:'Quotes'};
 const metrics=(['likes','replies','reposts','quotes'] as const).filter(key=>social.some(s=>data.interactions?.find(r=>r.source===s)?.metrics.some(m=>m.key===key&&m.reported>0)));
 const interactionMax=(key:string)=>Math.max(1,...(data.interactions??[]).filter(r=>social.includes(r.source)).flatMap(r=>r.metrics.filter(m=>m.key===key).map(m=>m.total))); 
 return <div className={styles.grid}>
 <section className={`${styles.card} ${styles.wide}`} id='collection-activity' aria-labelledby='activity-title'>
  <div className={styles.heading}><div><h2 id='activity-title' title='Collected observations, including repeated collections. YouTube includes videos and comments.'>Posts and listings over time</h2></div><div className={styles.tabs}>{(['all','7d'] as const).map(r=><button key={r} aria-pressed={range===r} onClick={()=>{setRange(r);setSelectedDate(null);}}>{r==='all'?'All dates':'Latest 7 days'}</button>)}</div></div>
  {!sources.length?<p className={styles.empty}>Choose chart sources in Settings.</p>:!rows.length?<p className={styles.empty}>Collection history is unavailable in this snapshot.</p>:<>
  <div className={styles.chartViewport} tabIndex={0} role='region' aria-label='Timeline chart; scroll horizontally on small screens'><svg viewBox='0 0 1000 270' className={styles.lineChart} role='img' aria-label='Daily collected observations by selected source'>
   {[0,.25,.5,.75,1].map(t=><g key={t}><line x1='60' x2='970' y1={y(t*ceiling)} y2={y(t*ceiling)} stroke='#cbd3c4' strokeDasharray='4 5'/><text x='48' y={y(t*ceiling)+4} textAnchor='end'>{compact(t*ceiling)}</text></g>)}
   {sources.map(s=><g key={s}><path d={rows.map((r,i)=>`${i?'L':'M'}${x(r.date)},${y(r.counts[historyKeys[s]])}`).join(' ')} stroke={colors[s]} strokeWidth='2.7' fill='none' strokeLinejoin='round'/>{rows.map(r=><circle key={r.date} cx={x(r.date)} cy={y(r.counts[historyKeys[s]])} r={active?.date===r.date?5.5:3.5} fill={colors[s]}><title>{date(r.date)} · {s==='youtube'?'YouTube videos & comments':labels[s]}: {r.counts[historyKeys[s]].toLocaleString()}</title></circle>)}</g>)}
   {[...new Set([0,Math.floor((rows.length-1)/2),rows.length-1])].map(i=><text key={i} x={x(rows[i].date)} y='250' textAnchor='middle'>{date(rows[i].date)}</text>)}
  </svg></div><div className={styles.footer}><div className={styles.legend}>{sources.map(s=><span key={s}><i style={{background:colors[s]}}/>{s==='youtube'?'YouTube videos & comments':labels[s]}</span>)}</div><label>Date<select value={active?.date??''} onChange={e=>setSelectedDate(e.target.value||null)}><option value=''>Choose a date</option>{rows.map(r=><option key={r.date} value={r.date}>{date(r.date)} · {r.date.slice(0,4)}</option>)}</select></label></div>
  {active&&<div className={styles.readout}>{sources.map(s=><span key={s}>{s==='youtube'?'YouTube':labels[s]} <strong>{active.counts[historyKeys[s]].toLocaleString()}</strong></span>)}</div>}
  <details className={styles.details}><summary>View daily counts</summary><div className={styles.table}><table><thead><tr><th>Date</th>{sources.map(s=><th key={s}>{s==='youtube'?'YouTube videos & comments':labels[s]}</th>)}</tr></thead><tbody>{rows.map(r=><tr key={r.date}><th>{r.date}</th>{sources.map(s=><td key={s}>{r.counts[historyKeys[s]].toLocaleString()}</td>)}</tr>)}</tbody></table></div></details></>}
 </section>
 <section className={styles.card} aria-labelledby='mix-title'><h2 id='mix-title'>Saved record mix</h2>{total>0?<div className={styles.mix}><svg viewBox='0 0 200 200' className={styles.donut} role='img' aria-label='Share of saved records by selected source'><circle cx='100' cy='100' r='72' fill='none' stroke='#edf0e6' strokeWidth='25'/>{sources.map((s,index)=>{const share=data.totals[s]/total*100;const old=sources.slice(0,index).reduce((n,source)=>n+data.totals[source]/total*100,0);return <circle key={s} cx='100' cy='100' r='72' pathLength='100' fill='none' stroke={colors[s]} strokeWidth='25' strokeDasharray={`${share} ${100-share}`} strokeDashoffset={-old} transform='rotate(-90 100 100)'><title>{labels[s]}: {data.totals[s].toLocaleString()} ({share.toFixed(1)}%)</title></circle>;})}<text x='100' y='100' textAnchor='middle' className={styles.donutTotal}>{compact(total)}</text><text x='100' y='120' textAnchor='middle'>saved records</text></svg><div className={styles.mixLabels}>{sources.map(s=><div key={s}><span><i style={{background:colors[s]}}/>{labels[s]}</span><strong>{data.totals[s].toLocaleString()}</strong><small>{(data.totals[s]/total*100).toFixed(1)}%</small></div>)}</div></div>:<p className={styles.empty}>No saved records for the selected sources.</p>}</section>
 <section className={styles.card} aria-labelledby='coverage-title'><h2 id='coverage-title'>Province-match coverage</h2><div className={styles.coverage}>{sources.map(s=>{const n=data.totals[s],matched=data.matched[s];return <div key={s}><div className={styles.barHeading}><span>{labels[s]}</span><strong>{n?Math.round(matched/n*100):0}%</strong></div><div className={styles.track} role='img' aria-label={`${labels[s]}: ${matched} with a province match, ${n-matched} without`}><i style={{width:`${n?matched/n*100:0}%`,background:colors[s]}}/></div><div className={styles.barHeading}><small>{matched.toLocaleString()} matched</small><small>{(n-matched).toLocaleString()} unmatched</small></div></div>;})}</div>{!sources.length&&<p className={styles.empty}>Choose chart sources in Settings.</p>}<details className={styles.details}><summary>What counts as a match?</summary><p>Place names in post text or venue addresses are matched to the 34-province reference. Social mentions are not verified court locations.</p></details></section>
 <section className={`${styles.card} ${styles.wide}`} aria-labelledby='interactions-title'><div className={styles.heading}><div><h2 id='interactions-title' title='Recorded totals on saved posts and videos, not new interactions gained.'>Recorded social interactions</h2></div><div className={styles.legend}>{social.map(s=><span key={s}><i style={{background:colors[s]}}/>{s==='threads'?'Threads':'YouTube'}</span>)}</div></div>{!metrics.length?<p className={styles.empty}>{social.length?'Interaction counts are unavailable in this snapshot.':'Choose Threads or YouTube in Settings.'}</p>:<div className={styles.interactions}>{metrics.map(key=><div key={key} className={styles.metric}><h3>{metricLabels[key]}</h3>{social.map(s=>{const m=data.interactions?.find(r=>r.source===s)?.metrics.find(m=>m.key===key);return <div key={s} className={styles.metricRow}><span>{s==='threads'?'Threads':'YouTube'}</span><div className={styles.track}><i style={{width:`${(m?.total??0)/interactionMax(key)*100}%`,background:colors[s]}}/></div><strong title={m?.reported?`${m.reported} items report this count`:'Not reported'}>{m?.reported?m.total.toLocaleString():'—'}</strong></div>;})}</div>)}</div>}</section>
 </div>;
}
