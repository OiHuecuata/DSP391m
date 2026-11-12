'use client';
import {useState} from 'react';
import {RidgeOverview} from './RidgeOverview';
import {useAuth} from '@/features/auth/AuthProvider';
import shapes from './provinces.json';
import {heatmapScale} from './heatmap-scale';
import type {RidgeDemo} from './ridge-demo';
import styles from './ridge-demo.module.css';
const number=(n:number)=>n.toLocaleString('en-GB',{maximumFractionDigits:2});
const date=(d:string)=>new Date(d+'T00:00:00Z').toLocaleDateString('en-GB',{day:'numeric',month:'short',year:'numeric',timeZone:'UTC'});
export function RidgeDemoResults({report}:{report:RidgeDemo}){
 const [selected,setSelected]=useState(()=>report.provinces.reduce((a,b)=>a.prediction>=b.prediction?a:b).id);
 const {user}=useAuth();
 const active=report.provinces.find(p=>p.id===selected)??report.provinces[0];
 const scale=heatmapScale(report.provinces.map(p=>p.prediction),false,'warm',false);
 const fill=scale.fill;
 const series=active.test,top=Math.max(1,...series.map(p=>p.prediction));
 const x=(i:number)=>65+i/Math.max(1,series.length-1)*660,y=(v:number)=>260-v/top*220;
 const path=series.map((p,i)=>`${i?'L':'M'}${x(i)} ${y(p.prediction)}`).join(' ');
 return <section id='ridge-forecast' className={styles.panel} aria-labelledby='ridge-title'>
  <header className={styles.heading}><h2 id='ridge-title'>Provincial interest forecast</h2></header>
  <div className={styles.layout}><div className={styles.map}>
   <svg viewBox='0 0 800 930' role='group' aria-label='Domain forecast by province, 34-province map'><defs><pattern id='ridge-unmodeled' width='7' height='7' patternUnits='userSpaceOnUse'><rect width='7' height='7' fill='#e5e8e1'/><path d='M-1 1 1-1M0 7 7 0M6 8 8 6' stroke='#aab3a2' strokeWidth='.7'/></pattern></defs>
    {shapes.map(shape=>{const p=report.provinces.find(p=>p.id===shape.id);return <path key={shape.id} d={shape.path} fill={p?fill(p.prediction):'url(#ridge-unmodeled)'} stroke='#202b27' strokeWidth={shape.id===active.id?2.4:1.2} role={p?'button':'img'} tabIndex={p?0:undefined} aria-label={`${shape.name}: ${p?number(p.prediction)+' predicted intent posts':'not included in this model'}`} aria-pressed={p?shape.id===active.id:undefined} onClick={()=>{if(p)setSelected(p.id);}} onKeyDown={e=>{if(p&&(e.key==='Enter'||e.key===' ')){e.preventDefault();setSelected(p.id);}}}><title>{shape.name}: {p?number(p.prediction)+' predicted intent posts':'Not included in model'}</title></path>;})}
   </svg><a className={styles.credit} href='https://github.com/TaliaCao/Vietnam-Shape-Map' target='_blank' rel='noreferrer'>Map: Talia Cao · MIT</a>
  </div><aside className={styles.side}>

   <div className={styles.forecast} aria-live='polite'><h3>{active.name}</h3><strong>{number(active.prediction)}</strong><span>Predicted intent posts</span><p>Week of {date(report.forecastWeek)}</p></div>
   <div className={styles.legend} aria-label='Forecast color scale'><h3>Predicted posts</h3>{scale.bands.map(b=><span key={b.label}><i style={{background:b.color}}/>{b.label}</span>)}<span><i style={{background:'repeating-linear-gradient(135deg,#e5e8e1 0 4px,#aab3a2 4px 5px)'}}/>Not included in model</span></div>
  </aside></div>
  <RidgeOverview report={report} selected={active.id} onSelect={setSelected}>
  <section className={styles.comparison} aria-labelledby='ridge-test-title'><h3 id='ridge-test-title'>Predicted interest over time · {active.name}</h3><p>Held-out V3 results · provisional evaluation</p>
   <svg viewBox='0 0 760 310' role='img' aria-label={`Held-out Ridge predicted weekly posts for ${active.name}`}>
    {[0,1,2,3,4].map(i=><g key={i}><line x1='65' x2='725' y1={y(top*i/4)} y2={y(top*i/4)} stroke='#cdd4c7' strokeDasharray='4 5'/><text x='53' y={y(top*i/4)+5} textAnchor='end'>{number(top*i/4)}</text></g>)}
    <path d={path} fill='none' stroke='#dc7309' strokeWidth='3' strokeDasharray='7 4'/>
    {[0,Math.floor((series.length-1)/2),series.length-1].map(i=><text key={i} x={x(i)} y='291' textAnchor={i===0?'start':i===series.length-1?'end':'middle'}>{date(series[i].week)}</text>)}
   </svg><div className={styles.chartKey}><span><i style={{background:'#dc7309'}}/>Trend predict</span></div>
  </section>
  </RidgeOverview>
  <details className={styles.details}><summary>Evaluation results</summary><div className={styles.metrics}><div><span>Ridge MAE</span><strong>{number(report.metrics.mae)}</strong></div><div><span>Baseline MAE</span><strong>{number(report.metrics.baselineMae)}</strong></div><div><span>Ridge RMSE</span><strong>{number(report.metrics.rmse)}</strong></div><div><span>Baseline RMSE</span><strong>{number(report.metrics.baselineRmse)}</strong></div></div><p>Dataset: {report.originalPosts.toLocaleString()} original posts and {report.simulatedPosts.toLocaleString()} simulated posts. Three walk-forward test folds. Spark reported solver warnings in the evaluation folds, so these scores are provisional. The final forecast model passed a separate numerical check. The baseline uses a moving average; these scores do not validate real-world demand.</p>{user?.role==='admin'&&<p>Run: {report.runId}<br/>Dataset: {report.datasetId}</p>}</details>
 </section>;
}
