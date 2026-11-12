'use client';
import type {ReactNode} from 'react';
import type {RidgeDemo} from './ridge-demo';
import styles from './ridge-overview.module.css';
const number=(n:number)=>n.toLocaleString('en-GB',{maximumFractionDigits:2});
const date=(d:string)=>new Date(d+'T00:00:00Z').toLocaleDateString('en-GB',{day:'numeric',month:'short',year:'numeric',timeZone:'UTC'});
export function RidgeOverview({report,selected,onSelect,children}:{report:RidgeDemo;selected:string;onSelect:(id:string)=>void;children:ReactNode}){
 const ranking=[...report.provinces].sort((a,b)=>b.prediction-a.prediction||a.name.localeCompare(b.name)),maximum=Math.max(1,...ranking.map(p=>p.prediction));
 return <div className={styles.overview}>
  <section className={styles.ranking} aria-labelledby='ridge-ranking-title'><h3 id='ridge-ranking-title'>Predicted interest by province</h3><p>Week of {date(report.forecastWeek)}</p><ol>{ranking.map(p=><li key={p.id}><button type='button' onClick={()=>onSelect(p.id)} aria-pressed={selected===p.id} aria-label={`${p.name}: ${number(p.prediction)} predicted intent posts, select province`}><span className={styles.name}>{p.name}</span><span className={styles.track}><i style={{width:`${p.prediction/maximum*100}%`}}/></span><strong>{number(p.prediction)}</strong></button></li>)}</ol></section>
  <div className={styles.province}>{children}</div>

 </div>;
}
