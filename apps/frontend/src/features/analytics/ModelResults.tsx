'use client';
import Link from 'next/link';
import {RidgeDemoResults} from './RidgeDemoResults';
import type {RidgeDemo} from './ridge-demo';
import {useSyncExternalStore} from 'react';
import {useAuth} from '@/features/auth/AuthProvider';
import {ModelClusters} from './ModelClusters';
import type {ModelReport} from './model-report';
import type {RidgeReadiness} from './ridge-report';
import styles from './model-results.module.css';
function subscribe(callback:()=>void){window.addEventListener('hashchange',callback);window.addEventListener('popstate',callback);return()=>{window.removeEventListener('hashchange',callback);window.removeEventListener('popstate',callback);};}
const ridgeSelected=()=>window.location.hash==='#ridge-forecast';
export function ModelResults({clustering,ridge,forecast}:{clustering:ModelReport|null;ridge:RidgeReadiness|null;forecast:RidgeDemo|null}){
 const selected=useSyncExternalStore(subscribe,ridgeSelected,()=>false),{user}=useAuth(),admin=user?.role==='admin';
 return <>
  <nav className={styles.models} aria-label='Analysis models'><a href='#model-title' aria-current={!selected?'true':undefined}>Topic cluster</a><a href='#ridge-forecast' aria-current={selected?'true':undefined}>Domain forecast</a></nav>
  {selected&&forecast?<RidgeDemoResults report={forecast}/>:selected?<section id='ridge-forecast' className={styles.panel} aria-labelledby='ridge-title'>
   <header className={styles.heading}><h2 id='ridge-title'>Domain forecast</h2><span className={styles.badge}>Unavailable</span></header>
   <div className={styles.empty} role='status'><svg viewBox='0 0 32 32' fill='none' stroke='currentColor' strokeWidth='1.7' aria-hidden='true'><path d='M5 5v22h23M9 22l6-7 5 3 8-10'/><path d='M23 8h5v5'/></svg><h3>No verified forecast yet</h3><p>No validated Domain forecast is available to display.</p><a href='#model-title'>View clustering results <span aria-hidden='true'>↗</span></a></div>
   {admin&&<div className={styles.audit} aria-labelledby='ridge-audit-title'><h3 id='ridge-audit-title'>Recorded Ridge readiness</h3>{ridge?<><p className={styles.date}>Audit: {new Date(ridge.recordedAt).toLocaleDateString('en-GB',{day:'numeric',month:'short',year:'numeric',timeZone:'UTC'})}. This is a recorded audit, not a live S3 assessment.</p><dl><div><dt>Eligible provinces</dt><dd>{ridge.eligible} / {ridge.required} required</dd></div><div><dt>Eligible in this audit</dt><dd>{ridge.provinces.join(', ')}</dd></div><div><dt>Forecast horizon</dt><dd>{ridge.horizonWeeks} week</dd></div><div><dt>Final model and real forecasts</dt><dd>Not produced</dd></div></dl><p>The recorded run was blocked by insufficient historical coverage. S3 updates must be processed and verified by the model pipeline before forecasts can be published.</p></>:<p>The Ridge readiness report is unavailable.</p>}<p>The prediction target is location-linked intent posts, not bookings or verified market demand. Synthetic test results are excluded.</p><Link href='/data-operations?source=storage'>Review S3 and data readiness ↗</Link></div>}
  </section>:<ModelClusters report={clustering}/>}
 </>;
}
