'use client';
import Link from 'next/link';
import {useAuth} from '@/features/auth/AuthProvider';
import {useState} from 'react';
import {useMapData} from './useMapData';
import {ModelResults} from './ModelResults';
import type {RidgeDemo} from './ridge-demo';
import type {RidgeReadiness} from './ridge-report';
import type {ModelReport} from './model-report';
import {compareProvinces, type SocialSource} from './market-comparison';
import styles from './demand-supply.module.css';
const labels = {threads:'Threads', youtube:'YouTube'};
const percent = (value:number|null) => value === null ? '—' : `${value.toFixed(1)}%`;
export function DemandSupplyAnalysis({model,ridge,forecast}:{model:ModelReport|null;ridge:RidgeReadiness|null;forecast:RidgeDemo|null}) {
  const {data,error}=useMapData();
  const {user}=useAuth(),admin=user?.role==="admin";
  const [sources,setSources]=useState<SocialSource[]>(['threads','youtube']);
  const [selected,setSelected]=useState(''),[query,setQuery]=useState('');
  const [sort,setSort]=useState('social');

  const rows=data?compareProvinces(data,sources):[];
  const normalize=(value:string)=>value.normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/đ/g,'d').toLowerCase();
  const table=rows.filter(row=>normalize(row.name).includes(normalize(query))).sort((a,b)=>sort==='listings'?b.pickleballplus-a.pickleballplus:sort==='difference'?(b.difference??-Infinity)-(a.difference??-Infinity):b.social-a.social);
  const matchedSocial=data?sources.reduce((n,s)=>n+data.matched[s],0):0;
  return <main className={styles.page}>
    <header className={styles.heading}><div><h1>Analytics</h1></div><Link href='/dashboard'>View geographic overview ↗</Link></header>
    <ModelResults clustering={model} ridge={ridge} forecast={forecast}/>
    {admin&&error&&<p role='alert' className={styles.error}>{error}</p>}
    {admin&&!data&&!error&&<p role='status'>Loading province evidence…</p>}
    {admin&&data&&<>
      <section className={styles.toolbar} aria-label='Comparison sources'><div><h2>Social activity signals</h2></div><fieldset><legend>Include sources</legend>{(['threads','youtube'] as const).map(source=><label key={source}><input type='checkbox' checked={sources.includes(source)} onChange={event=>setSources(previous=>event.target.checked?[...previous,source]:previous.filter(s=>s!==source))}/>{labels[source]}</label>)}</fieldset></section>
      {admin&&<div className={styles.metrics}><article><span>Mapped venue listings</span><strong>{data.matched.pickleballplus.toLocaleString()}</strong></article><article><span>Located social items</span><strong>{matchedSocial.toLocaleString()}</strong></article><article><span>Provinces with both</span><strong>{rows.filter(row=>row.pickleballplus>0&&row.social>0).length}<small> / 34</small></strong></article></div>}
      {admin&&<section className={styles.panel} aria-labelledby='evidence-title'><div className={styles.sectionHeading}><h2 id='evidence-title'>Province evidence</h2><div className={styles.filters}><label>Find a province<input value={query} onChange={event=>setQuery(event.target.value)} placeholder='Province name'/></label><label>Sort by<select value={sort} onChange={event=>setSort(event.target.value)}><option value='social'>Social mentions</option><option value='listings'>Venue listings</option><option value='difference'>Share difference</option></select></label></div></div><div className={styles.table}><table><thead><tr><th>Province</th><th>Venue listings</th>{sources.map(source=><th key={source}>{labels[source]}</th>)}<th>Listing share</th><th>Social share</th><th>Difference (pp)</th></tr></thead><tbody>{table.map(row=><tr key={row.id} data-selected={row.id===selected}><th scope='row'><button onClick={()=>setSelected(row.id)}>{row.name}</button></th><td>{row.pickleballplus}</td>{sources.map(source=><td key={source}>{row[source]}</td>)}<td>{percent(row.listingShare)}</td><td>{percent(row.socialShare)}</td><td>{row.difference===null?'—':`${row.difference>0?'+':''}${row.difference.toFixed(1)}`}</td></tr>)}</tbody></table>{!table.length&&<p>No provinces match your search.</p>}</div><details className={styles.method}><summary>What does the difference mean?</summary><p>Social share minus listing share, in percentage points. A positive number describes a relative difference in this sample; it does not establish a court shortage.</p></details></section>}

    </>}
  </main>;
}
