'use client';
import Link from 'next/link';
import {useState} from 'react';
import {useMapData} from './useMapData';
import {DemandForecastMap} from './DemandForecastMap';
import {compareProvinces, type SocialSource} from './market-comparison';
import styles from './demand-supply.module.css';
const labels = {threads:'Threads', youtube:'YouTube'};
const percent = (value:number|null) => value === null ? '—' : `${value.toFixed(1)}%`;
export function DemandSupplyAnalysis() {
  const {data,error}=useMapData();
  const [sources,setSources]=useState<SocialSource[]>(['threads','youtube']);
  const [selected,setSelected]=useState(''),[query,setQuery]=useState('');
  const [sort,setSort]=useState('social');

  const rows=data?compareProvinces(data,sources):[];
  const active=rows.find(row=>row.id===selected);
  const plotted=rows.filter(row=>row.pickleballplus>0||row.social>0);
  const scale=Math.max(20,Math.ceil(Math.max(0,...rows.flatMap(row=>[row.listingShare??0,row.socialShare??0]))/20)*20);
  const x=(n:number)=>70+n/scale*610,y=(n:number)=>340-n/scale*290;
  const normalize=(value:string)=>value.normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/đ/g,'d').toLowerCase();
  const table=rows.filter(row=>normalize(row.name).includes(normalize(query))).sort((a,b)=>sort==='listings'?b.pickleballplus-a.pickleballplus:sort==='difference'?(b.difference??-Infinity)-(a.difference??-Infinity):b.social-a.social);
  const totalSocial=data?sources.reduce((n,s)=>n+data.totals[s],0):0;
  const matchedSocial=data?sources.reduce((n,s)=>n+data.matched[s],0):0;
  return <main className={styles.page}>
    <header className={styles.heading}><div><h1>Analytics</h1></div><Link href='/dashboard'>View geographic overview ↗</Link></header>
    <DemandForecastMap/>
    {error&&<p role='alert' className={styles.error}>{error}</p>}
    {!data&&!error&&<p role='status'>Loading province comparisons…</p>}
    {data&&<>
      <section className={styles.toolbar} aria-label='Comparison sources'><div><h2>Social activity signals</h2></div><fieldset><legend>Include sources</legend>{(['threads','youtube'] as const).map(source=><label key={source}><input type='checkbox' checked={sources.includes(source)} onChange={event=>setSources(previous=>event.target.checked?[...previous,source]:previous.filter(s=>s!==source))}/>{labels[source]}</label>)}</fieldset></section>
      <div className={styles.metrics}><article><span>Mapped venue listings</span><strong>{data.matched.pickleballplus.toLocaleString()}</strong><small>of {data.totals.pickleballplus.toLocaleString()} available listings</small></article><article><span>Located social items</span><strong>{matchedSocial.toLocaleString()}</strong><small>of {totalSocial.toLocaleString()} selected posts and videos</small></article><article><span>Provinces with both</span><strong>{rows.filter(row=>row.pickleballplus>0&&row.social>0).length}<small> / 34</small></strong><small>Listings and selected social mentions</small></article></div>
      <section className={styles.panel} aria-labelledby='comparison-title'><div className={styles.sectionHeading}><div><h2 id='comparison-title'>Supply & social interest</h2></div><label className={styles.provincePicker}>Inspect province<select value={selected} onChange={event=>setSelected(event.target.value)}><option value=''>Choose a province</option>{rows.map(row=><option key={row.id} value={row.id}>{row.name}</option>)}</select></label></div>
      <div className={styles.key}><i/>Province <b>╱</b> Equal shares</div>
      {!sources.length?<p className={styles.empty}>Select Threads or YouTube to compare social activity with listings.</p>:!plotted.length||!matchedSocial||!data.matched.pickleballplus?<p className={styles.empty}>Both mapped court listings and located social items are needed for this comparison.</p>:<div className={styles.comparison}><div className={styles.chart}><svg viewBox='0 0 740 400' role='group' aria-label='Province share of venue listings versus share of social mentions'>
        <text x='70' y='20'>Share of social mentions (%)</text>
        {[0,1,2,3,4].map(i=>{const value=scale*i/4;return <g key={i}><line x1='70' x2='680' y1={y(value)} y2={y(value)} stroke='#d4dbcf' strokeDasharray='4 5'/><text x='58' y={y(value)+4} textAnchor='end'>{value.toFixed(0)}</text><text x={x(value)} y='363' textAnchor='middle'>{value.toFixed(0)}</text></g>;})}
        <line x1='70' y1='340' x2='680' y2='340' stroke='#64725e'/><line x1='70' y1='50' x2='70' y2='340' stroke='#64725e'/><line x1='70' y1='340' x2='680' y2='50' stroke='#64725e' strokeDasharray='6 6'/>
        {[...plotted].sort((a,b)=>Number(a.id===selected)-Number(b.id===selected)).map(row=><circle key={row.id} cx={x(row.listingShare??0)} cy={y(row.socialShare??0)} r={selected===row.id?8:6} fill={selected===row.id?'#d87409':'#2876d0'} fillOpacity='.85' stroke='#fffefa' strokeWidth='1.5' role='button' tabIndex={0} aria-label={`${row.name}: ${row.pickleballplus} venue listings, ${row.social} social mentions`} aria-pressed={selected===row.id} onClick={()=>setSelected(row.id)} onKeyDown={event=>{if(event.key==='Enter'||event.key===' '){event.preventDefault();setSelected(row.id);}}}><title>{row.name}: {percent(row.listingShare)} of listings, {percent(row.socialShare)} of mentions</title></circle>)}
        <text x='375' y='394' textAnchor='middle'>Share of venue listings (%)</text>
      </svg></div><aside className={styles.province} aria-live='polite'>{active?<><h3>{active.name}</h3><dl><div><dt>Venue listings</dt><dd>{active.pickleballplus}</dd></div>{sources.map(source=><div key={source}><dt>{labels[source]} mentions</dt><dd>{active[source]}</dd></div>)}<div><dt>Listing share</dt><dd>{percent(active.listingShare)}</dd></div><div><dt>Social mention share</dt><dd>{percent(active.socialShare)}</dd></div></dl><Link href={`/search?location=${encodeURIComponent(active.id)}`}>Inspect province records ↗</Link><button onClick={()=>setSelected('')}>Clear selection</button></>:<><h3>Read the comparison</h3><p>Above: higher social share.</p><p>Below: higher listing share.</p><p>Shares within this dataset.</p></>}</aside></div>}
      <details className={styles.method}><summary>How to read this</summary><p>Social mentions indicate interest, not measured player demand or court shortages.</p><p>Shares use the province totals for the selected sources. A social item can mention several provinces and count once in each. Venue listings are locations, not individual playing courts. This is a comparison of sampled records, not a shortage score or causal result.</p><p>{(data.totals.pickleballplus-data.matched.pickleballplus).toLocaleString()} listings and {(totalSocial-matchedSocial).toLocaleString()} selected social items have no province match and are excluded. YouTube counts videos; comments are available in Explore data but are not assigned independently on this map.</p></details></section>
      <section className={styles.panel} aria-labelledby='evidence-title'><div className={styles.sectionHeading}><h2 id='evidence-title'>Province evidence</h2><div className={styles.filters}><label>Find a province<input value={query} onChange={event=>setQuery(event.target.value)} placeholder='Province name'/></label><label>Sort by<select value={sort} onChange={event=>setSort(event.target.value)}><option value='social'>Social mentions</option><option value='listings'>Venue listings</option><option value='difference'>Share difference</option></select></label></div></div><div className={styles.table}><table><thead><tr><th>Province</th><th>Venue listings</th>{sources.map(source=><th key={source}>{labels[source]}</th>)}<th>Listing share</th><th>Social share</th><th>Difference (pp)</th></tr></thead><tbody>{table.map(row=><tr key={row.id} data-selected={row.id===selected}><th scope='row'><button onClick={()=>setSelected(row.id)}>{row.name}</button></th><td>{row.pickleballplus}</td>{sources.map(source=><td key={source}>{row[source]}</td>)}<td>{percent(row.listingShare)}</td><td>{percent(row.socialShare)}</td><td>{row.difference===null?'—':`${row.difference>0?'+':''}${row.difference.toFixed(1)}`}</td></tr>)}</tbody></table>{!table.length&&<p>No provinces match your search.</p>}</div><details className={styles.method}><summary>What does the difference mean?</summary><p>Social share minus listing share, in percentage points. A positive number describes a relative difference in this sample; it does not establish a court shortage.</p></details></section>
      <section className={styles.next} aria-labelledby='next-title'><div><h2 id='next-title'>What’s next</h2><details><summary>Additional data needed</summary><p>Population and area for court density. Google Trends and validated labels for stronger demand evidence.</p></details></div><div><h3>Forecasts & model results</h3><span>Awaiting model</span><Link href='/dashboard'>View recorded activity over time ↗</Link></div></section>
    </>}
  </main>;
}
