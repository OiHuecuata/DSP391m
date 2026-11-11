'use client';
import Link from 'next/link';
import {useState} from 'react';
import {usePreferences} from '@/features/settings/PreferencesProvider';
import {useMapData} from './useMapData';
import shapes from './provinces.json';
import {DashboardDetails} from './DashboardDetails';
import {heatmapOptions,heatmapValues,heatmapUnit,type HeatmapType,type AttentionSource} from './heatmap-values';
import {heatmapScale} from './heatmap-scale';
import styles from './analytics.module.css';
const format=(value:number|null,signed=false)=>value===null?'No data':`${signed&&value>0?'+':''}${value.toLocaleString('en-GB',{maximumFractionDigits:1})}`;
export function DashboardMap(){const {ready}=usePreferences();return ready?<DashboardContent/>:<main className={styles.page}><h1>Dashboard</h1><p>Loading preferences…</p></main>;}
function DashboardContent(){
 const {preferences}=usePreferences(),{data,error}=useMapData();
 const [type,setType]=useState<HeatmapType>('balance'),[source,setSource]=useState<AttentionSource>('threads'),[month,setMonth]=useState('');
 const [selected,setSelected]=useState<string|null>(preferences.province||null),[hovered,setHovered]=useState<string|null>(null);

 const months=data?.heatmap?.months??[],activeMonth=month==='all'?'all':months.includes(month)?month:months.at(-1)??'';
 const values=data?heatmapValues(data,type,source,activeMonth):new Map<string,number|null>();
 const available=[...values.values()].filter((n):n is number=>n!==null),signed=type==='balance';
 const colorScale=heatmapScale([...values.values()],signed,preferences.palette,type!=='engagement');
 const fill=(value:number|null)=>value===null?'url(#missing-heatmap-value)':colorScale.fill(value);
 const unit=heatmapUnit(type,source),active=data?.provinces.find(row=>row.id===selected),label=heatmapOptions.find(option=>option.id===type)!.label;
 const measurement=type==='balance'?'Social mention share minus venue listing share.':type==='supply'?'Sum of reported playing courts; missing counts are excluded.':type==='attention'?source==='threads'?'Likes + replies + reposts + quotes.':'Recorded video views.':type==='engagement'?source==='threads'?'Average likes + replies + reposts + quotes per post.':'Average likes + comments per video.':'Unique saved posts and videos, grouped by publication month (ICT).';
 return <main className={styles.page}>
 <header className={styles.heading}><h1>Dashboard</h1><Link href='/analytics'>Compare supply & interest ↗</Link></header>
 {error&&<p role='alert' className={styles.error}>{error}</p>}{!data&&!error&&<p role='status'>Loading map…</p>}
 {data&&<><section className={styles.panel} aria-label='Vietnam province heat map'><div className={styles.layout}><div className={styles.mapArea}>
 <div className={styles.heatmapControls}>
 <label htmlFor='heatmap-type'>Heatmap<select id='heatmap-type' value={type} onChange={e=>setType(e.target.value as HeatmapType)}>{heatmapOptions.map(option=><option key={option.id} value={option.id} disabled={'disabled' in option&&option.disabled}>{option.label}{'disabled' in option?' — needs classification':''}</option>)}</select></label>
 {(type==='attention'||type==='engagement')&&<label htmlFor='heatmap-platform'>Platform<select id='heatmap-platform' value={source} onChange={e=>setSource(e.target.value as AttentionSource)}><option value='threads'>Threads</option><option value='youtube'>YouTube</option></select></label>}
 {type==='activity'&&<label htmlFor='heatmap-month'>Month<select id='heatmap-month' value={activeMonth} disabled={!months.length} onChange={e=>setMonth(e.target.value)}>{!months.length?<option value=''>No dated items</option>:<><option value='all'>All dates</option>{months.map(value=><option key={value} value={value}>{new Date(value+'-01T00:00:00Z').toLocaleDateString('en-GB',{month:'long',year:'numeric',timeZone:'UTC'})}</option>)}</>}</select></label>}
 {active&&<div className={styles.heatmapSelection} aria-live='polite'><strong>{active.name}</strong><span>{format(values.get(active.id)??null,signed)}{values.get(active.id)!=null?` ${signed?'pp':unit.toLowerCase()}`:''}</span><button onClick={()=>setSelected(null)}>Clear selection</button></div>}
 </div>
 <svg className={styles.map} viewBox='0 0 800 930' role='group' aria-label='Vietnam choropleth map, 34 provinces'><defs><pattern id='missing-heatmap-value' width='6' height='6' patternUnits='userSpaceOnUse'><rect width='6' height='6' fill='#e4e7e1'/><path d='M-1 1 1-1M0 6 6 0M5 7 7 5' stroke='#b7c0b1' strokeWidth='.6'/></pattern></defs>
 {shapes.map(shape=>{const value=values.get(shape.id)??null,text=`${shape.name}: ${format(value,signed)}${value!==null?' '+(signed?'pp':unit.toLowerCase()):''}`;return <path key={shape.id} d={shape.path} fill={fill(value)} fillRule='evenodd' stroke='#171d18' strokeWidth={selected===shape.id||hovered===shape.id?2.5:.85} vectorEffect='non-scaling-stroke' role='button' tabIndex={0} aria-label={text} aria-pressed={selected===shape.id} onMouseEnter={()=>setHovered(shape.id)} onMouseLeave={()=>setHovered(null)} onFocus={()=>setHovered(shape.id)} onBlur={()=>setHovered(null)} onClick={()=>setSelected(shape.id)} onKeyDown={e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();setSelected(shape.id);}}}><title>{text}</title></path>;})}</svg>
 <div className={`${styles.legend} ${styles.bandLegend}`} aria-label='Map color scale'><span className={styles.scaleTitle}>{unit}</span>{signed&&<span className={styles.legendMeaning}>Blue: higher listing share<br/>Red: higher social share</span>}<div className={styles.bandList}>{colorScale.bands.map(band=><span key={band.label}><i style={{background:band.color}}/>{band.label}</span>)}</div>{available.length<values.size&&<span className={styles.missingKey}><i/>No data</span>}</div>
 <a className={styles.credit} href='https://github.com/TaliaCao/Vietnam-Shape-Map' target='_blank' rel='noreferrer'>Map: Talia Cao · MIT</a>
 </div></div>
 <details className={styles.mapMethod}><summary>About this heatmap</summary><h2>{label}</h2><p>{measurement}</p><p>{signed?"Blue marks a higher listing share, red a higher social share. Differences within 0.25 percentage points of zero stay neutral.":"Non-zero values are grouped into distinct colour bands so smaller differences remain visible. The legend shows each band’s actual range; ranges update with the selected measure."}</p><p>{signed?'Positive values mean a higher share of social mentions than venue listings. This is not a court-shortage score. ':''}Locations come from venue addresses or places mentioned in posts and videos. An item can count in several provinces. Comments are not mapped separately. Missing measurements are hatched; zero means none recorded in this sample.</p>{(type==='attention'||type==='engagement')&&<p>Only items with complete reported counts enter the calculation. Threads and YouTube are shown separately because their measurements differ. Engagement belongs to content mentioning a province, not verified local viewers.</p>}{type==='activity'&&<p>This shows the saved sample by publication month, not a measured change in player demand. Older content and places without a match may be missing.</p>}</details>
 </section><DashboardDetails data={data} sources={preferences.sources} selected={selected} onSelect={setSelected}/></>}
 </main>;
}
