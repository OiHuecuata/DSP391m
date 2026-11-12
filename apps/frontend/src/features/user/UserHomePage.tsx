'use client';
import Link from 'next/link';
import {useMapData} from '@/features/analytics/useMapData';
import {heatmapValues} from '@/features/analytics/heatmap-values';
import {heatmapScale} from '@/features/analytics/heatmap-scale';
import shapes from '@/features/analytics/provinces.json';
import styles from './user-workspace.module.css';

export function UserHomePage(){
 const {data,error}=useMapData();
 const values=data?heatmapValues(data,'balance','threads','all'):new Map<string,number|null>();
 const scale=heatmapScale([...values.values()],true,'spectrum',false);
 return <main id='system-status' className={styles.page}>
  <header className={styles.heading}><h1>See where courts and interest meet.</h1><p>Explore Vietnam’s pickleball landscape through maps and analysis.</p></header>
  <section className={styles.mapFeature} aria-labelledby='home-map-title'>
   <div className={styles.mapCopy}><h2 id='home-map-title'>A view across Vietnam</h2><p>Compare court supply with social interest, province by province.</p><Link className={styles.primary} href='/dashboard'>Explore the map <span aria-hidden='true'>↗</span></Link><div className={styles.mapKey}><span><i style={{background:'#3182bd'}}/>Higher court-supply share</span><span><i style={{background:'#f46d43'}}/>Higher social-interest share</span></div>{error&&<p role='status'>The map preview is unavailable. Open Dashboard to try again.</p>}</div>
   <Link href='/dashboard' className={styles.mapLink} aria-label='Open the Vietnam results map'><svg viewBox='0 0 800 930' role='img' aria-label='Vietnam map comparing court supply and social-interest shares'>{shapes.map(shape=>{const value=values.get(shape.id);return <path key={shape.id} d={shape.path} fill={value==null?'#e0e5dc':scale.fill(value)} stroke='#171d18' strokeWidth='.8' vectorEffect='non-scaling-stroke' fillRule='evenodd'><title>{shape.name}</title></path>;})}</svg>{!data&&!error&&<span role='status' className={styles.loading}>Loading map…</span>}</Link>
  </section>
  <div className={styles.results}>
   <Link href='/analytics#comparison-title' className={styles.result}><span className={styles.resultIcon} aria-hidden='true'><svg viewBox='0 0 40 40' fill='none' stroke='currentColor' strokeWidth='2'><path d='M7 32V10M7 32h28M12 27l8-9 6 4 8-13'/><circle cx='20' cy='18' r='3' fill='currentColor'/><circle cx='26' cy='22' r='3' fill='currentColor'/></svg></span><h2>Supply & interest</h2><p>See how court supply compares with social interest across provinces.</p><strong>Compare provinces <span aria-hidden='true'>↗</span></strong></Link>
   <Link href='/analytics#model-title' className={styles.result}><span className={styles.resultIcon} aria-hidden='true'><svg viewBox='0 0 40 40' fill='none' stroke='currentColor' strokeWidth='2'><circle cx='14' cy='14' r='7'/><circle cx='28' cy='14' r='4'/><circle cx='23' cy='28' r='8'/><circle cx='9' cy='29' r='3'/></svg></span><h2>Conversation clusters</h2><p>Discover how the model groups similar pickleball conversations.</p><strong>View cluster results <span aria-hidden='true'>↗</span></strong></Link>
  </div>
 </main>;
}
