"use client";
import Image from 'next/image';
import {ResearchGoals} from '@/features/analytics/ResearchGoals';
import Link from 'next/link';
import {useS3Status,snapshotDate} from '@/features/s3/status';
import styles from './user-workspace.module.css';
const sources=[{id:'courts',name:'PickleballPlus',logo:'pickleballplus.png',title:'Court listings',description:'Browse venues, explore the court map and download listings.',href:'/collectors/pickleballplus',unit:'venues',color:'#238b6c'},{id:'threads',name:'Threads',logo:'threads.svg',title:'Threads conversations',description:'Explore posts, recorded interactions and locations mentioned.',href:'/collectors/threads',unit:'posts',color:'#d87409'},{id:'youtube',name:'YouTube',logo:'youtube.png',title:'YouTube content',description:'Browse videos, linked comments and locations mentioned.',href:'/collectors/youtube',unit:'videos',color:'#2876d0'}] as const;
export function UserDataSources(){const {data}=useS3Status();return <div className={styles.sources}>{sources.map(s=><Link key={s.id} href={s.href} className={styles.source}><div className={styles.sourceTop}><Image src={`/brands/${s.logo}`} alt='' width={26} height={26} unoptimized/><span>{s.name}</span><span aria-hidden='true'>↗</span></div><h3>{s.title}</h3><div className={styles.sourceCount}><strong style={{color:s.color}}>{data?.counts[s.id].toLocaleString()??'—'}</strong><span>{s.unit}{s.id==='youtube'&&data?` · ${data.counts.comments.toLocaleString()} comments`:''}</span></div></Link>)}</div>;}
export function UserHomePage(){const {data,error}=useS3Status();return <main id='system-status' className={styles.page}>
 <header className={styles.heading}><div><h1>Overview</h1></div></header>
 {error&&<p role='alert' className={styles.error}>Data could not be refreshed.{data?' Showing the last available snapshot.':' Please try again shortly.'}</p>}
 {!data&&!error&&<p role='status'>Loading available data…</p>}
 <section className={styles.stats} aria-label='Available market data'><div><span>Court listings</span><strong>{data?.counts.courts.toLocaleString()??'—'}</strong></div><div><span>Provinces represented</span><strong>{data?.coverage?`${data.coverage.provinces} / ${data.coverage.totalProvinces}`:'—'}</strong></div><div><span>Threads posts</span><strong>{data?.counts.threads.toLocaleString()??'—'}</strong></div><div><span>YouTube videos</span><strong>{data?.counts.youtube.toLocaleString()??'—'}</strong></div></section>
 <ResearchGoals/>
 <section className={styles.explore}><div className={styles.courtPhoto}><Image src='/images/pickleball-courts-4k.jpg' alt='Pickleball courts with nets and court markings' fill sizes='(max-width: 750px) 100vw, 45vw' unoptimized/></div><div className={styles.exploreText}><h2>Explore the Vietnam map</h2><div className={styles.actions}><Link href='/dashboard' className={styles.primary}>Explore province map ↗</Link><Link href='/collectors/pickleballplus'>Browse courts →</Link></div></div></section>
 <section aria-labelledby='browse-data-title'><div className={styles.sectionHeading}><h2 id='browse-data-title'>Explore the data</h2><Link href='/search'>Search all records →</Link></div><UserDataSources/></section>
 <footer className={styles.footer}><span>Latest data refresh <strong>{snapshotDate(data?.syncedAt)}</strong></span><Link href='/account'>My account →</Link></footer>
 </main>;}
export function UserExplorePage(){return <main className={styles.page}><header className={styles.heading}><div><h1>Explore data</h1></div><Link href='/search' className={styles.primary}>Search all records ↗</Link></header><UserDataSources/></main>;}
