"use client";
import {ResearchGoals} from "@/features/analytics/ResearchGoals";
import {UserHomePage} from "@/features/user/UserHomePage";
import { S3Totals, S3Overview } from "./S3Overview";
import {useAuth} from "@/features/auth/AuthProvider";
import Image from "next/image";
import Link from "next/link";
import { useS3Status, snapshotDate } from "@/features/s3/status";
import { S3CollectionChart } from "./S3CollectionChart";
import { platforms } from "./CollectionChart";
import styles from "./dashboard.module.css";
export function HomePage() {
 const {user}=useAuth();
 const { data, error } = useS3Status();
 if(!user)return <main id="system-status" className={styles.dashboard}><p role="status">Loading your workspace…</p></main>;
 if(user.role==='user')return <UserHomePage/>;
 const counts = data?.counts;
 const rows = platforms.map(p => ({ ...p, count: counts ? p.id === "pbplus" ? counts.courts : p.id === "threads" ? counts.threads : counts.youtube + counts.comments : null }));
 return <main id="system-status" className={styles.dashboard}>
 <div className={styles.pageHeading}><div><h1>Pickleball at a glance</h1></div><div className={styles.toolbar}><Link href="/dashboard">Open dashboard ↗</Link>{user?.role==="admin"&&<Link href="/data-operations">Data Operations ↗</Link>}</div></div>
 {error && <div role="alert" className={styles.alert}>{error}{data && ". Showing the previous snapshot."}</div>}
 {!data && !error && <p role="status">Loading S3 data…</p>}
 <section className={styles.homeStatus} aria-label="Snapshot status"><span><i className={data&&!error?styles.loadedDot:styles.waitingDot}/>{error?'Snapshot needs attention':data?'Saved S3 snapshot':'Loading snapshot'}</span><span>Last synced <strong>{snapshotDate(data?.syncedAt)}</strong></span><span>{data?.coverage?`${data.coverage.provinces} of ${data.coverage.totalProvinces} provinces represented`:'34-province system'}</span></section>
 <S3Totals />
 <ResearchGoals/>
 <div className={styles.overviewGrid}>
 <S3CollectionChart history={data?.history} loaded={!!data} />
 <section className={styles.panel}><div className={styles.panelHeading}><div><h2>Data sources</h2></div></div><div className={styles.collectorList}>{rows.map(p => <Link key={p.id} href={user?.role==='admin'?`/data-operations?source=${p.id}`:p.href} className={styles.collectorRow}><div className={styles.platformIcon}><Image src={`/brands/${p.logo}`} alt="" width={24} height={24} unoptimized /></div><div className={styles.sourceInfo}><h3>{p.name}</h3><span>{p.id==='youtube'&&counts?`${counts.youtube.toLocaleString()} videos · ${counts.comments.toLocaleString()} comments`:p.description}</span></div><div className={styles.sourceMetric}><strong>{p.count?.toLocaleString() ?? "—"}</strong><span>{p.count == null ? "Unavailable" : "saved records"}</span></div><span aria-hidden="true">↗</span></Link>)}</div></section>
 </div>
 <S3Overview />

 </main>;
}
