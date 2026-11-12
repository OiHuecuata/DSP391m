'use client';
import {useState} from 'react';
import {useAuth} from '@/features/auth/AuthProvider';
import type {ModelReport} from './model-report';
import styles from './model-clusters.module.css';
const colors=['#2876d0','#e5820a','#14886e','#9c58b2','#d45079','#007d94','#94643c','#6a792a','#515cb8','#b54b2e'];
const referenceNames=["Vợt/paddle và grip — bài tiếng Anh","Giải đấu và tổ chức thi đấu","Tìm sân và tìm người chơi","Giá thuê, chi phí và chia tiền sân","Kỹ thuật đánh bóng và luyện tập","Kinh nghiệm chơi, luyện tập và thảo luận chung","Video, truyền thông và quảng bá pickleball (nội dung hỗn hợp)","Vợt, cách cầm vợt và lựa chọn dụng cụ"];
export function ModelClusters({report}:{report:ModelReport|null}){
 const {user}=useAuth(),admin=user?.role==="admin";
 const [hovered,setHovered]=useState<number|null>(null);
 const active=report?.candidates.find(c=>c.k===8);
 if(!report||!active)return <section className={styles.panel} aria-labelledby='model-title'><h2 id='model-title'>Topic cluster</h2><p role='status'>Model results are unavailable.</p></section>;
 const total=active.clusters.reduce((sum,c)=>sum+c.posts,0);
 const slices=active.clusters.map((c,index)=>{
  const start=active.clusters.slice(0,index).reduce((sum,cluster)=>sum+cluster.posts,0)/total*Math.PI*2-Math.PI/2;
  const angle=c.posts/total*Math.PI*2,end=start+angle;
  const point=(a:number)=>`${180+155*Math.cos(a)} ${180+155*Math.sin(a)}`;
  const path=angle>=Math.PI*2?`M180 25A155 155 0 1 1 180 335A155 155 0 1 1 180 25Z`:`M180 180L${point(start)}A155 155 0 ${angle>Math.PI?1:0} 1 ${point(end)}Z`;
  return {...c,name:active.k===8?referenceNames[c.id]:`Cluster ${c.id}`,path,percent:c.posts/total*100};
 });
 return <section className={styles.panel} aria-labelledby='model-title'>
  <header className={styles.heading}><div><h2 id='model-title'>Topic cluster</h2><p>Posts grouped by similar meaning.</p>{active.k===8&&<span className={styles.referenceTag}>Reference topic names</span>}</div><span className={styles.badge}>Candidate model</span></header>
  {admin&&<div className={styles.toolbar}><div className={styles.stats}><strong>{report.posts.toLocaleString()}</strong><span>posts in the recorded run · 8 clusters</span></div></div>}
  <div className={`${styles.content} ${!admin?styles.resultsOnly:""}`}><div className={styles.pieLayout} data-named={active.k===8}>
   <svg className={styles.pie} viewBox='0 0 360 360' role='group' aria-label={`Post distribution across ${active.k} clusters`}>
    {slices.filter(c=>c.posts>0).map(c=><path key={c.id} d={c.path} fill={colors[c.id%colors.length]} stroke='var(--cluster-pie-border,#fffefa)' strokeWidth='3' strokeLinejoin='round' tabIndex={0} role='img' aria-label={`Cluster ${c.id}: ${c.name}, ${c.posts} posts, ${c.percent.toFixed(1)}%`} opacity={hovered===null||hovered===c.id?1:.4} onMouseEnter={()=>setHovered(c.id)} onMouseLeave={()=>setHovered(null)} onFocus={()=>setHovered(c.id)} onBlur={()=>setHovered(null)}><title>{c.name}: {c.posts.toLocaleString()} posts ({c.percent.toFixed(1)}%)</title></path>)}
   </svg>
   <ul className={styles.pieLegend} aria-label='Posts per cluster'>{slices.map(c=><li key={c.id} data-active={hovered===c.id}><span className={styles.clusterName}><i style={{background:colors[c.id%colors.length]}}/>{c.name}</span><span className={styles.clusterCount}>{active.k===8&&<>Cluster {c.id} · </>}<strong>{c.posts.toLocaleString()}</strong> posts <span>({c.percent.toFixed(1)}%)</span></span></li>)}</ul>
  </div>
  {admin&&<aside className={styles.about}><h3>Recorded results</h3><dl>{report.sources.map(s=><div key={s.name}><dt>{s.name==='youtube'?'YouTube videos':s.name==='threads'?'Threads posts':s.name}</dt><dd>{s.posts.toLocaleString()}</dd></div>)}<div><dt>Run date</dt><dd>{new Date(report.recordedAt).toLocaleDateString('en-GB',{day:'numeric',month:'short',year:'numeric',timeZone:'UTC'})}</dd></div></dl><p>MiniLM → UMAP → KMeans</p><p>Historical candidate results, separate from the live S3 totals.</p></aside>}</div>
  {admin&&<details className={styles.details}><summary>Model details</summary><div className={styles.table}><table><thead><tr><th>Clusters</th><th>Silhouette</th><th>Stability (ARI)</th></tr></thead><tbody>{[active].map(c=><tr key={c.k} data-selected={c.k===active.k}><th scope='row'>{c.k}</th><td>{c.silhouette.toFixed(3)}</td><td>{c.stability.toFixed(3)}</td></tr>)}</tbody></table></div><p>Silhouette measures separation; ARI measures stability across resampled runs. Neither is forecast confidence. K=8 uses the supplied reference topic names. That reference covers a different run; the name-to-cluster mapping has not been verified for these recorded results.</p><p>Run: {report.runId}</p></details>}
  {admin&&<p className={styles.note}>Province-level cluster assignments are not included in the shared results yet.</p>}
 </section>;
}
