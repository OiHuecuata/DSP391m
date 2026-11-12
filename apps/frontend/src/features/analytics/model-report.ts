export type ClusterCandidate={k:number;silhouette:number;stability:number;clusters:{id:number;posts:number}[]};
export type ModelReport={runId:string;recordedAt:string;posts:number;sources:{name:string;posts:number}[];primary:number;candidates:ClusterCandidate[]};
function record(value:unknown):Record<string,unknown>{if(!value||typeof value!=='object'||Array.isArray(value))throw Error('Invalid model report');return value as Record<string,unknown>;}
function number(value:unknown,min=0,max=Infinity){if(typeof value!=='number'||!Number.isFinite(value)||value<min||value>max)throw Error('Invalid model metric');return value;}
function count(value:unknown){const n=number(value);if(!Number.isSafeInteger(n))throw Error('Invalid model count');return n;}
// Only return aggregate evidence. Training paths, raw text and artifacts stay on the server.
export function parseModelReport(value:unknown):ModelReport{
 const report=record(value),training=record(report.training),dataset=record(training.dataset),config=record(training.config);
 if(training.status!=='candidate'||typeof training.run_id!=='string'||typeof report.recorded_at!=='string'||!Number.isFinite(Date.parse(report.recorded_at)))throw Error('Unsupported model report');
 const posts=count(dataset.rows),sourceCounts=record(dataset.sources);
 const sources=Object.entries(sourceCounts).map(([name,n])=>({name,posts:count(n)}));
 if(sources.reduce((n,s)=>n+s.posts,0)!==posts)throw Error('Model source totals do not match');
 const metrics=record(report.candidate_metrics);
 const candidates=Object.entries(metrics).map(([key,value])=>{
  const k=count(Number(key)),m=record(value),sizes=record(m.cluster_sizes);
  if(k<2||count(m.rows)!==posts)throw Error('Model populations do not match');
  const clusters=Object.entries(sizes).map(([id,n])=>({id:count(Number(id)),posts:count(n)})).sort((a,b)=>a.id-b.id);
  if(clusters.length!==k||clusters.some((c,i)=>c.id!==i)||clusters.reduce((n,c)=>n+c.posts,0)!==posts)throw Error('Invalid cluster assignment totals');
  return {k,silhouette:number(m.cosine_silhouette_original_embeddings,-1,1),stability:number(record(m.stability).mean_ari,-1,1),clusters};
 }).sort((a,b)=>a.k-b.k);
 const primary=count(config.primary_candidate);if(!candidates.some(c=>c.k===primary))throw Error('Missing primary candidate');
 return {runId:training.run_id,recordedAt:report.recorded_at,posts,sources,primary,candidates};
}
