import { readFile } from "node:fs/promises";
import path from "node:path";
import { provinceMatcher, type GeoReference } from "@/features/analytics/geography";
import type { WebsiteSnapshot } from "./snapshot";
const latest=(values:(string|null|undefined)[])=>values.filter((v):v is string=>!!v&&Number.isFinite(Date.parse(v))).sort((a,b)=>Date.parse(b)-Date.parse(a))[0]??null;
export async function snapshotOverview(snapshot:WebsiteSnapshot) {
  const freshness={courts:snapshot.collected_at,threads:latest(snapshot.social.threads.map(r=>r.collectedAt)),youtube:latest(snapshot.social.youtube.map(r=>r.collectedAt)),comments:latest(snapshot.social.youtube_comments.map(r=>r.collectedAt))};
  let coverage=null;
  try {
    const root=process.env.DSP_REPO_ROOT??path.resolve(process.cwd(),"../..");
    const reference=JSON.parse(await readFile(/* turbopackIgnore: true */ path.join(root,"configs/reference/threads_geography.v1.json"),"utf8"));
    const references:GeoReference[]=reference.locations;
    const match=provinceMatcher(references);
    const provinces=new Set<string>();
    const sources=[{id:"courts",rows:snapshot.posts.map(r=>({text:typeof (r as {address?:unknown})?.address==="string"?(r as {address:string}).address:""}))},{id:"threads",rows:snapshot.social.threads},{id:"youtube",rows:snapshot.social.youtube}].map(({id,rows})=>{
      let matched=0;
      for(const row of rows){const ids=match(row.text);if(!ids.length||id==="courts"&&ids.length!==1)continue;matched++;ids.forEach(id=>provinces.add(id));}
      return {id,total:rows.length,matched,unmatched:rows.length-matched};
    });
    coverage={provinces:provinces.size,totalProvinces:34,sources};
  }catch{/* Coverage is unavailable rather than a false zero. */}
  return {freshness,coverage,changes:snapshot.changes??null};
}
