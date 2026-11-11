import { readFile } from "node:fs/promises";
import path from "node:path";
import { readS3Snapshot } from "@/features/s3/snapshot";
import { normalize, provinceMatcher, type GeoReference } from "@/features/analytics/geography";
import { normalizeSnapshot } from "@/features/courts/normalize";
import type {SearchRecord} from "./types";
export function youtubeId(url:string|null){try{const u=new URL(url??"");const id=u.hostname==="youtu.be"?u.pathname.slice(1):u.searchParams.get("v")??u.pathname.match(/^\/(?:shorts|embed)\/([^/]+)/)?.[1];return id&&/^[\w-]{11}$/.test(id)?id:null;}catch{return null;}}
export async function searchRecords(params:URLSearchParams){
 if(process.env.TRACKER_DATA_SOURCE!=="s3")throw Error("S3 not configured");
 const snapshot=await readS3Snapshot();
 const root=process.env.DSP_REPO_ROOT??path.resolve(process.cwd(),"../..");
 const refs:GeoReference[]=JSON.parse(await readFile(/* turbopackIgnore: true */ path.join(root,"configs/reference/threads_geography.v1.json"),"utf8")).locations;
 const match=provinceMatcher(refs),names=new Map(refs.map(r=>[r.geo_id,r.canonical_name]));
 const locations=(text:string)=>match(text).map(id=>({id,name:names.get(id)??id}));
 const records:SearchRecord[]=[];
 for(const source of ["threads","youtube","comments"] as const)for(const row of snapshot.social[source==="comments"?"youtube_comments":source])records.push({...row,source,locations:locations(row.text),videoId:source!=="threads"?youtubeId(row.url):null});
 for(const venue of normalizeSnapshot(snapshot,[],snapshot.synced_at,"S3").venues)records.push({id:venue.id,source:"courts",text:venue.name+"\n"+venue.address,url:venue.url,collectedAt:venue.collectedAt??snapshot.collected_at,publishedAt:null,likes:null,views:null,replies:null,locations:locations(venue.address),courts:venue.courts,services:venue.services,openingHours:venue.openingHours});
 const source=params.get("source")??"all",q=normalize((params.get("q")??"").slice(0,300)),location=params.get("location"),from=params.get("from"),to=params.get("to"),parent=params.get("parent"),sort=params.get("sort")??"newest";
 if(!["all","threads","youtube","courts","comments"].includes(source)||!["newest","likes","discussed","views"].includes(sort)||[from,to].some(d=>d&&!/^\d{4}-\d{2}-\d{2}$/.test(d))||from&&to&&from>to)throw new RangeError("Invalid search filters");
 let rows=records.filter(r=>source==="all"||r.source===source);
 const catalog=[...new Map(rows.flatMap(r=>r.locations).map(r=>[r.id,r])).values()].sort((a,b)=>a.name.localeCompare(b.name,"vi"));
 const newest=[...rows].sort((a,b)=>Date.parse(b.collectedAt??"")-Date.parse(a.collectedAt??"")).slice(0,3);
 rows=rows.filter(r=>{
  const date=r.collectedAt?new Date(r.collectedAt).toLocaleDateString("en-CA",{timeZone:"Asia/Ho_Chi_Minh"}):"";
  return (!q||normalize(r.text+" "+r.locations.map(l=>l.name).join(" ")).includes(q))&&(!location||r.locations.some(l=>l.id===location))&&(!from||!!date&&date>=from)&&(!to||!!date&&date<=to)&&(!parent||r.parentRecordId===parent||r.videoId===parent);
 });
 const metric=sort==="likes"?"likes":sort==="discussed"?"replies":sort==="views"?"views":null;
 rows.sort((a,b)=>(metric?(b[metric]??-1)-(a[metric]??-1):0)||(Date.parse(b.collectedAt??"")||0)-(Date.parse(a.collectedAt??"")||0)||a.id.localeCompare(b.id));
 const page=Number(params.get("page")??1);if(!Number.isSafeInteger(page)||page<1||page>100000)throw new RangeError("Invalid page");
 return {records:rows.slice((page-1)*12,page*12),total:rows.length,page,pageSize:12,locations:catalog,newest,syncedAt:snapshot.synced_at,all:rows};
}
export function recordsCsv(rows:SearchRecord[]){
 const columns=["source","id","text","url","publishedAt","collectedAt","likes","views","replies","reposts","quotes","videoDuration","courts","locations"] as const;
 const cell=(v:unknown)=>{let text=String(v??"");if(/^[\s]*[=+@-]/.test(text))text="'"+text;return '"'+text.replace(/"/g,'""')+'"';};
 return '\uFEFF'+[columns.join(","),...rows.map(r=>columns.map(c=>cell(c==="locations"?r.locations.map(l=>l.name).join("; "):r[c])).join(","))].join("\r\n");
}
