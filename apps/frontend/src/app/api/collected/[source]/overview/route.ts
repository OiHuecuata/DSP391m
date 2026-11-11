import { readFile } from "node:fs/promises";
import path from "node:path";
import { locationMentions, type MentionReference } from "@/features/collectors/location-mentions";
import { readS3Snapshot, type SocialRecord } from "@/features/s3/snapshot";
export const runtime = "nodejs";
export const dynamic = "force-dynamic";
function metric(rows: SocialRecord[], field: "likes" | "views" | "replies" | "reposts" | "quotes") {
 const values = rows.map(r => r[field]).filter((v): v is number => typeof v === "number" && Number.isFinite(v) && v >= 0);
 return { total: values.length ? values.reduce((a,b)=>a+b,0) : null, reported: values.length, records: rows.length };
}
export async function GET(_request: Request, {params}: {params: Promise<{source: string}>}) {
 const {source} = await params;
 if (source !== "threads" && source !== "youtube") return Response.json({error:"Unknown source"},{status:404});
 try {
  if (process.env.TRACKER_DATA_SOURCE !== "s3") throw Error("Not configured");
  const s = await readS3Snapshot();
  const rows = s.social[source];
  let locations = null;
  try {
   const root=process.env.DSP_REPO_ROOT ?? path.resolve(process.cwd(),"../..");
   const catalog=JSON.parse(await readFile(/* turbopackIgnore: true */ path.join(root,"configs/reference/vietnam_locations.v1.json"),"utf8"));
   if(!Array.isArray(catalog.locations)) throw Error("Invalid location reference");
   const references: MentionReference[]=catalog.locations.filter((r: MentionReference)=>typeof r.location_id === "string" && typeof r.canonical_name === "string" && Array.isArray(r.aliases) && r.aliases.every(a=>typeof a === "string"));
   if(!references.length) throw Error("Empty location reference");
   locations=locationMentions(rows,references);
  } catch { /* Missing reference is unavailable, never zero mentions. */ }

  const durations=rows.map(r=>r.videoDuration?.match(/^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$/)).filter(m=>m && (m[1]||m[2]||m[3])).map(m=>Number(m![1]??0)*3600+Number(m![2]??0)*60+Number(m![3]??0)).sort((a,b)=>a-b);
  const duration=durations.length ? (durations[Math.floor((durations.length-1)/2)]+durations[Math.floor(durations.length/2)])/2 : null;
  const tagCounts=new Map<string,{name:string;count:number}>();
  for(const row of rows) for(const tag of new Set((row.tags??[]).map(t=>t.trim().toLowerCase()).filter(Boolean))) {const item=tagCounts.get(tag);tagCounts.set(tag,{name:tag,count:(item?.count??0)+1});}
  const dates = rows.map(r=>r.collectedAt).filter((d):d is string=>!!d && Number.isFinite(Date.parse(d))).sort((a,b)=>Date.parse(a)-Date.parse(b));
  return Response.json({ source, syncedAt:s.synced_at, posts:rows.length, comments:source === "youtube" ? s.social.youtube_comments.length : null,
   firstCollected:dates[0] ?? null, lastCollected:dates.at(-1) ?? null,
   engagement:s.engagement?.[source] ?? [], reposts:metric(rows,"reposts"), quotes:metric(rows,"quotes"),
   media:{total:rows.some(r=>typeof r.hasMedia === "boolean")?rows.filter(r=>r.hasMedia === true).length:null,reported:rows.filter(r=>typeof r.hasMedia === "boolean").length,records:rows.length},
   duration:{total:duration,reported:durations.length,records:rows.length}, tags:[...tagCounts.values()].sort((a,b)=>b.count-a.count||a.name.localeCompare(b.name)).slice(0,8),
   locations, likes:metric(rows,"likes"), views:metric(rows,"views"), replies:metric(rows,"replies"), history:s.history
  }, {headers:{"Cache-Control":"no-store"}});
 } catch { return Response.json({error:"S3 overview unavailable. Refresh the saved snapshot and try again."},{status:503}); }
}
