import {buildHeatmapData} from "@/features/analytics/build-heatmap";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { readS3Snapshot } from "@/features/s3/snapshot";
import { provinceMatcher, type GeoReference } from "@/features/analytics/geography";
export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export async function GET() {
  try {
    if(process.env.TRACKER_DATA_SOURCE !== "s3") throw Error("Snapshot not configured");
    const root = process.env.DSP_REPO_ROOT ?? path.resolve(process.cwd(),"../..");
    const [snapshot,catalog] = await Promise.all([readS3Snapshot(),readFile(/* turbopackIgnore: true */ path.join(root,"configs/reference/threads_geography.v1.json"),"utf8")]);
    const references: GeoReference[] = JSON.parse(catalog).locations;
    const match = provinceMatcher(references);
    const provinces = references.filter(r=>r.admin_version === "2025_07" && r.level === "province").map(r=>({id:r.geo_id,name:r.canonical_name,threads:0,youtube:0,pickleballplus:0}));
    if(provinces.length !== 34) throw Error("Invalid province reference");
    const lookup = new Map(provinces.map(r=>[r.id,r]));
    const sources = {
      threads:snapshot.social.threads.map(r=>({id:r.id,text:r.text})),
      youtube:snapshot.social.youtube.map(r=>({id:r.id,text:r.text})),
      pickleballplus:snapshot.posts.filter((r):r is Record<string,unknown>=>!!r && typeof r === "object").map(r=>({id:String(r.id),text:typeof r.address === "string" ? r.address : ""})),
    };
    const totals = {threads:0,youtube:0,pickleballplus:0};
    const matched = {...totals};
    for(const source of Object.keys(sources) as (keyof typeof sources)[]) {
      const seen = new Set<string>();
      for(const row of sources[source]) {
        if(seen.has(row.id)) continue;
        seen.add(row.id);totals[source]++;
        const ids = match(row.text);
        // An ambiguous venue address is left unmapped; social content may mention several provinces.
        if(!ids.length || source === "pickleballplus" && ids.length !== 1) continue;
        matched[source]++;
        for(const id of ids) { const province=lookup.get(id);if(province) province[source]++; }
      }
    }
    const courtSizes=[{label:"1–2 courts",count:0},{label:"3–5 courts",count:0},{label:"6–10 courts",count:0},{label:"11+ courts",count:0},{label:"Zero reported",count:0},{label:"Not reported",count:0}];
    const courtIds=new Set<string>();
    for(const item of snapshot.posts){if(!item||typeof item!=="object")continue;const row=item as Record<string,unknown>;if(courtIds.has(String(row.id)))continue;courtIds.add(String(row.id));const n=row.number_of_courts;const index=typeof n!=="number"||!Number.isFinite(n)||n<0?5:n===0?4:n<=2?0:n<=5?1:n<=10?2:3;courtSizes[index].count++;}
    const interactions = (['threads','youtube'] as const).map(source=>({source,metrics:(['likes','replies','reposts','quotes'] as const).map(key=>{const values=snapshot.social[source].map(row=>row[key]).filter((n):n is number=>typeof n==='number'&&Number.isFinite(n)&&n>=0);return {key,total:values.reduce((sum,n)=>sum+n,0),reported:values.length};})}));
    return Response.json({heatmap:buildHeatmapData(snapshot,references),courtSizes,provinces,totals,matched,history:snapshot.history??null,interactions,syncedAt:snapshot.synced_at},{headers:{"Cache-Control":"no-store"}});
  } catch { return Response.json({error:"Map data unavailable. Refresh the S3 snapshot and try again."},{status:503}); }
}
