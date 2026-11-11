import type {WebsiteSnapshot} from '@/features/s3/snapshot';
import type {HeatmapData, ProvinceHeatMetrics} from './types';
import {provinceMatcher, type GeoReference} from './geography';
const valid=(value:unknown):value is number=>typeof value==='number'&&Number.isFinite(value)&&value>=0;
const monthFormat=new Intl.DateTimeFormat('en-CA',{timeZone:'Asia/Ho_Chi_Minh',year:'numeric',month:'2-digit'});
export function buildHeatmapData(snapshot:WebsiteSnapshot,references:GeoReference[]):HeatmapData {
 const match=provinceMatcher(references);
 const provinces:ProvinceHeatMetrics[]=references.filter(r=>r.admin_version==='2025_07'&&r.level==='province').map(r=>({id:r.geo_id,playingCourts:0,listingsWithCourtCount:0,threadsInteractions:0,threadsReported:0,youtubeViews:0,youtubeViewsReported:0,youtubeInteractions:0,youtubeReported:0,activity:{}}));
 const lookup=new Map(provinces.map(row=>[row.id,row])),months=new Set<string>(),courtIds=new Set<string>();
 for(const item of snapshot.posts){
  if(!item||typeof item!=='object')continue;
  const row=item as Record<string,unknown>;
  if(row.id==null||courtIds.has(String(row.id)))continue;
  courtIds.add(String(row.id));
  const ids=match(typeof row.address==='string'?row.address:'');
  if(ids.length!==1||!valid(row.number_of_courts)||!Number.isInteger(row.number_of_courts))continue;
  const province=lookup.get(ids[0]);if(!province)continue;
  province.playingCourts+=row.number_of_courts;province.listingsWithCourtCount++;
 }
 for(const source of ['threads','youtube'] as const){
  const seen=new Set<string>();
  for(const row of snapshot.social[source]){
   if(seen.has(row.id))continue;seen.add(row.id);
   const ids=match(row.text);if(!ids.length)continue;
   const interactionValues=source==='threads'?[row.likes,row.replies,row.reposts,row.quotes]:[row.likes,row.replies];
   const complete=interactionValues.every(valid),interactions=complete?interactionValues.reduce((sum,value)=>sum+(value??0),0):0;
   let month:string|null=null;
   if(row.publishedAt&&Number.isFinite(Date.parse(row.publishedAt))){const parts=monthFormat.formatToParts(new Date(row.publishedAt));month=parts.find(p=>p.type==='year')!.value+'-'+parts.find(p=>p.type==='month')!.value;months.add(month);}
   for(const id of ids){const province=lookup.get(id);if(!province)continue;
    if(source==='threads'&&complete){province.threadsInteractions+=interactions;province.threadsReported++;}
    if(source==='youtube'){
     if(valid(row.views)){province.youtubeViews+=row.views;province.youtubeViewsReported++;}
     if(complete){province.youtubeInteractions+=interactions;province.youtubeReported++;}
    }
    if(month){const activity=province.activity[month]??(province.activity[month]={threads:0,youtube:0});activity[source]++;}
   }
  }
 }
 return {provinces,months:[...months].sort()};
}
