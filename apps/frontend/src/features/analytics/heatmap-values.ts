import type {MapData} from './types';
import {compareProvinces} from './market-comparison';
export const heatmapOptions=[{id:'balance',label:'Interest versus supply'},{id:'attention',label:'Social attention'},{id:'engagement',label:'Average engagement'},{id:'supply',label:'Playing-court supply'},{id:'activity',label:'Activity over time'},{id:'topics',label:'Discussion topics',disabled:true}] as const;
export type HeatmapType=typeof heatmapOptions[number]['id'];
export type AttentionSource='threads'|'youtube';
export function heatmapValues(data:MapData,type:HeatmapType,source:AttentionSource,month:string){
 const metrics=new Map(data.heatmap?.provinces.map(row=>[row.id,row]));
 const comparisons=new Map(compareProvinces(data,['threads','youtube']).map(row=>[row.id,row]));
 return new Map(data.provinces.map(province=>{
  const row=metrics.get(province.id);let value:number|null=null;
  if(type==='balance')value=comparisons.get(province.id)?.difference??null;
  if(row){
   if(type==='supply')value=row.listingsWithCourtCount>0?row.playingCourts:province.pickleballplus===0?0:null;
   if(type==='attention')value=source==='threads'?row.threadsReported>0?row.threadsInteractions:province.threads===0?0:null:row.youtubeViewsReported>0?row.youtubeViews:province.youtube===0?0:null;
   if(type==='engagement')value=source==='threads'?row.threadsReported>0?row.threadsInteractions/row.threadsReported:null:row.youtubeReported>0?row.youtubeInteractions/row.youtubeReported:null;
   if(type==='activity')value=month==='all'?Object.values(row.activity).reduce((sum,day)=>sum+day.threads+day.youtube,0):month?(row.activity[month]?.threads??0)+(row.activity[month]?.youtube??0):null;
  }
  return [province.id,value] as const;
 }));
}
export function heatmapUnit(type:HeatmapType,source:AttentionSource){return type==='balance'?'Share gap (pp)':type==='supply'?'Playing courts':type==='activity'?'Posts & videos':type==='engagement'?'Interactions / item':source==='youtube'?'Video views':'Interactions';}
