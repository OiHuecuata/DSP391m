'use client';
import {useEffect,useState} from 'react';
import {useAuth} from '@/features/auth/AuthProvider';
import {useS3Status} from '@/features/s3/status';
import type {MapData} from './types';
type Entry={data:MapData|null;savedAt:number;pending:Promise<MapData>|null};
const entries=new Map<string,Entry>();
const newer=(wanted:string,actual:string)=>wanted!==actual&&Date.parse(wanted)>Date.parse(actual);
async function load(owner:string,revision?:string,retry=false):Promise<MapData>{
 let entry=entries.get(owner);if(!entry){entry={data:null,savedAt:0,pending:null};entries.set(owner,entry);}
 if(entry.data&&(revision?!newer(revision,entry.data.syncedAt):Date.now()-entry.savedAt<30000))return entry.data;
 if(!entry.pending){const current=entry;current.pending=fetch('/api/analytics/map',{cache:'no-store'}).then(async response=>{if(!response.ok)throw Error('Map data unavailable. Please try again.');return response.json() as Promise<MapData>;}).then(data=>{current.data=data;current.savedAt=Date.now();return data;}).finally(()=>{current.pending=null;});}
 const result=await entry.pending!;
 return revision&&newer(revision,result.syncedAt)&&!retry?load(owner,revision,true):result;
}
export function useMapData(){
 const {user}=useAuth(),{data:snapshot}=useS3Status(),owner=user?.id??'',revision=snapshot?.syncedAt;
 const [state,setState]=useState<{owner:string;data:MapData|null;error:string}>({owner:'',data:null,error:''});
 useEffect(()=>{if(!owner)return;let active=true;void load(owner,revision).then(data=>{if(active)setState(previous=>previous.owner===owner&&previous.data===data&&!previous.error?previous:{owner,data,error:''});}).catch(error=>{if(active)setState(previous=>({owner,data:previous.owner===owner?previous.data:null,error:error.message}));});return()=>{active=false;};},[owner,revision]);
 return {data:owner?(state.owner===owner?state.data:entries.get(owner)?.data??null):null,error:state.owner===owner?state.error:''};
}
