import {readFile,mkdir,writeFile,rename} from "node:fs/promises";
import path from "node:path";
export type SyncSettings={enabled:boolean;intervalMinutes:5|10|30};
export const repoRoot=()=>process.env.DSP_REPO_ROOT??path.resolve(process.cwd(),"../..");
export const syncDefaults:SyncSettings={enabled:true,intervalMinutes:5};
export async function readSyncSettings():Promise<SyncSettings>{try{const v=JSON.parse(await readFile(/* turbopackIgnore: true */ path.join(repoRoot(),".local/website-data/settings.json"),"utf8"));return {enabled:typeof v.enabled==="boolean"?v.enabled:true,intervalMinutes:[5,10,30].includes(v.intervalMinutes)?v.intervalMinutes:5};}catch{return syncDefaults;}}
export async function writeSyncSettings(v:SyncSettings){const directory=path.join(repoRoot(),".local/website-data");await mkdir(directory,{recursive:true});const temporary=path.join(directory,`settings-${process.pid}-${Date.now()}.tmp`);await writeFile(temporary,JSON.stringify(v));await rename(temporary,path.join(directory,"settings.json"));}
// Only loopback requests or the explicitly configured HTTPS demo origin may mutate data.
export function localMutation(request:Request){
 try{
  const url=new URL(request.url),host=request.headers.get('host')??url.host;
  const target=new URL(`${url.protocol}//${host}`),origin=request.headers.get('origin');
  if(['cross-site','same-site'].includes(request.headers.get('sec-fetch-site')??''))return false;
  if(['localhost','127.0.0.1','[::1]'].includes(target.hostname))return !origin||new URL(origin).origin===target.origin;
  const configured=process.env.TRACKER_PUBLIC_ORIGIN;if(!configured||!origin)return false;
  const allowed=new URL(configured);
  if(allowed.protocol!=='https:'||allowed.username||allowed.password||allowed.pathname!=='/'||allowed.search||allowed.hash)return false;
  return target.host===allowed.host&&new URL(origin).origin===allowed.origin;
 }catch{return false;}
}
