import { execFile } from "node:child_process";
import path from "node:path";
import { mkdir,writeFile,rename } from "node:fs/promises";
import {readSyncSettings,repoRoot} from "@/features/settings/server";
type RefreshState={timer?:ReturnType<typeof setInterval>;busy:boolean;lastAttempt:number;lastResult?:{success:boolean;message:string}};
const globalState=globalThis as typeof globalThis&{trackerS3Refresh?:RefreshState};
const state=()=>globalState.trackerS3Refresh??(globalState.trackerS3Refresh={busy:false,lastAttempt:0});
export function refreshState(){const s=state();return {busy:s.busy,lastAttempt:s.lastAttempt,lastResult:s.lastResult};}
export function requestS3Refresh(){
 const s=state();if(s.busy)return {started:false,message:"An S3 sync is already running."};s.busy=true;s.lastAttempt=Date.now();
 const root=repoRoot(),directory=path.join(root,".local/website-data");
 execFile("bash",[path.join(root,"scripts/website/refresh_s3.sh")],{cwd:root,env:{...process.env,AWS_PAGER:""},timeout:960000,maxBuffer:1024*1024},async(error,stdout)=>{
  const needsLogin=stdout.includes("AWS sign-in is required"),message=error?needsLogin?"AWS sign-in expired. Saved data remains available.":"S3 refresh failed. Saved data remains available.":"S3 sync completed.";
  s.lastResult={success:!error,message};
  try{await mkdir(directory,{recursive:true});const temporary=path.join(directory,`sync-status-${process.pid}.tmp`);await writeFile(temporary,JSON.stringify({checkedAt:new Date().toISOString(),state:error?needsLogin?"login_required":"failed":"ready",message}));await rename(temporary,path.join(directory,"sync-status.json"));}catch{console.warn("Unable to save S3 sync status.");}finally{s.busy=false;}
 });
 return {started:true,message:"Sync started. Saved data stays available while S3 is checked."};
}
export function startS3Refresh(){const s=state();if(s.timer)return;async function tick(){const settings=await readSyncSettings();if(process.env.TRACKER_S3_AUTO_REFRESH!=="0"&&settings.enabled&&!s.busy&&Date.now()-s.lastAttempt>=settings.intervalMinutes*60000)requestS3Refresh();}void tick();s.timer=setInterval(()=>void tick(),10000);s.timer.unref();}
