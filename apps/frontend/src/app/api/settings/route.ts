import {localMutation,readSyncSettings,writeSyncSettings} from "@/features/settings/server";
import {refreshState} from "@/features/s3/auto-refresh";
export const runtime="nodejs";
export const dynamic="force-dynamic";
export async function GET(){return Response.json({sync:await readSyncSettings(),runtime:refreshState(),automaticAllowed:process.env.TRACKER_S3_AUTO_REFRESH!=="0",connection:{profile:process.env.TRACKER_AWS_PROFILE??"tracker",region:process.env.AWS_REGION??"ap-southeast-2",source:process.env.TRACKER_S3_BASE??"Newest timestamped export in s3://dsp391m-data-lake/bootstrap/",mode:process.env.TRACKER_DATA_SOURCE??"local"}},{headers:{"Cache-Control":"no-store"}});}
export async function PATCH(request:Request){if(!localMutation(request))return Response.json({error:"Requests must come from the configured website"},{status:403});try{const v=await request.json();if(typeof v.enabled!=="boolean"||![5,10,30].includes(v.intervalMinutes))throw Error();await writeSyncSettings({enabled:v.enabled,intervalMinutes:v.intervalMinutes});return Response.json({saved:true});}catch{return Response.json({error:"Invalid update settings"},{status:400});}}
