import {execFile} from "node:child_process";
import {promisify} from "node:util";
import {existsSync} from "node:fs";
import path from "node:path";
import os from "node:os";
import {localMutation} from "@/features/settings/server";
import {requestS3Refresh} from "@/features/s3/auto-refresh";
const run=promisify(execFile);
export const runtime="nodejs";
export async function POST(request:Request){if(!localMutation(request))return Response.json({error:"Requests must come from the configured website"},{status:403});if(process.env.TRACKER_DATA_SOURCE!=="s3")return Response.json({error:"S3 is not configured"},{status:400});let action;try{action=(await request.json()).action;}catch{return Response.json({error:"Invalid action"},{status:400});}
 if(action==="sync")return Response.json(requestS3Refresh(),{status:202});
 if(action!=="test")return Response.json({error:"Unknown action"},{status:400});
 const bundled=path.join(os.homedir(),"Documents/Codex/tools/bin/aws"),aws=process.env.TRACKER_AWS_CLI??(existsSync(bundled)?bundled:"aws"),profile=process.env.TRACKER_AWS_PROFILE??"tracker",region=process.env.AWS_REGION??"ap-southeast-2";
 try{const source=process.env.TRACKER_S3_BASE;const bucket=source&&source!=="auto"?new URL(source).hostname:"dsp391m-data-lake";await run(aws,["s3api","list-objects-v2","--bucket",bucket,"--max-keys","1","--query","KeyCount","--profile",profile,"--region",region],{timeout:45000,maxBuffer:65536,env:{...process.env,AWS_PAGER:""}});return Response.json({success:true,message:"AWS login and S3 read access are working."});}catch(error){const stderr=(error as {stderr?:string}).stderr??"";return Response.json({success:false,message:/expired|login|credential/i.test(stderr)?"AWS login is required. Sign in through your local Terminal, then test again.":"S3 connection failed. Check your network and read permissions."});}}
