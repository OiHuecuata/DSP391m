#!/usr/bin/env node
// Run in your own Terminal; Ctrl+C stops all three demo services.
import {spawn} from 'node:child_process';
import {websiteAwsSettings} from './aws_profile.mjs';
import {mkdir,readFile,writeFile,appendFile,access} from 'node:fs/promises';
import {existsSync} from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../..');
const frontend=path.join(root,'apps/frontend'),directory=path.join(root,'.local/cloudflare-demo');
const cloudflared=process.env.TRACKER_CLOUDFLARED??path.join(os.homedir(),'Documents/Codex/tools/bin/cloudflared');
const next=path.join(frontend,'node_modules/next/dist/bin/next');
const awsSettings=websiteAwsSettings(),websiteEnv={...process.env,TRACKER_AWS_PROFILE:awsSettings.profile,AWS_REGION:awsSettings.region};
const children=new Set();let closing=false;
function stop(code=0){if(closing)return;closing=true;for(const child of children)try{process.kill(-child.pid,'SIGTERM');}catch{}setTimeout(()=>{for(const child of children)try{process.kill(-child.pid,'SIGKILL');}catch{}process.exit(code);},1500);}
process.on('SIGINT',()=>stop());process.on('SIGTERM',()=>stop());
function launch(command,args,options={},service=false){const child=spawn(command,args,{cwd:root,env:process.env,detached:true,stdio:'inherit',...options});children.add(child);child.on('error',error=>{console.error(error.message);stop(1);});child.on('exit',(code,signal)=>{children.delete(child);if(service&&!closing){console.error(`Demo service stopped (${code??signal}). Run the launcher again.`);stop(1);}});return child;}
async function run(command,args,options={}){const child=launch(command,args,options);await new Promise((resolve,reject)=>{child.once('exit',code=>code===0?resolve():reject(Error(`${command} exited with status ${code}`)));child.once('error',reject);});}
async function portBusy(port){return new Promise(resolve=>{const socket=net.connect({port,host:'127.0.0.1'});socket.once('connect',()=>{socket.destroy();resolve(true);});socket.once('error',()=>resolve(false));});}
async function waitFor(url,timeout=90000){const deadline=Date.now()+timeout;while(Date.now()<deadline&&!closing){try{const result=await fetch(url,{signal:AbortSignal.timeout(5000)});if(result.status===200)return;}catch{}await new Promise(resolve=>setTimeout(resolve,600));}throw Error(`Timed out waiting for ${url}`);}
try{
 await mkdir(directory,{recursive:true,mode:0o700});await access(cloudflared);await access(next);
 if(await portBusy(3000)||await portBusy(8080))throw Error('Ports 3000 or 8080 are already in use. Stop the existing local servers first, then run this launcher again.');
 const backend=path.join(directory,'backend');
 const buildEnv={...websiteEnv,TRACKER_NEXT_DIST_DIR:'.next-demo',BACKEND_URL:'http://127.0.0.1:8080',NEXT_TELEMETRY_DISABLED:'1',TRACKER_S3_AUTO_REFRESH:'0'};
 if(!process.argv.includes('--reuse-build')){
  console.log('Building the website for the public demo…');
  const files=['tsconfig.json','next-env.d.ts'];const previous=await Promise.all(files.map(async file=>[file,await readFile(path.join(frontend,file))]));
  try{await run(process.execPath,[next,'build'],{cwd:frontend,env:buildEnv});}finally{for(const [file,content]of previous)await writeFile(path.join(frontend,file),content);}
  await run('go',['build','-o',backend,'./cmd/server'],{cwd:path.join(root,'apps/backend'),env:{...process.env,GOCACHE:process.env.GOCACHE??path.join(directory,'go-cache')}});
 }
 if(!existsSync(path.join(frontend,'.next-demo/BUILD_ID'))||!existsSync(backend))throw Error('Demo build missing. Run without --reuse-build.');
 const log=path.join(directory,'cloudflared.log');await writeFile(log,'',{mode:0o600});
 console.log('Creating a temporary Cloudflare HTTPS address…');
 const tunnel=launch(cloudflared,['tunnel','--url','http://127.0.0.1:3000','--no-autoupdate','--protocol','http2','--edge-ip-version','4'],{stdio:['ignore','pipe','pipe']},true);
 const origin=await new Promise((resolve,reject)=>{
  let buffer='';const timer=setTimeout(()=>reject(Error('Cloudflare did not provide an address. Check '+log)),90000);
  const output=chunk=>{const text=String(chunk);void appendFile(log,text);buffer=(buffer+text).slice(-20000);const match=buffer.match(/https:\/\/[a-z0-9-]+\.trycloudflare\.com\b/);if(match){clearTimeout(timer);resolve(match[0]);}};
  tunnel.stdout.on('data',output);tunnel.stderr.on('data',output);tunnel.once('exit',()=>{clearTimeout(timer);reject(Error('Cloudflare tunnel stopped. Check '+log));});
 });
 const env={...websiteEnv,TRACKER_PUBLIC_ORIGIN:origin,AUTH_COOKIE_SECURE:'1'};
 launch(backend,[],{env:{...env,SERVER_PORT:'8080',AUTH_SERVICE_URL:'http://127.0.0.1:3000/api/auth/me',GIN_MODE:'release'}},true);
 launch(process.execPath,[next,'start','--hostname','127.0.0.1','--port','3000'],{cwd:frontend,env:{...env,TRACKER_NEXT_DIST_DIR:'.next-demo',BACKEND_URL:'http://127.0.0.1:8080',NEXT_TELEMETRY_DISABLED:'1'}},true);
 await waitFor('http://127.0.0.1:8080/health');await waitFor('http://127.0.0.1:3000/login');await waitFor(origin+'/login');
 await writeFile(path.join(directory,'url.txt'),origin+'\n',{mode:0o600});
 console.log(`\nPickleballDB is ready: ${origin}\nLocal access: http://localhost:3000\nKeep this Terminal open. Press Ctrl+C to stop the demo.\nThe public address changes each time you start the tunnel.\n`);
}catch(error){console.error('Demo setup failed: '+error.message);stop(1);}
