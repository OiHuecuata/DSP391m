// Run locally to bootstrap the first administrator. Never logs the password.
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {randomBytes,randomUUID,scrypt} from 'node:crypto';
import {mkdir,mkdtemp,writeFile,readFile,rm,chmod} from 'node:fs/promises';
import {existsSync} from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import {fileURLToPath} from 'node:url';
const exec=promisify(execFile),root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../..');
const cli=process.env.TRACKER_AWS_CLI??path.join(os.homedir(),'Documents/Codex/tools/bin/aws');
const location=new URL(process.env.TRACKER_AUTH_S3_URI??'s3://dsp391m-data-lake/website-auth/v1');
const key=location.pathname.slice(1).replace(/\/$/,'')+'/accounts.json';
const directory=await mkdtemp(path.join(os.tmpdir(),'pickleball-bootstrap-'));
async function aws(args){return exec(cli,[...args,'--profile',process.env.TRACKER_AWS_PROFILE??'tracker','--region',process.env.AWS_REGION??'ap-southeast-2','--output','json'],{timeout:30000,maxBuffer:1048576,env:{...process.env,AWS_PAGER:''}});}
let credentialCreated=null,uploaded=false;
try{
 const block=JSON.parse((await aws(['s3api','get-public-access-block','--bucket',location.hostname])).stdout).PublicAccessBlockConfiguration;
 if(!block||!['BlockPublicAcls','IgnorePublicAcls','BlockPublicPolicy','RestrictPublicBuckets'].every(k=>block[k]===true))throw Error('Public access must be blocked before storing accounts.');
 try{const policy=JSON.parse((await aws(['s3api','get-bucket-policy-status','--bucket',location.hostname])).stdout);if(policy.PolicyStatus?.IsPublic)throw Error('Bucket policy is public; refusing to store account data.');}catch(e){if(!/NoSuchBucketPolicy/.test(e.stderr??''))throw e;}
 let existing=false;
 try{await aws(['s3api','get-object','--bucket',location.hostname,'--key',key,path.join(directory,'existing.json')]);existing=true;}catch(e){if(!/NoSuchKey|\(404\)/.test(e.stderr??''))throw e;}
 if(existing){const db=JSON.parse(await readFile(path.join(directory,'existing.json'),'utf8'));if(db.users?.length){console.log('Accounts already exist. No accounts or passwords were changed.');process.exitCode=0;}else throw Error('Existing account store needs administrator review.');}
 else{
  const password=randomBytes(24).toString('base64url'),salt=randomBytes(16),hash=await new Promise((resolve,reject)=>scrypt(password,salt,64,{N:32768,r:8,p:3,maxmem:67108864},(e,v)=>e?reject(e):resolve(v)));
  const user={id:randomUUID(),name:'Administrator',email:'admin@pickleballdb.local',passwordHash:`scrypt$32768$8$3$${salt.toString('hex')}$${hash.toString('hex')}`,role:'admin',active:true,passwordVersion:1,mustChangePassword:true,createdAt:new Date().toISOString()};
  const privateDirectory=path.join(root,'.local/auth');await mkdir(privateDirectory,{recursive:true,mode:0o700});await chmod(privateDirectory,0o700);
  const credentialFile=path.join(privateDirectory,'initial-admin.txt');
  if(existsSync(credentialFile))throw Error('Initial credential file already exists; refusing to overwrite.');
  await writeFile(credentialFile,`PickleballDB first administrator\nLogin: http://localhost:3000/login\nEmail: ${user.email}\nTemporary password: ${password}\n\nChange this password at first login. This private file can then be deleted.\n`,{mode:0o600,flag:'wx'});
  credentialCreated=credentialFile;
  const file=path.join(directory,'accounts.json');await writeFile(file,JSON.stringify({schemaVersion:1,users:[user]}),{mode:0o600});
  await aws(['s3api','put-object','--bucket',location.hostname,'--key',key,'--body',file,'--if-none-match','*','--server-side-encryption','AES256','--content-type','application/json']);
  uploaded=true;
  console.log('First administrator created in S3. Private login details: '+credentialFile);
 }
}catch(e){if(credentialCreated&&!uploaded)await rm(credentialCreated,{force:true});const message=e.stderr??e.message;console.error(/expired|credential|login/i.test(message)?'AWS sign-in is required.':'Admin setup failed: '+message);process.exitCode=1;}finally{await rm(directory,{recursive:true,force:true});}
