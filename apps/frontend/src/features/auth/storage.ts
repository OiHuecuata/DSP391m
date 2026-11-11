import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {existsSync} from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import {S3Client,GetObjectCommand,PutObjectCommand} from '@aws-sdk/client-s3';
const exec=promisify(execFile);
export class AuthError extends Error{constructor(message:string,public status=503){super(message);}}
export type Account={id:string;name:string;email:string;passwordHash:string;role:'user'|'admin';active:boolean;passwordVersion:number;mustChangePassword:boolean;createdAt:string};
export type AccountDB={schemaVersion:1;users:Account[]};
type Accounts={db:AccountDB;etag:string|null};
type Credentials={accessKeyId:string;secretAccessKey:string;sessionToken?:string;expiration:Date};
const shared=globalThis as typeof globalThis&{accountClients?:Map<string,S3Client>;accountReads?:Map<string,Promise<Accounts>>};
export function authSource(){const uri=new URL(process.env.TRACKER_AUTH_S3_URI??'s3://dsp391m-data-lake/website-auth/v1');if(uri.protocol!=='s3:'||!uri.hostname||!uri.pathname.replace(/^\//,'')||uri.pathname.split('/').includes('..'))throw new AuthError('Account storage is not configured.');return {bucket:uri.hostname,key:uri.pathname.replace(/^\//,'').replace(/\/$/,'')+'/accounts.json'};}
function storageError(error:unknown):AuthError{
 if(error instanceof AuthError)return error;
 const item=error as {name?:string;message?:string;stderr?:string;$metadata?:{httpStatusCode?:number}};
 if(item.name==='PreconditionFailed'||item.name==='ConditionalRequestConflict'||[409,412].includes(item.$metadata?.httpStatusCode??0))return new AuthError('Accounts changed. Please retry.',409);
 const text=[item.name,item.message,item.stderr].join(' ');
 return new AuthError(/read-only file system|permission denied|EACCES/i.test(text)?'AWS credentials cannot refresh because the local credential folder is not writable. Restart the website with access to ~/.aws.':/expired|login|credential|token/i.test(text)?'AWS sign-in is required on this computer. Saved accounts remain in S3.':'Account storage is unavailable. Check S3 access.');
}
function connection(){
 const bundled=path.join(os.homedir(),'Documents/Codex/tools/bin/aws'),cli=process.env.TRACKER_AWS_CLI??(existsSync(bundled)?bundled:'aws');
 const region=process.env.AWS_REGION??'ap-southeast-2',profile=process.env.TRACKER_AWS_PROFILE??'tracker';
 const source=authSource(),key=JSON.stringify([cli,profile,region,source.bucket,source.key]);
 const clients=shared.accountClients??(shared.accountClients=new Map());let client=clients.get(key);
 if(!client){
  let credentials:Credentials|undefined,pending:Promise<Credentials>|undefined;
  const provide=async()=>{
   if(credentials&&credentials.expiration.getTime()>Date.now()+60000)return credentials;
   if(!pending)pending=(async()=>{
    // Invoke the CLI only to renew temporary credentials, never for each S3 request.
    const {stdout}=await exec(cli,['configure','export-credentials','--profile',profile,'--region',region,'--format','process'],{timeout:25000,maxBuffer:65536,env:{...process.env,AWS_PAGER:''}});
    const value=JSON.parse(stdout);if(typeof value.AccessKeyId!=='string'||typeof value.SecretAccessKey!=='string')throw new AuthError('AWS sign-in is required on this computer. Saved accounts remain in S3.');
    const expiration=value.Expiration?new Date(value.Expiration):new Date(Date.now()+300000);if(!Number.isFinite(expiration.getTime()))throw new AuthError('AWS credential expiry is invalid.');
    credentials={accessKeyId:value.AccessKeyId,secretAccessKey:value.SecretAccessKey,sessionToken:value.SessionToken,expiration};return credentials;
   })().finally(()=>{pending=undefined;});
   return pending;
  };
  client=new S3Client({region,credentials:provide,maxAttempts:2,requestHandler:{connectionTimeout:3000,requestTimeout:10000}});clients.set(key,client);
 }
 return {client,source,key};
}
export async function readAccounts():Promise<Accounts>{
 const {client,source,key}=connection(),reads=shared.accountReads??(shared.accountReads=new Map());let pending=reads.get(key);
 if(!pending){
  pending=(async()=>{try{
   const response=await client.send(new GetObjectCommand({Bucket:source.bucket,Key:source.key}));
   if(!response.Body||(response.ContentLength??0)>1024*1024)throw new AuthError('Invalid account storage format.');
   const text=await response.Body.transformToString();if(Buffer.byteLength(text)>1024*1024)throw new AuthError('Invalid account storage format.');
   const db:AccountDB=JSON.parse(text);if(db.schemaVersion!==1||!Array.isArray(db.users))throw new AuthError('Invalid account storage format.');return {db,etag:response.ETag??null};
  }catch(error){const e=error as {name?:string;$metadata?:{httpStatusCode?:number}};if(e.name==='NoSuchKey'||e.$metadata?.httpStatusCode===404)return {db:{schemaVersion:1 as const,users:[]},etag:null};throw storageError(error);}})();
  reads.set(key,pending);
 }
 try{return structuredClone(await pending);}finally{if(reads.get(key)===pending)reads.delete(key);}
}
export async function writeAccounts(db:AccountDB,etag:string|null){const {client,source,key}=connection();try{await client.send(new PutObjectCommand({Bucket:source.bucket,Key:source.key,Body:JSON.stringify(db),ContentType:'application/json',ServerSideEncryption:'AES256',...(etag?{IfMatch:etag}:{IfNoneMatch:'*'})}));}catch(error){throw storageError(error);}finally{shared.accountReads?.delete(key);}}
