// This receipt contains profile metadata only; secrets stay in ~/.aws/credentials.
import {readFileSync} from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../..');
export function websiteAwsSettings(env=process.env){
 let saved={};
 try{saved=JSON.parse(readFileSync(path.join(root,'.local/website-data/aws-identity.json'),'utf8'));}
 catch(error){if(error.code!=='ENOENT')throw new Error('Invalid website AWS identity configuration.');}
 const profile=env.TRACKER_AWS_PROFILE??saved.profile??'tracker';
 const region=env.AWS_REGION??saved.region??'ap-southeast-2';
 if(typeof profile!=='string'||!/^[-A-Za-z0-9_.@]{1,128}$/.test(profile)||typeof region!=='string'||!/^[-a-z0-9]+-\d+$/.test(region))throw new Error('Invalid website AWS profile or region.');
 return {profile,region};
}
