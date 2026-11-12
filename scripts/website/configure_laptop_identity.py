"""Provision an explicitly requested, narrowly scoped S3 website IAM user.

Secrets are captured in memory and written only to ~/.aws/credentials (0600).
No root keys are created; no existing profile or IAM identity is overwritten.
"""
import argparse
import configparser
import json
import os
from pathlib import Path
import subprocess
import tempfile
from datetime import datetime,timezone

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--aws',required=True)
parser.add_argument('--admin-profile',default='tracker')
parser.add_argument('--execute',action='store_true')
args=parser.parse_args()
root=Path(__file__).resolve().parents[2]
username='pickleballdb-website-laptop'
profile='tracker-website'
policy_name='PickleballDBWebsiteS3Only'
policy_file=Path(__file__).parent/'iam/website-laptop-policy.json'
policy=json.loads(policy_file.read_text())
if not args.execute:
 print(json.dumps({'user':username,'profile':profile,'policy':policy},indent=2))
 raise SystemExit(0)

def aws(*command):
 result=subprocess.run([args.aws,*command,'--profile',args.admin_profile,'--region','ap-southeast-2','--output','json'],capture_output=True,text=True,timeout=45,env={**os.environ,'AWS_PAGER':''})
 if result.returncode:
  raise RuntimeError(command[0]+' '+command[1]+': '+result.stderr.strip()[:800])
 return json.loads(result.stdout) if result.stdout.strip() else {}

def atomic(path,text):
 path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
 fd,name=tempfile.mkstemp(prefix='.website-',dir=path.parent)
 try:
  os.fchmod(fd,0o600)
  with os.fdopen(fd,'w') as stream:
   stream.write(text);stream.flush();os.fsync(stream.fileno())
  os.replace(name,path);os.chmod(path,0o600)
 finally:
  if os.path.exists(name):os.unlink(name)

folder=Path.home()/'.aws'
folder.mkdir(parents=True,exist_ok=True,mode=0o700)
folder.chmod(0o700)
credentials=folder/'credentials'
config=folder/'config'
old_credentials=credentials.read_text() if credentials.exists() else ''
old_config=config.read_text() if config.exists() else ''
for text,section in [(old_credentials,profile),(old_config,'profile '+profile)]:
 parsed=configparser.RawConfigParser();parsed.read_string(text)
 if parsed.has_section(section):raise RuntimeError('Website profile already exists; refusing to overwrite credentials. Verify the existing identity instead.')
try:
 aws('iam','get-user','--user-name',username)
except RuntimeError as e:
 if 'NoSuchEntity' not in str(e):raise
else:raise RuntimeError('IAM identity already exists; refusing to modify an unknown existing user.')
created=False;key_id=None;persisted=False
try:
 user=aws('iam','create-user','--user-name',username,'--tags','Key=Project,Value=PickleballDB','Key=Purpose,Value=WebsiteS3Only','Key=ManagedBy,Value=Codex')['User'];created=True
 aws('iam','put-user-policy','--user-name',username,'--policy-name',policy_name,'--policy-document','file://'+str(policy_file))
 key=aws('iam','create-access-key','--user-name',username)['AccessKey'];key_id=key['AccessKeyId']
 atomic(credentials,old_credentials.rstrip()+'\n\n['+profile+']\naws_access_key_id = '+key_id+'\naws_secret_access_key = '+key['SecretAccessKey']+'\n')
 persisted=True
 atomic(config,old_config.rstrip()+'\n\n[profile '+profile+']\nregion = ap-southeast-2\noutput = json\n')
 receipt={'profile':profile,'region':'ap-southeast-2','userName':username,'userArn':user['Arn'],'policyName':policy_name,'configuredAt':datetime.now(timezone.utc).isoformat()}
 atomic(root/'.local/website-data/aws-identity.json',json.dumps(receipt,indent=2)+'\n')
 print(json.dumps({'configured':True,'profile':profile,'userArn':user['Arn'],'credentialsStored':'~/.aws/credentials','mode':'0600','secretPrinted':False}))
except BaseException:
 if not persisted and created:
  if key_id:aws('iam','delete-access-key','--user-name',username,'--access-key-id',key_id)
  aws('iam','delete-user-policy','--user-name',username,'--policy-name',policy_name)
  aws('iam','delete-user','--user-name',username)
 raise
