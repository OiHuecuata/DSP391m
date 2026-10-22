"""Superset public REST client with JWT + CSRF and credential-safe errors."""
import os
import requests
import prison

class API:
    def __init__(self,base='http://127.0.0.1:8088'):
        self.base=base
        self.http=requests.Session()
        self.login()

    def login(self):
        self.http.headers.pop('Authorization',None)
        self.http.headers.pop('X-CSRFToken',None)
        result=self.call('POST','security/login',{'username':os.environ['SUPERSET_ADMIN_USERNAME'],'password':os.environ['SUPERSET_ADMIN_PASSWORD'],'provider':'db','refresh':True})
        self.http.headers['Authorization']='Bearer '+result['access_token']
        token=self.call('GET','security/csrf_token/')['result']
        self.http.headers['X-CSRFToken']=token
        self.http.headers['Referer']=self.base+'/'

    def call(self,method,path,payload=None,params=None):
        r=self.http.request(method,self.base+'/api/v1/'+path,json=payload,params=params,timeout=120)
        if r.status_code>=400:
            error=r.text[:2500]
            for key in ('SUPERSET_ADMIN_PASSWORD','SUPERSET_SECRET_KEY','SUPERSET_METADATA_PASSWORD','SUPERSET_CLICKHOUSE_PASSWORD'):
                error=error.replace(os.getenv(key,'UNSET'),'<redacted>')
            raise RuntimeError(f'{method} {path}: HTTP {r.status_code}: {error}')
        return r.json() if r.content else {}

    def list(self,resource,filters=()):
        result=[]; page=0
        while True:
            q={'page':page,'page_size':100,'filters':list(filters)}
            out=self.call('GET',resource+'/',params={'q':prison.dumps(q)})
            result.extend(out['result'])
            if len(result)>=out['count']: return result
            page+=1

    def find(self,resource,column,value):
        found=self.list(resource,[{'col':column,'opr':'eq','value':value}])
        if len(found)>1: raise RuntimeError(f'Ambiguous managed asset: {resource} {value}')
        return found[0] if found else None
