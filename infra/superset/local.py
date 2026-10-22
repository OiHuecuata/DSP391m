"""Host setup for persistent secrets, metadata backup and read-only CH account.

Never prints credentials. It does not ingest or mutate analytical tables.
"""
import json
import os
import secrets
import subprocess
import hashlib
from xml.sax.saxutils import escape
from pathlib import Path
from dotenv import dotenv_values

ROOT=Path(__file__).resolve().parents[2]
LOCAL=ROOT/'.local/superset'
ENV=LOCAL/'.env'
COMPOSE=['docker','compose','--env-file',str(ENV),'-f',str(ROOT/'docker-compose.yaml'),'-f',str(ROOT/'infra/superset/compose.yaml'),'--profile','superset']

def run(args,**kwargs):
    return subprocess.run(args,check=True,cwd=ROOT,**kwargs)

def inspect(name):
    p=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
    if p.returncode:
        return None
    return json.loads(p.stdout)[0]

def prepare():
    LOCAL.mkdir(parents=True,exist_ok=True)
    (LOCAL/'config').mkdir(exist_ok=True)
    (LOCAL/'runtime').mkdir(exist_ok=True)
    os.chmod(LOCAL/'runtime',0o777)  # bind results written by uid 1000 container
    env={k:v for k,v in dotenv_values(ENV).items() if v}
    rootenv=dotenv_values(ROOT/'.env',interpolate=False)
    env.setdefault('CLICKHOUSE_DATABASE',rootenv.get('CLICKHOUSE_DATABASE') or 'dsp391m')
    env.setdefault('SUPERSET_PORT','8088')
    env.setdefault('SUPERSET_ADMIN_USERNAME','dsp_eda_admin')
    env.setdefault('SUPERSET_ADMIN_EMAIL','eda@localhost.invalid')
    env.setdefault('SUPERSET_CLICKHOUSE_USER','dsp_superset_eda')
    secret_file=LOCAL/'config/secret_key'
    if secret_file.exists():
        # The persistent file is authoritative, including a restored backup.
        env['SUPERSET_SECRET_KEY']=secret_file.read_text().strip()
    metadata_file=LOCAL/'config/metadata_password'
    if metadata_file.exists():
        env['SUPERSET_METADATA_PASSWORD']=metadata_file.read_text().strip()
    # Preserve original environment secrets if that deployment used env values.
    old=inspect('dsp391m-superset-1')
    if old:
        oldenv=dict(item.split('=',1) for item in old['Config']['Env'] if '=' in item)
        if oldenv.get('SUPERSET_SECRET_KEY'):
            env.setdefault('SUPERSET_SECRET_KEY',oldenv['SUPERSET_SECRET_KEY'])
    lost_key=bool(old) and not env.get('SUPERSET_SECRET_KEY')
    for k in ('SUPERSET_SECRET_KEY','SUPERSET_ADMIN_PASSWORD','SUPERSET_METADATA_PASSWORD','SUPERSET_CLICKHOUSE_PASSWORD'):
        env.setdefault(k,secrets.token_hex(32))
    for k,v in env.items():
        if '\n' in v or '\r' in v:
            raise ValueError(f'{k}: multiline environment value is unsupported')
    ENV.write_text(''.join(f"{k}='{v.replace(chr(39), chr(92)+chr(39))}'\n" for k,v in sorted(env.items())))
    os.chmod(ENV,0o600)
    for filename,key in [('secret_key','SUPERSET_SECRET_KEY'),('metadata_password','SUPERSET_METADATA_PASSWORD')]:
        p=LOCAL/'config'/filename; p.write_text(env[key]+'\n'); os.chmod(p,0o600)
    user=env['SUPERSET_CLICKHOUSE_USER']
    if not user.replace('_','').isalnum(): raise ValueError('Reader name must be an identifier')
    import sys
    sys.path.insert(0,str(ROOT/'pipelines/spark/src'))
    from dsp_spark.processing.intelligence.catalog import GOLD
    grants=''.join(f'<query>GRANT SELECT ON {escape(env["CLICKHOUSE_DATABASE"])}.{table}</query>' for table in GOLD)
    hashed=hashlib.sha256(env['SUPERSET_CLICKHOUSE_PASSWORD'].encode()).hexdigest()
    xml=f'<clickhouse><profiles><dsp_eda_readonly><readonly>1</readonly></dsp_eda_readonly></profiles><users><{user}><password_sha256_hex>{hashed}</password_sha256_hex><networks><ip>127.0.0.1</ip><ip>172.16.0.0/12</ip></networks><profile>dsp_eda_readonly</profile><quota>default</quota><grants>{grants}</grants></{user}></users></clickhouse>\n'
    p=LOCAL/'config/clickhouse_eda.xml'; p.write_text(xml); os.chmod(p,0o644)
    # PostgreSQL's container runs as root then drops privileges; app secret env
    # comes from Compose, so host secret files can remain owner-readable only.
    state=LOCAL/'credential_state.json'
    if lost_key and not state.exists():
        state.write_text(json.dumps({'previous_key_available':False,'metadata_preserved':True,'note':'Old encrypted connection credentials need the previous key; new EDA connection uses the new persistent key.'},indent=2))
    print('Persistent local credentials prepared; values are not printed.')

def configure_metadata():
    env=dotenv_values(ENV)
    backup=LOCAL/'metadata-before-eda.sql'
    if not backup.exists():
        with backup.open('wb') as f:
            run(COMPOSE+['exec','-T','superset-db','pg_dump','-U','superset','-d','superset'],stdout=f)
        os.chmod(backup,0o600)
    # Existing PG volume keeps its old role password. Set only this metadata
    # role's password via the local trusted socket, preserving all stored data.
    pw=env['SUPERSET_METADATA_PASSWORD'].replace("'","''")
    run(COMPOSE+['exec','-T','superset-db','psql','-U','superset','-d','superset','-v','ON_ERROR_STOP=1'],input=f"ALTER ROLE superset WITH PASSWORD '{pw}';\n",text=True,stdout=subprocess.DEVNULL)
    print('Metadata backup saved; existing metadata and role retained.')

def configure_reader():
    import sys
    sys.path[:0]=[str(ROOT/'pipelines/spark/src'),str(ROOT/'apps/collector/src')]
    from dsp_spark.io.clickhouse import ClickHouseClient
    from dsp_spark.runtime.clickhouse import ClickHouseSettings
    from dsp_spark.processing.intelligence.catalog import GOLD
    rootenv=dotenv_values(ROOT/'.env',interpolate=False)
    env=dotenv_values(ENV)
    settings=ClickHouseSettings(host='localhost',port=int(rootenv.get('CLICKHOUSE_PORT') or 8123),database=env['CLICKHOUSE_DATABASE'],user=rootenv.get('CLICKHOUSE_USER') or 'dsp391m',password=rootenv.get('CLICKHOUSE_PASSWORD') or '')
    # The existing loader account cannot CREATE USER. A persisted isolated XML
    # user config adds only SELECT on ten tables; no existing user is changed.
    run(COMPOSE+['up','-d','--wait','--wait-timeout','180','clickhouse'])
    with ClickHouseClient(settings) as ch:
        inventory=[]
        for table in GOLD:
            inventory.append({'table':table,'rows':ch.query(f'SELECT count() AS n FROM {settings.database}.{table}')[0]['n'],'columns':ch.query(f'DESCRIBE TABLE {settings.database}.{table}')})
        legacy={t:ch.query(f'EXISTS TABLE {settings.database}.{t}')[0]['result'] for t in ('court_supply_by_location','social_activity_daily','location_market_signal_snapshot')}
    (LOCAL/'runtime/clickhouse_inventory.json').write_text(json.dumps({'database':settings.database,'tables':inventory,'legacy_exists':legacy},indent=2))
    print(f'Read-only ClickHouse user configured for {len(GOLD)} Gold tables; no analytical data changed.')

if __name__=='__main__':
    import sys
    {'prepare':prepare,'metadata':configure_metadata,'reader':configure_reader}[sys.argv[1]]()
