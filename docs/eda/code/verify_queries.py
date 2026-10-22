"""Execute every saved SELECT against existing ClickHouse without mutations."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from dotenv import dotenv_values
from dsp_spark.io.clickhouse import ClickHouseClient
from dsp_spark.runtime.clickhouse import ClickHouseSettings

ROOT=Path(__file__).resolve().parents[3]
for k,v in dotenv_values(ROOT/'.env',interpolate=False).items():
    if k.startswith('CLICKHOUSE_') and v is not None:
        os.environ.setdefault(k,v)
result={'executed_at':datetime.now(timezone.utc).isoformat(),'queries':{}}
with ClickHouseClient(ClickHouseSettings.from_env()) as client:
    for p in sorted((ROOT/'docs/eda/queries').glob('*.sql')):
        sql='\n'.join(line for line in p.read_text().splitlines() if not line.startswith('--')).rstrip().rstrip(';')
        assert sql.lstrip().startswith(('SELECT','WITH')) and ';' not in sql
        sql=sql.replace('dsp391m.',client.settings.database+'.')
        rows=client.query(sql)
        if not rows:
            raise RuntimeError(f'{p.name}: unexpectedly empty result; verify populated-data filters')
        result['queries'][p.name]={'status':'EXECUTED','result_rows':len(rows),'results':rows}
        print(f'{p.name}: {len(rows)} result rows',flush=True)
(ROOT/'docs/eda/evidence/query_results.json').write_text(json.dumps(result,indent=2,ensure_ascii=False))
