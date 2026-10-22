"""Read-only, version-pinned EDA input capture. No lake/serving mutations."""
import json
import os
from datetime import datetime, date, timezone
from decimal import Decimal
from pathlib import Path

from delta.tables import DeltaTable
from dotenv import dotenv_values
from dsp_collector.runtime.bronze import DATASETS
from dsp_spark.runtime.config import SparkSettings
from dsp_spark.runtime.spark import create_spark
from dsp_spark.runtime.clickhouse import ClickHouseSettings
from dsp_spark.io.clickhouse import ClickHouseClient
from dsp_spark.io.intelligence_clickhouse import canonical_served
from dsp_spark.processing.intelligence.catalog import SILVER, GOLD

ROOT = Path(__file__).resolve().parents[3]
PRIVATE = ROOT / '.local/eda'
OUT = ROOT / 'docs/eda/evidence'
LIMIT = 250000
LEGACY = ('court_supply_by_location', 'social_activity_daily', 'location_market_signal_snapshot')
EXTRA = ('courts', 'social_posts', 'social_comments', 'intelligence_manifest')

def serial(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, datetime):
        # PySpark returns TimestampType as host-local naive datetimes. Convert
        # that actual local instant; attaching UTC would shift it by seven hours.
        return v.astimezone(timezone.utc).isoformat(timespec='microseconds').replace('+00:00','Z')
    if isinstance(v, date):
        return v.isoformat()
    raise TypeError(type(v).__name__)

def main():
    PRIVATE.mkdir(parents=True, exist_ok=True)
    OUT.mkdir(parents=True, exist_ok=True)
    # Reuse local settings without printing connection credentials.
    for k, v in dotenv_values(ROOT / '.env', interpolate=False).items():
        if v is not None and k.startswith(('CLICKHOUSE_', 'DSP_LAKEHOUSE_', 'DSP_DELTA_')):
            os.environ.setdefault(k, v)
    settings = SparkSettings.from_env()
    spark = create_spark(settings)
    captured = {'started_at': datetime.now(timezone.utc).isoformat(), 'tables': {}, 'clickhouse': {},
                'timestamp_serialization': 'HOST_LOCAL_NAIVE_CONVERTED_TO_UTC'}
    inventory = {}
    try:
        for name in (*DATASETS, *SILVER, *GOLD, *LEGACY, *EXTRA):
            path = getattr(settings.paths, name)
            if not DeltaTable.isDeltaTable(spark, path):
                inventory[name] = {'status': 'MISSING', 'path': path}
                continue
            dt = DeltaTable.forPath(spark, path)
            hist = dt.history(1).first().asDict()
            version = hist['version']
            df = spark.read.format('delta').option('versionAsOf', version).load(path)
            rows = [r.asDict(recursive=True) for r in df.limit(LIMIT + 1).collect()]
            if len(rows) > LIMIT:
                raise RuntimeError(f'{name}: exceeds full-scan driver guard; not a full EDA')
            rows = json.loads(json.dumps(rows, default=serial))
            captured['tables'][name] = rows
            inventory[name] = {'status': 'QUERIED_FULL', 'rows': len(rows), 'path': path,
                               'delta_version': version, 'last_commit_at': serial(hist['timestamp']),
                               'schema': df.schema.jsonValue()}
            print(f'{name}: {len(rows)} rows, version {version}', flush=True)
        for name, entry in inventory.items():
            if entry['status'] == 'QUERIED_FULL':
                entry['version_after_read'] = DeltaTable.forPath(spark, entry['path']).history(1).first()['version']
                entry['unchanged_during_read'] = entry['delta_version'] == entry['version_after_read']
    finally:
        spark.stop()
    (PRIVATE / 'snapshot.json').write_text(json.dumps(captured, ensure_ascii=False))
    (OUT / 'inventory.json').write_text(json.dumps(inventory, indent=2, ensure_ascii=False))
    with ClickHouseClient(ClickHouseSettings.from_env()) as client:
        captured['database'] = client.settings.database
        for name in (*GOLD, *LEGACY):
            exists = client.query(f"EXISTS TABLE {client.settings.database}.{name}")[0]['result']
            if not exists:
                inventory[name]['clickhouse_status'] = 'MISSING'
                continue
            fields = client.query(f'DESCRIBE TABLE {client.settings.database}.{name}')
            inventory[name]['clickhouse_status'] = 'QUERIED_FULL'
            inventory[name]['clickhouse_schema'] = fields
            count = client.query(f'SELECT count() AS n FROM {client.settings.database}.{name}')[0]['n']
            if count > LIMIT:
                raise RuntimeError(f'{name}: ClickHouse driver guard exceeded')
            rows = canonical_served(client, name, name) if name in GOLD else client.query(f'SELECT * FROM {client.settings.database}.{name}')
            captured['clickhouse'][name] = rows
            inventory[name]['clickhouse_rows'] = len(rows)
    captured['finished_at'] = datetime.now(timezone.utc).isoformat()
    (PRIVATE / 'snapshot.json').write_text(json.dumps(captured, ensure_ascii=False))
    (OUT / 'inventory.json').write_text(json.dumps(inventory, indent=2, ensure_ascii=False))
    print('Captured full read-only snapshots; raw records saved only under .local/eda.')

if __name__ == '__main__':
    main()
