"""One-time repair of the initial EDA reader's naive timestamp serialization.

This touches ONLY the analysis capture, never Delta or ClickHouse. Current
read_snapshot.py already converts correctly and this repair becomes a no-op.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
path = ROOT/'.local/eda/snapshot.json'
snap = json.loads(path.read_text())
if snap.get('timestamp_serialization') != 'HOST_LOCAL_NAIVE_CONVERTED_TO_UTC':
    inventory_path = ROOT/'docs/eda/evidence/inventory.json'
    inventory = json.loads(inventory_path.read_text())
    def fix(v):
        return datetime.fromisoformat(v).replace(tzinfo=None).astimezone(timezone.utc).isoformat(timespec='microseconds').replace('+00:00','Z')
    for n, rr in snap['tables'].items():
        fields = [f['name'] for f in inventory[n]['schema']['fields'] if f['type']=='timestamp']
        for r in rr:
            for k in fields:
                if r.get(k):
                    r[k] = fix(r[k])
        inventory[n]['last_commit_at'] = fix(inventory[n]['last_commit_at'])
    snap['timestamp_serialization'] = 'HOST_LOCAL_NAIVE_CONVERTED_TO_UTC'
    path.write_text(json.dumps(snap,ensure_ascii=False))
    inventory_path.write_text(json.dumps(inventory,indent=2,ensure_ascii=False))
    print('Corrected only captured TimestampType fields; UTC string fields preserved.')
else:
    print('Capture already uses correct UTC conversion; unchanged.')
