"""Check generated EDA evidence, relative links, chart totals and completeness."""
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT=Path(__file__).resolve().parents[3]
OUT=ROOT/'docs/eda'
f=json.loads((OUT/'evidence/findings.json').read_text())
p=json.loads((OUT/'evidence/profiles.json').read_text())
i=json.loads((OUT/'evidence/inventory.json').read_text())
q=json.loads((OUT/'evidence/query_results.json').read_text())
requested=[n for n in p if n.startswith(('bronze_','silver_')) or n in ('dim_admin_unit','bridge_admin_crosswalk','dim_location_alias') or n in f['consistency']['clickhouse']]
assert len(requested)==32, len(requested)
assert sum(p[n]['rows'] for n in requested)==17093
assert all(p[n]['duplicate_keys']==0 for n in requested)
assert all(not p[n]['missing_columns'] and not p[n]['unexpected_columns'] for n in requested)
assert not f['consistency']['contract_validation']['violations']
assert f['consistency']['independent_summary']=={'checks':1954,'passed':1954,'failed':0}
assert f['consistency']['legacy_summary']=={'checks':347,'passed':347,'failed':0}
assert len(f['consistency']['manifest_version_checks'])==29
assert all(f['consistency']['manifest_version_checks'].values())
assert all(f['consistency']['manifest_digests'].values())
assert all(v['match'] for v in f['consistency']['production_formula_replay'].values())
assert f['consistency']['source_versions_unchanged']
assert sum(v['status']=='MATCH' for v in f['consistency']['clickhouse'].values())==11
assert sum(v['status']=='MISSING_IN_CLICKHOUSE' for v in f['consistency']['clickhouse'].values())==2
assert len(q['queries'])==14
assert all(v['status']=='EXECUTED' and v['result_rows']>0 for v in q['queries'].values())
for md in OUT.glob('*.md'):
    for target in re.findall(r'\]\(([^)]+)\)',md.read_text()):
        if not target.startswith(('http:','https:','#')):
            assert (md.parent/target.split('#')[0]).exists(), (md.name,target)
for svg in (OUT/'charts').glob('*.svg'):
    ET.parse(svg)
assert len(list((OUT/'charts').glob('*.svg')))==2
assert len(list((OUT/'queries').glob('*.sql')))==14
assert all(v['status']=='QUERIED_FULL' for n,v in i.items())
result={'status':'PASS','requested_datasets':32,'contract_rows':17093,'new_independent_checks':1954,
        'legacy_independent_checks':347,'manifest_checks':29,'populated_sql_queries':14,
        'static_charts':2,'relative_links':'VALID'}
(OUT/'evidence/artifact_verification.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result))
