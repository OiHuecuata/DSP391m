"""Full observed-population EDA, standard library only; never writes source data.

Input is a version-pinned read_snapshot.py capture in ignored .local/eda.
Quantiles use linear interpolation (type 7); NULLs are excluded, never zero-filled.
Counts are sampled-source counts, not Vietnamese population estimates.
"""
import hashlib
import itertools
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, date, timezone
from pathlib import Path
from statistics import mean, median
from zoneinfo import ZoneInfo

from dsp_collector.runtime.bronze import DATASETS
from dsp_spark.processing.clean.contracts import contract_documents
from dsp_spark.processing.intelligence.catalog import SILVER, GOLD, KEYS
from dsp_spark.processing.intelligence.geography import PRE, POST, assignments
from dsp_spark.processing.intelligence.venues import court_count, price

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / 'docs/eda/evidence'
LEGACY = ('court_supply_by_location', 'social_activity_daily', 'location_market_signal_snapshot')

def instant(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))

def quantile(a, p):
    if not a:
        return None
    a = sorted(a)
    pos = (len(a) - 1) * p
    lo, hi = math.floor(pos), math.ceil(pos)
    return a[lo] + (a[hi] - a[lo]) * (pos - lo)

def stats(values):
    a = [v for v in values if v is not None]
    return {'n': len(a), 'min': min(a) if a else None, 'max': max(a) if a else None,
            'mean': mean(a) if a else None, 'median': median(a) if a else None,
            **{f'p{p}': quantile(a, p/100) for p in (5,25,75,90,95,99)}}

def count(rows, field):
    return dict(Counter(str(r.get(field)) if r.get(field) is not None else 'NULL' for r in rows))

def groups(rows, fields):
    result = defaultdict(list)
    for r in rows:
        result[tuple(r.get(k) for k in fields)].append(r)
    return result

def grouped(rows, fields, metrics=()):
    return [dict(zip(fields, key), rows=len(rr),
                 **{m: sum(r[m] for r in rr if r.get(m) is not None)
                    if any(r.get(m) is not None for r in rr) else None for m in metrics})
            for key, rr in sorted(groups(rows, fields).items(), key=lambda x: str(x[0]))]

def digest(rows):
    return hashlib.sha256('\n'.join(sorted(json.dumps(r,sort_keys=True,ensure_ascii=False,separators=(',',':')) for r in rows)).encode()).hexdigest()

def normalized_rows(rows, schema):
    # Date/time representations differ between Delta strings and ClickHouse.
    result = []
    for r in rows:
        rr = dict(r)
        for col, spec in schema.items():
            v = rr.get(col)
            if v is not None and spec.get('format') == 'date-time':
                dt = instant(v)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                rr[col] = dt.isoformat(timespec='microseconds').replace('+00:00','Z')
            if v is not None and 'number' in spec.get('type', []):
                rr[col] = float(v)
        result.append(rr)
    return result

def profile(name, rows, columns, contracts):
    n = len(rows)
    fields = {f['name']: f['type'] for f in columns}
    expected = contracts.get(name,{}).get('properties',{})
    result = {'rows': n, 'missing_columns': sorted(set(expected)-set(fields)),
              'unexpected_columns': sorted(set(fields)-set(expected)), 'nulls': {},
              'empty_values': {}, 'numeric': {}, 'categories': {}, 'dates': {}}
    keys = KEYS.get(name, ('observation_id',) if name in DATASETS else
                    ('location_id',) if name == 'court_supply_by_location' else
                    ('snapshot_date','location_id') if name != 'social_activity_daily' else ('activity_date','location_id'))
    if all(k in fields for k in keys):
        result['grain'] = list(keys)
        result['duplicate_keys'] = n - len({tuple(r.get(k) for k in keys) for r in rows})
    categorical = {'source_platform','source','platform','record_type','content_type','execution_status',
                   'change_type','entity_record_type','metric_scope','verification_status','court_count_status',
                   'venue_status','match_status','match_method','geocoding_precision','coordinate_source',
                   'classification_status','extraction_method','is_pickleball_related','is_commercial','sentiment',
                   'is_reply','is_duplicate','snapshot_status','delta_status','resolution_status','resolution_precision',
                   'provider','dependency_status','requested_time_precision','actual_booking_confirmed',
                   'admin_level','admin_version','alias_type','relation_type','mapping_method','reference_status',
                   'collection_status','query_evidence_status','term_admin_version','social_coverage_status',
                   'supply_coverage_status','completeness_status','price_unit','language','language_raw'}
    for k, dtype in fields.items():
        null = sum(r.get(k) is None for r in rows)
        result['nulls'][k] = {'count':null,'rate':null/n if n else None}
        result['empty_values'][k] = sum(r.get(k) in ('',[]) for r in rows)
        nonnull = [r[k] for r in rows if r.get(k) is not None]
        if nonnull and isinstance(nonnull[0], (int,float)) and not isinstance(nonnull[0],bool):
            result['numeric'][k] = stats(nonnull)
        if k in categorical:
            result['categories'][k] = count(rows,k)
        if expected.get(k,{}).get('format') or k.endswith(('_at','_date')) or k in ('month','week_start','period_start','period_end'):
            if nonnull:
                result['dates'][k] = {'min': min(nonnull),'max': max(nonnull),'distinct':len(set(nonnull))}
        if k.endswith('_labels') or k in ('metrics_available','conflicting_fields','source_platforms'):
            result['categories'][k] = dict(Counter(v for r in rows for v in (r.get(k) or [])))
    return result

def snapshot_profile(rows, bronze=False):
    platform = 'source_platform' if bronze else 'platform'
    cid = 'source_record_id' if bronze else 'content_id'
    when = 'observed_at'
    metrics = ('view_count','like_count','comment_count','share_count','repost_count','quote_count','reply_count') if bronze else ('views','likes','comments','shares','reposts','quotes','replies')
    result = []
    for key, rr in groups(rows,(platform,'content_type')).items():
        entities = groups(rr,(cid,))
        intervals, deltas, latest = [], defaultdict(list), []
        equal_times = 0
        for snapshots in entities.values():
            ordered = sorted(snapshots,key=lambda r: r[when])
            eligible = ordered if bronze else [r for r in ordered if r['snapshot_status']=='AVAILABLE']
            if eligible:
                latest.append(eligible[-1])
            for a,b in zip(eligible,eligible[1:]):
                elapsed = (instant(b[when])-instant(a[when])).total_seconds()/3600
                equal_times += elapsed == 0
                if elapsed > 0:
                    intervals.append(elapsed)
                    for m in metrics:
                        if a[m] is not None and b[m] is not None:
                            deltas[m].append(b[m]-a[m])
        result.append({platform:key[0],'content_type':key[1],'observations':len(rr),'distinct_content':len(entities),
                       'snapshots_per_content':stats(map(len,entities.values())),
                       'single_snapshot_content':sum(len(v)==1 for v in entities.values()),
                       'interval_hours':stats(intervals),'equal_observation_times':equal_times,
                       'availability':{m:{'available':sum(r[m] is not None for r in rr),'null_rate':sum(r[m] is None for r in rr)/len(rr)} for m in metrics},
                       'latest_metrics':{m:stats(r[m] for r in latest) for m in metrics},
                       'delta_metrics':{m:{**stats(vals),'negative_count':sum(v<0 for v in vals),
                           'above_q3_plus_3iqr':sum(v>quantile(vals,.75)+3*(quantile(vals,.75)-quantile(vals,.25)) for v in vals)} for m,vals in deltas.items()}})
    return result

def main():
    snap = json.loads((ROOT/'.local/eda/snapshot.json').read_text())
    tables = snap['tables']
    inventory = json.loads((OUT/'inventory.json').read_text())
    contracts = json.loads(contract_documents())
    profiles = {n:profile(n,rr,inventory[n]['schema']['fields'],contracts) for n,rr in tables.items() if n!='intelligence_manifest'}
    (OUT/'profiles.json').write_text(json.dumps(profiles,indent=2,ensure_ascii=False))
    result = {'capture_started_at':snap['started_at'],'capture_finished_at':snap['finished_at'],'bronze':{},'silver':{},'gold':{},'consistency':{}}
    b = result['bronze']
    for n in DATASETS:
        rr=tables[n]
        entities=groups(rr,('source_platform','source_record_id','record_type'))
        daily=count([{'day':r['collected_at'][:10]} for r in rr],'day')
        byrun=Counter(r['collection_run_id'] for r in rr)
        b[n]={'observations':len(rr),'distinct_source_entities':len(entities),
              'duplicate_entity_run_time':len(rr)-len({(r['source_platform'],r['source_record_id'],r['record_type'],r['collection_run_id'],r.get('observed_at',r['collected_at'])) for r in rr}),
              'observations_per_entity':stats(map(len,entities.values())), 'collection_days':daily,
              'top_run_observations':max(byrun.values()) if byrun else 0,
              'top_run_share':max(byrun.values())/len(rr) if byrun else None,
              'collection_run_count':len(byrun),
              'entities_with_changed_payload':sum(len({r['payload_hash'] for r in v})>1 for v in entities.values())}
        for field in ('text_raw','description_raw','title_raw','comment_text_raw'):
            if field in inventory[n]['schema']['fields'][0] or any(field in r for r in rr):
                b[n][field+'_length']=stats(len(r[field]) if r.get(field) is not None else None for r in rr)
        dated=[r for r in rr if r.get('published_at')]
        if dated:
            b[n]['publication_to_collection_days']=stats((instant(r['collected_at'])-instant(r['published_at'])).total_seconds()/86400 for r in dated)
            b[n]['published_after_collection']=sum(instant(r['published_at'])>instant(r['collected_at']) for r in dated)
            b[n]['platform_publication']= {str(key):{'n':len(v),'dated':sum(r.get('published_at') is not None for r in v),
                'min':min((r['published_at'] for r in v if r.get('published_at')),default=None),
                'max':max((r['published_at'] for r in v if r.get('published_at')),default=None)} for key,v in groups(rr,('source_platform',)).items()}
    courts=tables['bronze_court_observations']
    latest_courts=[max(rr,key=lambda r:r['collected_at']) for rr in groups(courts,('source_platform','source_record_id')).values()]
    b['court_parsing']={'observation_court_counts':stats(court_count(r['court_count_raw']) for r in courts),
                        'latest_entity_court_counts':stats(court_count(r['court_count_raw']) for r in latest_courts),
                        'raw_price_nonempty':sum(bool(r['price_raw']) for r in latest_courts),
                        'comparable_latest_min_prices':stats(price(r['price_raw'] or '')[0] for r in latest_courts)}
    b['engagement']=snapshot_profile(tables['bronze_engagement_snapshots'],True)
    runs=tables['bronze_collection_runs']
    b['run_outcomes']=grouped(runs,('source_platform','execution_status'),('records_returned','records_accepted','records_rejected'))
    b['run_days']=grouped([dict(r,day=r['collected_at'][:10]) for r in runs],('day','source_platform'),('records_returned','records_accepted','records_rejected'))
    b['run_intervals']={key[0]:stats((instant(y['collected_at'])-instant(x['collected_at'])).total_seconds()/3600 for x,y in zip(sorted(v,key=lambda r:r['collected_at']),sorted(v,key=lambda r:r['collected_at'])[1:])) for key,v in groups(runs,('source_platform',)).items()}
    b['run_duration_minutes']=stats((instant(r['ended_at'])-instant(r['started_at'])).total_seconds()/60 for r in runs if r['ended_at'] and r['started_at'])
    b['vn_run_days']=grouped([dict(r,day=instant(r['collected_at']).astimezone(ZoneInfo('Asia/Ho_Chi_Minh')).date().isoformat()) for r in runs],('day','source_platform'),('records_returned','records_accepted','records_rejected'))
    b['platform_run_concentration']={n:grouped([dict(r,one=1) for r in tables[n]],('source_platform','collection_run_id'),('one',)) for n in DATASETS[:3]}
    b['payload_key_frequencies']={n:dict(Counter(k for r in tables[n] for k in json.loads(r['raw_payload']))) for n in DATASETS if tables[n]}
    b['missing_original_artifacts']=sum(not Path(r['artifact_uri'].removeprefix('file://')).exists() for r in runs if r.get('artifact_uri') and not r['artifact_uri'].startswith(('s3:','s3a:','https:')))
    raw_changed=Counter(); attribute_changed=Counter()
    for rr in groups(courts,('source_platform','source_record_id')).values():
        parsed=[json.loads(r['raw_payload']) for r in rr]
        raw_changed.update(k for k in set().union(*(r.keys() for r in parsed)) if len({json.dumps(r.get(k),sort_keys=True) for r in parsed})>1)
        attribute_changed.update(k for k in ('venue_name_raw','address_raw','court_count_raw','price_raw','venue_status_raw') if len({str(r[k]) for r in rr})>1)
    b['court_changed_keys']={'raw_payload':dict(raw_changed),'projected_attributes':dict(attribute_changed)}
    b['changes']=grouped(tables['bronze_source_changes'],('source_platform','entity_record_type','change_type'))
    s=result['silver']; venues=tables['silver_court_venues']; candidates=tables['silver_venue_candidates']
    s['venue_status_counts']=grouped(venues,('verification_status','court_count_status','venue_status'))
    s['venue_source_counts']=count(venues,'source_count')
    s['venue_court_counts']=stats(r['court_count'] for r in venues if r['court_count_status']=='KNOWN')
    s['venue_prices']=stats(r['price_min_vnd'] for r in venues if r['price_unit']=='VND_PER_COURT_HOUR')
    eligible=[r for r in venues if r['verification_status']!='UNVERIFIED' and r['venue_status']!='CLOSED']
    known=[r for r in eligible if r['court_count_status']=='KNOWN']
    extreme=[r for r in known if r['court_count']>quantile([v['court_count'] for v in known],.99)]
    s['eligible_court_stats']=stats(r['court_count'] for r in known)
    s['court_outlier_review']=[{'venue_name':r['venue_name'],'court_count':r['court_count'],
        'court_total_share':r['court_count']/sum(v['court_count'] for v in known),
        'source_platforms':r['source_platforms']} for r in extreme]
    s['court_sensitivity_excluding_gt_p99']={'excluded':len(extreme), 'known_courts':sum(r['court_count'] for r in known if r not in extreme),
        'label':'Sensitivity only; stored supply is unchanged. Cutoff is sample p99, not verified invalidity.'}
    s['candidate_matches']=grouped(candidates,('source_platform','match_status','verification_status'))
    s['venue_province_coverage']={'old':sum(r['old_province_id'] is not None for r in venues),'new':sum(r['new_province_id'] is not None for r in venues),
        'any':sum(bool(r['old_province_id'] or r['new_province_id']) for r in venues),'poi':sum(r['geocoding_precision']=='POI' for r in venues),
        'coordinate_pairs':sum(r['latitude'] is not None and r['longitude'] is not None for r in venues)}
    same_names=groups(venues,('venue_name',))
    s['potential_duplicate_names']=[{'name':k[0],'venues':len(rr),'distinct_addresses':len({r['address_normalized'] for r in rr}),
        'court_counts': [r['court_count'] for r in rr]} for k,rr in same_names.items() if k[0] and len(rr)>1]
    posts=tables['silver_social_posts']; comments=tables['silver_social_comments']; contents=posts+comments
    for n,rr in (('posts',posts),('comments',comments)):
        s[n]={'platforms':count(rr,'platform'),'relevance_by_platform':grouped(rr,('platform','is_pickleball_related')),
              'commercial_by_platform':grouped(rr,('platform','is_commercial')),
              'intents_by_platform':[{'platform':key[0],'labels':dict(Counter(l for r in v for l in r['intent_labels']))} for key,v in groups(rr,('platform',)).items()],
              'label_combinations':dict(Counter('|'.join(sorted(r['intent_labels'])) or 'NO_LABEL' for r in rr)),
              'multilabel_content':sum(len(r['intent_labels'])>1 for r in rr),
              'topics':dict(Counter(l for r in rr for l in r['topic_labels'])), 'sentiment':count(rr,'sentiment'),
              'text_length':stats(len(r.get('text_normalized',r.get('comment_text_normalized',''))) for r in rr),
              'publication_months':count([{'month':r['published_date_vn'][:7] if r['published_date_vn'] else None} for r in rr],'month'),
              'published_days':len({r['published_date_vn'] for r in rr if r['published_date_vn']})}
    mentions=tables['silver_social_geo_mentions']; bycontent=groups(mentions,('content_id',))
    s['geography']={'status':count(mentions,'resolution_status'),'precision':count(mentions,'resolution_precision'),
         'mentioned_content':len(bycontent),'resolved_post_content':sum(any(m['resolution_status']=='RESOLVED' for m in bycontent.get((r['content_id'],),[])) for r in posts),
         'resolved_comment_content':sum(any(m['resolution_status']=='RESOLVED' for m in bycontent.get((r['content_id'],),[])) for r in comments),
         'old_provinces':count(mentions,'old_province_id'),'new_provinces':count(mentions,'new_province_id'),
         'roles':count(mentions,'location_role'),
         'ambiguous_as_written':sum(len(json.loads(r['interpretation_json'])['AS_WRITTEN'])>1 for r in mentions),
         'raw_terms':dict(Counter(r['location_text_raw'].casefold() for r in mentions)),
         'projection_rows':dict(Counter(ver for r in mentions for ver,_ in assignments([r])))}
    postlookup={r['content_id']:r for r in posts}; nativeposts={(r['platform'],r['source_record_id']):r for r in posts}
    s['comment_parent_geography']=[]
    for r in comments:
        parent=postlookup.get(r['post_id']) or nativeposts.get((r['platform'],r['post_id']))
        own=assignments(bycontent.get((r['content_id'],),[]))
        par=assignments(bycontent.get((parent['content_id'],),[])) if parent else []
        s['comment_parent_geography'].append({'is_reply':r['is_reply'],'parent_found':parent is not None,'own':own,'parent':par,'different':own!=par if parent else None})
    s['engagement']=snapshot_profile(tables['silver_social_engagement_snapshots'])
    s['intent_evidence']={'labels':count(tables['silver_social_intent_evidence'],'intent_type'),
        'extracted_participant_counts':stats(r['participant_count_mentioned'] for r in tables['silver_social_intent_evidence']),
        'resolved_location_evidence':sum(r['intent_location_id'] is not None for r in tables['silver_social_intent_evidence'])}
    aliases=tables['dim_location_alias']; aliasgroups=groups(aliases,('alias_text',))
    s['ambiguous_aliases']={'alias_texts':len(aliasgroups),'multi_unit_aliases':sum(len({r['admin_unit_id'] for r in rr})>1 for rr in aliasgroups.values())}
    g=result['gold']
    for n in GOLD:
        rr=tables[n]
        national=[r for r in rr if r.get('admin_version')=='NATIONAL' and r.get('admin_unit_id')=='NATIONAL']
        metricfields=[k for k in profiles[n]['numeric'] if k.endswith(('_count','_views','_likes','_shares','_reported'))]
        g[n]={'national':grouped(national,tuple(k for k in ('snapshot_date','platform','intent_type') if any(k in r for r in rr)),metricfields) if national else [],
              'numeric_totals_by_version':grouped(rr,('admin_version',),metricfields) if any('admin_version' in r for r in rr) else []}
    supply=tables['court_supply_by_admin_version_v2']; latest_day=max(r['snapshot_date'] for r in supply)
    admin={r['admin_unit_id']:r['admin_name'] for r in tables['dim_admin_unit']}
    g['supply_latest']=[dict(r,admin_name=admin.get(r['admin_unit_id'],r['admin_unit_id'])) for r in supply if r['snapshot_date']==latest_day]
    g['activity_months']=grouped([dict(r,month=r['activity_date'][:7]) for r in tables['social_activity_by_admin_version_daily'] if r['admin_version']=='NATIONAL'],('month',),('post_count','comment_count','reply_count','unique_content_count','classified_content_count'))
    g['intent_platforms']=grouped([r for r in tables['social_intent_daily_v2'] if r['admin_version']=='NATIONAL'],('platform','intent_type'),('post_count','comment_count','reply_count','unique_content_count'))
    g['coverage_status_platform']=grouped(tables['national_location_coverage_v1'],('source_platform','admin_version','collection_status'))
    g['community_totals']=grouped(tables['venue_community_feedback_weekly_v1'],(),('mentioning_post_count','mentioning_comment_count','classified_evidence_count','positive_feedback_count','negative_feedback_count','facility_complaint_count','booking_difficulty_count'))
    g['monthly_supply_status']=grouped(tables['location_market_research_monthly_v1'],('admin_version','supply_coverage_status'))
    # Current-snapshot exploratory province comparison. No historical supply backfill.
    activity=tables['social_activity_by_admin_version_daily']; intent=tables['social_intent_daily_v2']
    pairs=[]
    for r in supply:
        if r['snapshot_date']!=latest_day or r['admin_version']!=POST:
            continue
        social=[x for x in activity if x['admin_version']==POST and x['admin_unit_id']==r['admin_unit_id']]
        seeking=[x for x in intent if x['admin_version']==POST and x['admin_unit_id']==r['admin_unit_id'] and x['intent_type']=='SEEK_COURT']
        pairs.append({'province':admin[r['admin_unit_id']],'venue_count':r['venue_count'],'known_courts':r['known_court_count'],
                      'relevant_post_count':sum(x['relevant_post_count'] for x in social),
                      'seeking_content':sum(x['unique_content_count'] for x in seeking),
                      'published_day_rows':len(social),'social_has_observations':bool(social)})
    result['supply_social_comparison']={'pairs':pairs,'n_supply_provinces':len(pairs),
        'n_with_social':sum(r['social_has_observations'] for r in pairs),
        'correlation_status':'WITHHELD: purposive collection, missing province-day sampling denominators and historical supply; publication periods do not match supply snapshot. Raw paired counts are diagnostic only.'}
    # Independent checks of counts/partitions using Silver entities.
    checks=[]
    def check(label,actual,expected):
        checks.append({'check':label,'gold':actual,'silver_expected':expected,'pass':actual==expected})
    eligible=[r for r in venues if r['verification_status']!='UNVERIFIED' and r['venue_status']!='CLOSED']
    for r in supply:
        if r['snapshot_date']!=latest_day:
            continue
        vv=eligible if r['admin_version']=='NATIONAL' else [v for v in eligible if (r['admin_version'],r['admin_unit_id']) in assignments([v])]
        check('supply '+r['admin_version']+' '+r['admin_unit_id']+' venue_count',r['venue_count'],len(vv))
        known=[v for v in vv if v['court_count_status']=='KNOWN' and v['court_count'] is not None]
        check('supply '+r['admin_version']+' '+r['admin_unit_id']+' known_courts',r['known_court_count'],sum(v['court_count'] for v in known) if known else None)
        check('supply count partition '+r['admin_unit_id'],r['venue_count'],r['known_count_venue_count']+r['unknown_count_venue_count']+r['conflicting_count_venue_count'])
    content_by_day=groups([r for r in contents if r['published_date_vn'] and not r['is_duplicate']],('published_date_vn',))
    latestmetrics={}
    for r in tables['silver_social_engagement_snapshots']:
        if r['snapshot_status']=='AVAILABLE' and (r['content_id'] not in latestmetrics or r['observed_at']>latestmetrics[r['content_id']]['observed_at']):
            latestmetrics[r['content_id']]=r
    for r in tables['social_activity_by_admin_version_daily']:
        vv=content_by_day.get((r['activity_date'],),[])
        if r['admin_version']!='NATIONAL':
            vv=[v for v in vv if (r['admin_version'],r['admin_unit_id']) in assignments(bycontent.get((v['content_id'],),[]))]
        for col,predicate in [('unique_content_count',lambda v:True),('post_count',lambda v:'comment_id' not in v),
                              ('comment_count',lambda v:'comment_id' in v and not v['is_reply']),('reply_count',lambda v:'comment_id' in v and v['is_reply']),
                              ('classified_content_count',lambda v:v['is_pickleball_related'] is not None)]:
            check(f'activity {r["activity_date"]} {r["admin_unit_id"]} {col}',r[col],len({v['content_id'] for v in vv if predicate(v)}))
    for r in tables['social_engagement_daily_v1']:
        vv=[v for v in content_by_day.get((r['activity_date'],),[]) if 'comment_id' not in v and v['platform']==r['platform'] and v['is_pickleball_related'] is True]
        if r['admin_version']!='NATIONAL':
            vv=[v for v in vv if (r['admin_version'],r['admin_unit_id']) in assignments(bycontent.get((v['content_id'],),[]))]
        check(f'engagement {r["activity_date"]} {r["admin_unit_id"]} {r["platform"]} posts',r['post_count'],len(vv))
        for raw,aggregate in (('views','total_views'),('likes','total_likes'),('comments','total_comments_reported'),('shares','total_shares')):
            vals=[latestmetrics[v['content_id']][raw] for v in vv if v['content_id'] in latestmetrics and latestmetrics[v['content_id']][raw] is not None]
            check(f'engagement {r["activity_date"]} {r["admin_unit_id"]} {r["platform"]} {aggregate}',r[aggregate],sum(vals) if vals else None)
    for r in tables['social_intent_daily_v2']:
        vv=[v for v in content_by_day.get((r['activity_date'],),[]) if v['platform']==r['platform'] and v['is_pickleball_related'] is not False and r['intent_type'] in v['intent_labels']]
        if r['admin_version']!='NATIONAL':
            vv=[v for v in vv if (r['admin_version'],r['admin_unit_id']) in assignments(bycontent.get((v['content_id'],),[]))]
        check(f'intent {r["activity_date"]} {r["admin_unit_id"]} {r["intent_type"]}',r['unique_content_count'],len({v['content_id'] for v in vv}))
    # Price medians, feedback/coverage/monthly definitions also checked via a pure
    # production-formula recomputation; reported separately from independent checks.
    manifest=tables['intelligence_manifest'][0]
    manifest=json.loads(manifest['payload_json'])
    version_checks={n: inventory[n]['delta_version']==version for n,version in {**manifest['input_versions'],**manifest['output_versions']}.items()}
    result['consistency']['manifest_version_checks']=version_checks
    from dsp_spark.processing.intelligence.quality import table_digest
    result['consistency']['manifest_digests']={n:table_digest(tables[n])==expected['digest'] for n,expected in {**manifest['before'],**manifest['after']}.items()}
    result['manifest_as_of']=manifest['as_of']
    result['manifest_generated_at']=manifest['generated_at']
    result['consistency']['bronze_silver_entities']={}
    for bn,sn,splatform in [('bronze_social_posts','silver_social_posts','platform'),('bronze_social_comments','silver_social_comments','platform'),('bronze_court_observations','silver_venue_source_links','source_platform')]:
        expected={(r['source_platform'],r['source_record_id']) for r in tables[bn]}
        actual={(r[splatform],r['source_record_id']) for r in tables[sn] if bn!='bronze_court_observations' or r['source_platform']=='pbplus'}
        result['consistency']['bronze_silver_entities'][bn]={'bronze':len(expected),'silver':len(actual),'missing':len(expected-actual),'unexpected':len(actual-expected)}
    # Formula replay is supplementary: it is not independent validation.
    from dsp_spark.processing.intelligence.gold import build_gold
    from dsp_spark.processing.intelligence.geography import Geography
    accepted_by_run=defaultdict(list)
    for n in DATASETS[:3]:
        for r in tables[n]:
            accepted_by_run[(r['source_platform'],r['collection_run_id'])].append(r['observation_id'])
    attributed_runs=[dict(r,accepted_observation_ids=accepted_by_run[(r['source_platform'],r['collection_run_id'])]) for r in runs]
    replay=build_gold({n:tables[n] for n in SILVER},attributed_runs,Geography(),as_of=manifest['as_of'],generated_at=manifest['generated_at'])
    result['consistency']['production_formula_replay']={n:{'match':table_digest(tables[n])==table_digest(replay[n]),'stored_rows':len(tables[n]),'recomputed_rows':len(replay[n])} for n in GOLD}
    from dsp_spark.processing.clean.contracts import validate
    violations=[]
    for n in (*DATASETS,*SILVER,*GOLD,*LEGACY):
        for idx,r in enumerate(tables[n]):
            try:
                validate(r,n)
            except Exception as e:
                violations.append({'table':n,'row_index':idx,'error':type(e).__name__})
    result['consistency']['contract_validation']={'tables':32,'rows':sum(len(tables[n]) for n in (*DATASETS,*SILVER,*GOLD,*LEGACY)),'violations':violations}
    result['manifest_fields']=list(manifest)
    result['consistency']['independent_checks']=checks
    result['consistency']['independent_summary']={'checks':len(checks),'passed':sum(c['pass'] for c in checks),'failed':sum(not c['pass'] for c in checks)}
    from dsp_spark.processing.transform.location import resolve_alias, resolve_court_address
    legacy_checks=[]
    def legacy_check(label,actual,expected):
        legacy_checks.append({'check':label,'gold':actual,'silver_expected':expected,'pass':actual==expected})
    legacy_courts=[max(rr,key=lambda r:tuple(str(r.get(k) or '') for k in ('collected_at','geocoded_at','normalized_at','lakehouse_ingested_at','run_id'))) for rr in groups(tables['courts'],('record_id',)).values()]
    legacy_geo=defaultdict(list)
    for r in legacy_courts:
        loc,_,_=resolve_court_address(r['address'],r['address_source'])
        if loc:
            legacy_geo[loc].append(r)
    for r in tables['court_supply_by_location']:
        vv=legacy_geo[r['location_id']]; known=[v['number_of_courts'] for v in vv if v['number_of_courts'] is not None]
        for col,expected in [('venue_count',len(vv)),('known_court_count',sum(known) if known else None),('venues_with_known_court_count',len(known)),('venues_without_known_court_count',len(vv)-len(known))]:
            legacy_check('v1 supply '+r['location_id']+' '+col,r[col],expected)
    legacy_posts=[max(rr,key=lambda r:tuple(str(r.get(k) or '') for k in ('collected_at','emitted_at','lakehouse_ingested_at','event_id'))) for rr in groups(tables['social_posts'],('record_id',)).values()]
    legacy_social=defaultdict(dict)
    for r in legacy_posts:
        if r['source']!='threads' or r['is_reply'] or not r['published_at']:
            continue
        for text in set(r['detected_locations']):
            loc=resolve_alias(text)
            key=(r['published_at'][:10],loc[0] if loc else None)
            legacy_social[key][r['record_id']]=r
    for r in tables['social_activity_daily']:
        vv=list(legacy_social[(r['activity_date'],r['location_id'])].values())
        legacy_check('v1 activity '+r['activity_date']+' '+str(r['location_id'])+' posts',r['post_count'],len(vv))
        for col in ('like_count','reply_count','repost_count','quote_count'):
            legacy_check('v1 activity '+r['activity_date']+' '+str(r['location_id'])+' '+col,r[col],sum(v[col] or 0 for v in vv))
    legacy_supply={r['location_id']:r for r in tables['court_supply_by_location']}
    for r in tables['location_market_signal_snapshot']:
        vv=[v for v in tables['social_activity_daily'] if v['location_id']==r['location_id'] and r['window_start_date']<=v['activity_date']<=r['window_end_date']]
        for col in ('post_count','like_count','reply_count','repost_count','quote_count','engagement_total'):
            legacy_check('v1 market social '+r['location_id']+' '+col,r['social_'+col],sum(v[col] for v in vv))
        for col in ('venue_count','known_court_count','venues_with_known_court_count','venues_without_known_court_count'):
            legacy_check('v1 market supply '+r['location_id']+' '+col,r[col],legacy_supply[r['location_id']][col])
    result['consistency']['legacy_checks']=legacy_checks
    result['consistency']['legacy_summary']={'checks':len(legacy_checks),'passed':sum(c['pass'] for c in legacy_checks),'failed':sum(not c['pass'] for c in legacy_checks)}
    served={}
    for n in (*GOLD,*LEGACY):
        if n not in snap['clickhouse']:
            served[n]={'status':'MISSING_IN_CLICKHOUSE','delta_rows':len(tables[n])}
            continue
        props=contracts[n]['properties']
        delta=normalized_rows(tables[n],props); ch=normalized_rows(snap['clickhouse'][n],props)
        served[n]={'status':'MATCH' if digest(delta)==digest(ch) else 'MISMATCH',
                   'delta_rows':len(delta),'clickhouse_rows':len(ch),'delta_digest':digest(delta),'clickhouse_digest':digest(ch)}
    result['consistency']['clickhouse']=served
    result['consistency']['source_versions_unchanged']=all(r.get('unchanged_during_read',False) for r in inventory.values() if r['status']=='QUERIED_FULL')
    (OUT/'findings.json').write_text(json.dumps(result,indent=2,ensure_ascii=False))
    print(json.dumps({'datasets':len(profiles),'consistency':result['consistency']['independent_summary'],
                      'serving':{k:v['status'] for k,v in served.items()}},indent=2))

if __name__=='__main__':
    main()
