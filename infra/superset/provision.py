"""Idempotent public-API provisioning; preserves non-managed user assets."""
import copy
import json
import os
from pathlib import Path
from urllib.parse import quote
from uuid import uuid5, NAMESPACE_URL

import clickhouse_connect
from api import API
from definitions import GOLD, PREFIX, MARKER, DASHBOARDS, charts, datasets, sql_queries

RUNTIME=Path('/app/eda-runtime')
POST='VN_ADMIN_POST_2025'

def identity(kind,key): return str(uuid5(NAMESPACE_URL,'https://dsp391m.local/eda/v2/'+kind+'/'+key))

def metrics_for(columns,key,raw=False):
    result=[]
    for col in columns:
        name=col['column_name']; kind=(col.get('type') or '').upper()
        if not any(t in kind for t in ('INT','FLOAT','DECIMAL','DOUBLE','NUMERIC')): continue
        if name in ('schema_version','high_confidence_count','high_engagement_post_count','high_engagement_threshold','median_engagement','price_min_vnd','price_median_vnd','price_complaint_count','service_complaint_count'): continue
        expression=f'sumOrNull({name})'
        description='Nullable sum of available values; NULL stays unavailable. Keep the declared dataset grain/scope.'
        if name.endswith(('_rate','_share','_ratio')) or name in ('resolution_coverage','term_usage_share'):
            expression=f'max({name})'
            description='Stored ratio at the full native grain. MAX prevents summing ratios; do not use across different denominator groups.'
        if name=='relevance_coverage': expression='toFloat64(sum(relevance_classified_content))/nullIf(sum(collected_dated_content),0)'
        if name=='court_count_coverage_rate': expression='toFloat64(sum(known_count_venue_count))/nullIf(sum(venue_count),0)' if 'known_count_venue_count' in {c['column_name'] for c in columns} else 'max(court_count_coverage_rate)'
        if name=='geo_resolution_rate': expression='sumOrNull(resolved_records)/nullIf(sumIf(records_accepted,resolved_records IS NOT NULL AND records_accepted IS NOT NULL),0)'
        if name=='available_views_per_video': expression='sumOrNull(latest_observed_views)/nullIf(sum(videos_with_views),0)'
        if name.startswith('avg_'):
            if name=='avg_likes_per_post': expression='sumOrNull(total_likes)/nullIf(sum(posts_with_likes),0)'
            elif name=='avg_comments_per_post': expression='sumOrNull(total_comments_reported)/nullIf(sum(posts_with_comments),0)'
            else: continue
        if raw and key=='social_intent_daily_v2':
            description+=' Intent labels overlap; group/filter by intent_type before interpreting distinct-content counts.'
        if raw and any(c['column_name']=='admin_version' for c in columns):
            description+=' REQUIRED: one admin_version and scope. Never add NATIONAL to province rows.'
        result.append({'metric_name':name,'expression':expression,'verbose_name':name.replace('_',' ').title(),
                       'description':description,'d3format':'.1%' if name.endswith(('_rate','_share','_ratio','coverage')) else ',.0f'})
    if key=='03_collection_evidence_matrix':
        result.append({'metric_name':'evidence_groups','expression':'count()','verbose_name':'Source / province / day evidence groups','description':'Counts coverage groups, not distinct runs or market entities.','d3format':',.0f'})
    return result

def update_dataset(api,asset,key,time,description):
    full=api.call('GET',f'dataset/{asset["id"]}')['result']
    fields=[]
    for c in full['columns']:
        name=c['column_name']; hidden=name in ('lineage_ids','schema_version','processing_version','evidence_ids','collection_run_ids') or name.endswith('_json')
        fields.append({'id':c['id'],'column_name':name,'verbose_name':name.replace('_',' ').title(),
                       'is_dttm':c.get('is_dttm',False) or name==time,
                       'groupby':not hidden,'filterable':not hidden,
                       'description':'Lineage/internal provenance; not a default chart dimension.' if hidden else 'Actual ClickHouse field; NULL is unavailable, not zero.'})
    api.call('PUT',f'dataset/{asset["id"]}',{'description':MARKER+key+'\n'+description,
        'main_dttm_col':time,'filter_select_enabled':True,'columns':fields,
        'metrics':metrics_for(full['columns'],key,raw=key in GOLD),'cache_timeout':30})
    return api.call('GET',f'dataset/{asset["id"]}')['result']

def dataset_asset(api,dbid,key,sql=None,time=None,description=''):
    table_name=key if key in GOLD else 'eda_v2__'+key
    matches=[r for r in api.list('dataset',[{'col':'table_name','opr':'eq','value':table_name}]) if r['database']['id']==dbid]
    if len(matches)>1: raise RuntimeError('Duplicate managed dataset '+key)
    if matches:
        asset=matches[0]
        if sql:
            api.call('PUT',f'dataset/{asset["id"]}',{'sql':sql})
    else:
        body={'database':dbid,'schema':os.environ['CLICKHOUSE_DATABASE'] if key in GOLD else None,
              'table_name':table_name,'uuid':identity('dataset',key)}
        if sql: body['sql']=sql
        asset=api.call('POST','dataset/',body)
    return update_dataset(api,asset,key,time,description)

def chart_payload(definition,ds):
    dims=definition['dimensions']; ms=definition['metrics']; viz=definition['viz']; t=ds.get('main_dttm_col')
    available={m['metric_name'] for m in ds['metrics']}
    if not set(ms)<=available: raise RuntimeError(f'Missing metrics for {definition["key"]}: {set(ms)-available}')
    filters=[{'col':col,'op':'IN' if op=='==' else op,'val':[val] if op=='==' else val} for col,op,val in definition['filters']]
    adhoc=[{'expressionType':'SIMPLE','clause':'WHERE','subject':r['col'],'operator':r['op'],'comparator':r['val'],'sqlExpression':None,'isExtra':False,'isNew':False,'filterOptionName':'filter_'+str(i)} for i,r in enumerate(filters)]
    params={'datasource':str(ds['id'])+'__table','viz_type':viz,'metrics':ms,'groupby':dims,
            'time_range':'No filter','granularity_sqla':t,'adhoc_filters':adhoc,'row_limit':10000,
            'color_scheme':'supersetColors','show_legend':True,'rich_tooltip':True,
            'legendOrientation':'top','legendType':'scroll','show_totals':False,
            'page_length':15,'include_search':True,'query_mode':'aggregate' if ms else 'raw',
            'all_columns':dims if not ms else [],'table_timestamp_format':'%Y-%m-%d',
            'number_format':',.0f','y_axis_format':'.1%' if any(m.endswith(('_coverage','_rate','_share')) for m in ms) else ',.0f',
            'label_colors':{},'emit_filter':True}
    query={'columns':dims,'metrics':ms,'filters':filters,'extras':{'having':'','where':''},
           'time_range':'No filter','granularity':t,'row_limit':10000,'order_desc':True,
           'orderby':[[ms[0],False]] if ms and not definition['time'] else [[dims[0],True]],
           'post_processing':[]}
    if viz.startswith('echarts_timeseries'):
        params.update({'x_axis':dims[0],'groupby':dims[1:],'time_grain_sqla':'P1D' if definition['time'] else None,
            'orientation':'vertical' if definition['time'] else 'horizontal','show_value':False,
            'x_axis_title':dims[0].replace('_',' ').title(),'y_axis_title':' / '.join(m.replace('_',' ') for m in ms),
            'stack':True if definition['key']=='supply_completeness' else False,
            'contributionMode':None,'truncateYAxis':True,'showTooltipTotal':False})
        if dims[1:]:
            query['post_processing']=[{'operation':'pivot','options':{'index':[dims[0]],'columns':dims[1:],'aggregates':{m:{'operator':'mean'} for m in ms},'drop_missing_columns':True}},{'operation':'flatten'}]
    if viz=='pie': params.update({'donut':True,'show_labels':True,'label_type':'key_value','outerRadius':75})
    context={'datasource':{'id':ds['id'],'type':'table'},'force':False,'queries':[query],
             'form_data':params,'result_format':'json','result_type':'full'}
    return params,context

def filter_config(dashboard_keys,definitions,assets,dsassets):
    scoped=[d for d in definitions if d['key'] in dashboard_keys]
    result=[]
    for column,label in [('province','Province / canonical target'),('platform','Platform'),('source_platform','Collection source'),('intent_type','Intent label'),('collection_status','Collection evidence status'),('term_admin_version','Raw place-name administrative version')]:
        capable=[d for d in scoped if column in {c['column_name'] for c in dsassets[d['dataset']]['columns']}]
        if column=='province': capable=[d for d in capable if d['key']!='geo_unresolved']
        if not capable: continue
        allowed=[assets[d['key']]['id'] for d in capable]
        result.append({'id':'NATIVE_FILTER-'+identity('filter',','.join(dashboard_keys)+column),
            'name':label,'filterType':'filter_select','type':'NATIVE_FILTER',
            'targets':[{'datasetId':dsassets[capable[0]['dataset']]['id'],'column':{'name':column}}],
            'controlValues':{'enableEmptyFilter':True,'multiSelect':True,'searchAllOptions':False,'inverseSelection':False,'defaultToFirstItem':False},
            'defaultDataMask':{'extraFormData':{},'filterState':{'value':None}},'cascadeParentIds':[],
            'scope':{'rootPath':['ROOT_ID'],'excluded':[assets[d['key']]['id'] for d in scoped if d not in capable]},
            'chartsInScope':allowed,'tabsInScope':[]})
    # Separate time meanings: never apply collection dates to publication charts.
    for kind,label,capable in [
        ('collection','Collection-day range',[d for d in scoped if dsassets[d['dataset']].get('main_dttm_col')=='period_start']),
        ('publication','Publication / first-observed period',[d for d in scoped if dsassets[d['dataset']].get('main_dttm_col') in ('activity_date','month','publication_month')])]:
        if not capable: continue
        result.append({'id':'NATIVE_FILTER-'+identity('filter',','.join(dashboard_keys)+kind),'name':label,
            'filterType':'filter_time','type':'NATIVE_FILTER','targets':[{}],'controlValues':{},
            'defaultDataMask':{'extraFormData':{'time_range':'No filter'},'filterState':{'value':'No filter','label':'All collected periods'}},
            'cascadeParentIds':[],'scope':{'rootPath':['ROOT_ID'],'excluded':[assets[d['key']]['id'] for d in scoped if d not in capable]},
            'chartsInScope':[assets[d['key']]['id'] for d in capable],'tabsInScope':[]})
    return result

def layout(title,warning,keys,assets,definitions):
    result={'DASHBOARD_VERSION_KEY':'v2','ROOT_ID':{'id':'ROOT_ID','type':'ROOT','children':['GRID_ID']},
            'GRID_ID':{'id':'GRID_ID','type':'GRID','children':['INTRO_ROW'],'parents':['ROOT_ID']},
            'HEADER_ID':{'id':'HEADER_ID','type':'HEADER','meta':{'text':title}},
            'INTRO_ROW':{'id':'INTRO_ROW','type':'ROW','children':['INTRO_MARKDOWN'],'parents':['ROOT_ID','GRID_ID'],'meta':{'background':'BACKGROUND_TRANSPARENT'}},
            'INTRO_MARKDOWN':{'id':'INTRO_MARKDOWN','type':'MARKDOWN','children':[],'parents':['ROOT_ID','GRID_ID','INTRO_ROW'],'meta':{'width':12,'height':38,'code':'### '+title+'\n**Observed sample · live ClickHouse Gold data**\n\n'+warning+'\n\n[Bronze source profile](/eda-reference/bronze_eda.md) · [Silver intelligence](/eda-reference/silver_eda.md) · [Gold definitions and findings](/eda-reference/gold_eda.md) · [Cross-layer limits](/eda-reference/cross_layer_findings.md)\n\nUse native filters; open a chart menu → Explore to adjust dimensions and metrics. Province scopes are fixed or explicitly filtered to one administrative version.','background':'BACKGROUND_WHITE'}}}
    defs={d['key']:d for d in definitions}
    for i in range(0,len(keys),2):
        row='ROW_'+str(i); selected=keys[i:i+2]
        result['GRID_ID']['children'].append(row)
        result[row]={'id':row,'type':'ROW','parents':['ROOT_ID','GRID_ID'],'children':[],'meta':{'background':'BACKGROUND_TRANSPARENT'}}
        for key in selected:
            cid='CHART_'+key; asset=assets[key]; d=defs[key]
            result[row]['children'].append(cid)
            result[cid]={'id':cid,'type':'CHART','children':[],'parents':['ROOT_ID','GRID_ID',row],
                         'meta':{'chartId':asset['id'],'uuid':asset['uuid'],'sliceName':d['title'],'width':6 if len(selected)==2 else 12,'height':65 if d['viz']=='table' else 52}}
    return result

def main():
    api=API(); db=os.environ['CLICKHOUSE_DATABASE']
    reader=clickhouse_connect.get_client(host='clickhouse',port=8123,username=os.environ['SUPERSET_CLICKHOUSE_USER'],password=os.environ['SUPERSET_CLICKHOUSE_PASSWORD'],database=db)
    identity_row=reader.query("SELECT currentUser(), getSetting('readonly')").result_rows[0]
    if identity_row[1]!=1: raise RuntimeError('Dedicated ClickHouse account must be strictly read-only')
    sqlproof={k:{'rows':reader.query(sql).row_count} for k,sql in sql_queries(db).items()}
    print('In-container read-only SELECT and 14 verified SQL queries passed.',flush=True)
    name=PREFIX+'Gold (read-only)'
    uri='clickhousedb://'+quote(os.environ['SUPERSET_CLICKHOUSE_USER'],safe='')+':'+quote(os.environ['SUPERSET_CLICKHOUSE_PASSWORD'],safe='')+'@clickhouse:8123/'+db
    payload={'database_name':name,'sqlalchemy_uri':uri,'expose_in_sqllab':True,'allow_run_async':False,'allow_dml':False,'allow_ctas':False,'allow_cvas':False,'allow_file_upload':False,'extra':json.dumps({'engine_params':{},'metadata_params':{}})}
    api.call('POST','database/test_connection',{'sqlalchemy_uri':uri,'database_name':name})
    existing=api.find('database','database_name',name)
    if existing:
        api.call('PUT',f'database/{existing["id"]}',payload); database=existing
    else:
        database=api.call('POST','database/',dict(payload,uuid=identity('database','gold')))
    dbid=database['id']; assets={}
    for table,(time,description) in GOLD.items():
        assets[table]=dataset_asset(api,dbid,table,time=time,description=description)
        print('Registered physical dataset '+table,flush=True)
    for key,definition in datasets(db).items():
        assets[key]=dataset_asset(api,dbid,key,definition['sql'],definition['time'],definition['description'])
        print('Registered virtual dataset '+key,flush=True)
    definitions=charts(); chartassets={}
    for d in definitions:
        params,context=chart_payload(d,assets[d['dataset']])
        title=PREFIX+d['title']; old=api.find('chart','slice_name',title)
        body={'slice_name':title,'description':MARKER+d['key']+'\n'+d['status']+' · '+d['note'],
              'viz_type':d['viz'],'datasource_id':assets[d['dataset']]['id'],'datasource_type':'table',
              'params':json.dumps(params),'query_context':json.dumps(context),'cache_timeout':30}
        if old:
            detail=api.call('GET',f'chart/{old["id"]}')['result']
            if not (detail.get('description') or '').startswith(MARKER): raise RuntimeError('Refusing to overwrite user chart '+title)
            api.call('PUT',f'chart/{old["id"]}',body); asset=old
        else:
            asset=api.call('POST','chart/',dict(body,uuid=identity('chart',d['key'])))
        detail=api.call('GET',f'chart/{asset["id"]}')['result']
        chartassets[d['key']]={'id':asset['id'],'uuid':detail['uuid'],'dataset':d['dataset'],'params':params,'query_context':context,'title':d['title']}
        print('Saved chart '+d['key'],flush=True)
    dashboards={}
    for slug,title,keys,warning in DASHBOARDS:
        fullname=PREFIX+title; native=filter_config(keys,definitions,chartassets,assets)
        metadata={'native_filter_configuration':native,'cross_filters_enabled':True,'color_scheme':'supersetColors','refresh_frequency':0,'default_filters':'{}','label_colors':{},'chart_configuration':{str(chartassets[k]['id']):{'id':chartassets[k]['id'],'crossFilters':{'scope':'global','chartsInScope':[chartassets[j]['id'] for j in keys if j!=k]}} for k in keys}}
        payload={'dashboard_title':fullname,'slug':'dsp391m-eda-'+slug,'published':True,'position_json':json.dumps(layout(title,warning,keys,chartassets,definitions)),'json_metadata':json.dumps(metadata),'css':'.dashboard-component-chart-holder{border-radius:8px}.dashboard-markdown{line-height:1.55}'}
        old=api.find('dashboard','dashboard_title',fullname)
        if old:
            api.call('PUT',f'dashboard/{old["id"]}',payload); asset=old
        else:
            asset=api.call('POST','dashboard/',dict(payload,uuid=identity('dashboard',slug)))
        # Dashboard chart membership follows layout chart IDs on supported API.
        dashboards[slug]={'id':asset['id'],'title':title,'url':'http://localhost:8088/superset/dashboard/dsp391m-eda-'+slug+'/',
                          'charts':[chartassets[k]['id'] for k in keys],'filters':native}
        print('Saved dashboard '+title,flush=True)
    manifest={'database_id':dbid,'reader':identity_row[0],'physical_datasets':{k:v['id'] for k,v in assets.items() if k in GOLD},
              'virtual_datasets':{k:v['id'] for k,v in assets.items() if k not in GOLD},'charts':chartassets,'dashboards':dashboards,'verified_eda_sql':sqlproof}
    RUNTIME.mkdir(exist_ok=True); (RUNTIME/'provisioned.json').write_text(json.dumps(manifest,indent=2))
    print(json.dumps({'physical_datasets':len(manifest['physical_datasets']),'virtual_datasets':len(manifest['virtual_datasets']),'charts':len(chartassets),'dashboards':len(dashboards)}))

if __name__=='__main__': main()
