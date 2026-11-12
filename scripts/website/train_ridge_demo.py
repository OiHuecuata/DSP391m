import os,json,hashlib,zipfile,argparse,sys
from pathlib import Path
os.environ.setdefault('PYSPARK_PYTHON',sys.executable)
from dsp_spark.runtime.config import LakehousePaths,SparkSettings
from dsp_spark.runtime.spark import create_spark
from dsp_spark.ml.registry import Registry
from dsp_spark.ml.provincial.config import ForecastConfig
from dsp_spark.ml.provincial.pipeline import run_benchmark,forecast
from dsp_spark.ml.provincial.data import read_sources,weekly_counts,make_panel
from pyspark.sql import functions as F
parser=argparse.ArgumentParser(description='Train and export a clearly labelled mixed/simulated V3 Ridge demo, using the repository pipeline. Writes local artifacts only.')
parser.add_argument('--dataset',type=Path,required=True)
parser.add_argument('--workdir',type=Path,required=True)
parser.add_argument('--run-id',default='v3-demo-20261112')
args=parser.parse_args()
root=args.workdir.resolve();root.mkdir(parents=True,exist_ok=True)
z=zipfile.ZipFile(args.dataset);m=json.loads(z.read('manifest.json'))
assert m.get('simulation_disclosure',{}).get('contains_simulated_content') is True, 'This exporter is only for the mixed V3 demo dataset'
(root/'source_manifest.json').write_text(json.dumps(m,ensure_ascii=False,indent=2))
spark=create_spark(SparkSettings(LakehousePaths((root/'lakehouse').as_uri()),master='local[2]',social_post_topic='demo-unused'))
try:
 inputs={'dataset_id':m['dataset_id'],'provenance':'mixed','generation':'v3-demo','tables':{}}
 for name in ['social_posts','social_geo_mentions','social_intent_evidence','dim_admin_unit']:
  t=m['tables'][name];payload=z.read(t['file']);assert hashlib.sha256(payload).hexdigest()==t['sha256']
  p=root/(name+'.parquet');p.write_bytes(payload)
  target=root/'silver'/name
  if not target.exists():spark.read.parquet(str(p)).write.format('delta').save(target.as_uri())
  inputs['tables'][('silver_'+name) if name!='dim_admin_unit' else name]={'path':target.as_uri(),'version':0}
 (root/'inputs.json').write_text(json.dumps(inputs,indent=2))
 config=ForecastConfig(as_of_date='2026-11-12',observed_through='2026-10-28')
 store=Registry((root/'staging').as_uri());run_id=args.run_id
 report=run_benchmark(spark,inputs,config,store,final_refit=True,run_id=run_id,execute=True)
 (root/'training.json').write_text(json.dumps(report,indent=2,default=str))
 print('TRAINING_STATUS',report['status'],flush=True)
 if report['status']!='TRAINED':raise RuntimeError(report['reason'])
 output,pred=forecast(spark,inputs,store,run_id)
 predictions=[r.asDict() for r in output.orderBy('province_id').collect()]
 (root/'predictions.json').write_text(json.dumps({'report':pred,'forecasts':predictions},indent=2,default=str))
 base='provincial_ridge/runs/'+run_id+'/tables/'
 tests=spark.read.format('delta').load(store.uri(base+'ridge_predictions_by_province_week'))
 table_names=[p.name for p in (root/'staging'/base).iterdir()];print('TABLES',table_names,flush=True)
 test_rows=[r.asDict() for r in tests.select('province_id','forecast_week','label','prediction').orderBy('province_id','forecast_week').collect()]
 summary=report['summary']
 demo={'status':'TRAINED','provenance':'mixed','evaluationStatus':'provisional-solver-warning','datasetId':m['dataset_id'],'runId':run_id,'generatedAt':predictions[0]['generated_at'].isoformat(),'originalPosts':m['simulation_disclosure']['original_posts_retained'],'simulatedPosts':m['simulation_disclosure']['simulated_posts_rewritten'],'trainingCutoff':report['training_cutoff'],'forecastWeek':pred['forecast_week'],'metrics':{'mae':summary['mae'],'rmse':summary['rmse'],'baselineMae':summary['baseline_mae'],'baselineRmse':summary['baseline_rmse']},'provinces':[]}
 for p in predictions:
  pid=p['province_id']
  demo['provinces'].append({'id':pid,'name':p['province_name'],'prediction':p['predicted_geo_linked_intent_posts'],'test':[{'week':str(t['forecast_week']),'actual':t['label'],'prediction':t['prediction']} for t in test_rows if t['province_id']==pid]})
 (root/'ridge_v3_demo.json').write_text(json.dumps(demo,ensure_ascii=False,indent=2,allow_nan=False))
 print('FORECASTS',json.dumps(predictions,default=str),flush=True)
finally:spark.stop()
