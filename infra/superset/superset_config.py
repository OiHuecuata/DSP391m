"""Local dedicated EDA settings; credentials come from ignored local environment."""
import os
from urllib.parse import quote_plus
from cachelib.redis import RedisCache

SECRET_KEY = os.environ['SUPERSET_SECRET_KEY']
if len(SECRET_KEY) < 40:
    raise RuntimeError('SUPERSET_SECRET_KEY must be strong and persistent')
SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://superset:' + quote_plus(os.environ['SUPERSET_METADATA_PASSWORD']) + '@superset-db:5432/superset'
FEATURE_FLAGS = {'DASHBOARD_NATIVE_FILTERS': True, 'DASHBOARD_CROSS_FILTERS': True, 'ENABLE_TEMPLATE_PROCESSING': False}
WTF_CSRF_ENABLED = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = 'Lax'
SESSION_COOKIE_SECURE = False  # loopback HTTP; change to True behind HTTPS
ENABLE_PROXY_FIX = False
LOAD_EXAMPLES = False
SUPERSET_WEBSERVER_TIMEOUT = 120
SQLLAB_TIMEOUT = 90
SQLLAB_ASYNC_TIME_LIMIT_SEC = 90
SQL_MAX_ROW = 10000
ROW_LIMIT = 10000
RESULTS_BACKEND = RedisCache(host='superset-cache', port=6379, db=1, key_prefix='dsp_sql_results_')
CACHE_CONFIG = {'CACHE_TYPE':'RedisCache','CACHE_DEFAULT_TIMEOUT':60,'CACHE_KEY_PREFIX':'dsp_eda_','CACHE_REDIS_HOST':'superset-cache','CACHE_REDIS_PORT':6379,'CACHE_REDIS_DB':0}
DATA_CACHE_CONFIG = dict(CACHE_CONFIG, CACHE_KEY_PREFIX='dsp_eda_data_', CACHE_DEFAULT_TIMEOUT=30)
APP_NAME = 'DSP391m EDA'
TALISMAN_ENABLED = False  # local HTTP deployment only

previous = os.getenv('SUPERSET_PREVIOUS_SECRET_KEY')
if previous:
    PREVIOUS_SECRET_KEY = previous

def reference_routes(app):
    """Authenticated analytical references; no raw Bronze/Silver records."""
    from pathlib import Path
    from flask import abort, Response
    from flask_login import login_required
    import markdown
    allowed={'README.md','bronze_eda.md','silver_eda.md','gold_eda.md','cross_layer_findings.md','superset_dashboard_spec.md','SUPERSET_RUNBOOK.md'}
    @login_required
    def reference(name):
        if name not in allowed:
            abort(404)
        file=Path('/app/eda')/name
        if not file.exists(): abort(404)
        body=markdown.markdown(file.read_text(),extensions=['tables','fenced_code'])
        return Response('<html><head><meta charset="utf-8"><style>body{max-width:1150px;margin:32px auto;font:15px system-ui;line-height:1.55;padding:20px}table{border-collapse:collapse;width:100%;display:block;overflow:auto}td,th{border:1px solid #ddd;padding:8px}pre{overflow:auto;background:#f4f6f8;padding:12px}</style></head><body><p>Captured EDA reference; dashboard charts query current ClickHouse data.</p>'+body+'</body></html>',mimetype='text/html')
    app.add_url_rule('/eda-reference/<name>','eda_reference',reference)

FLASK_APP_MUTATOR = reference_routes
