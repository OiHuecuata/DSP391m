FROM apache/superset:6.0.0
USER root
# Superset 6 uses SQLAlchemy 1.4; this driver is pinned and verified at runtime.
RUN uv pip install --python /app/.venv/bin/python \
    clickhouse-connect==0.8.18 psycopg2-binary==2.9.10
USER superset
