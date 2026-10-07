"""Orchestrate existing finite DSP391m jobs; streaming runs independently."""

import os
import shlex
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

DAG_ID = "dsp391m_market_signal_refresh"
TASK_IDS = (
    "preflight",
    "collect_pbplus",
    "ingest_pbplus_bronze",
    "ingest_pbplus_delta",
    "build_court_supply_gold",
    "check_social_delta",
    "build_social_activity_gold",
    "build_market_signal_snapshot",
    "ensure_clickhouse_ready",
    "migrate_clickhouse",
    "load_market_signals_clickhouse",
    "validate",
)


def build_dag(schedule=None):
    root = Path(os.environ["DSP_AIRFLOW_REPO_ROOT"])
    command = shlex.join(
        ["/opt/dsp-python/bin/python", str(root / "orchestration/airflow/run_job.py")]
    )
    with DAG(
        DAG_ID,
        schedule=schedule,
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        catchup=False,
        max_active_runs=1,
        max_active_tasks=1,
        dagrun_timeout=timedelta(hours=1),
        default_args={"retries": 0, "execution_timeout": timedelta(minutes=20)},
        tags=["dsp391m", "batch", "local"],
        doc_md="Refresh existing Gold and serving. PBPlus is source-specific batch; social locations are text mentions. Kafka/continuous Structured Streaming are independent.",
    ) as dag:
        tasks = {
            task_id: BashOperator(
                task_id=task_id,
                bash_command=f"{command} {task_id}",
                cwd=str(root),
                env={
                    "DSP_AIRFLOW_RUN_ID": "{{ run_id }}",
                    "DSP_AIRFLOW_RUN_CONF": "{{ dag_run.conf | tojson }}",
                },
                append_env=True,
                do_xcom_push=False,
                skip_on_exit_code=None,
                retries=2 if task_id == "ensure_clickhouse_ready" else 0,
                retry_delay=timedelta(seconds=15),
            )
            for task_id in TASK_IDS
        }
        (
            tasks["preflight"]
            >> tasks["collect_pbplus"]
            >> tasks["ingest_pbplus_bronze"]
            >> tasks["ingest_pbplus_delta"]
            >> tasks["build_court_supply_gold"]
        )
        (
            tasks["preflight"]
            >> tasks["check_social_delta"]
            >> tasks["build_social_activity_gold"]
        )
        [
            tasks["build_court_supply_gold"],
            tasks["build_social_activity_gold"],
        ] >> tasks["build_market_signal_snapshot"]
        (
            tasks["build_market_signal_snapshot"]
            >> tasks["ensure_clickhouse_ready"]
            >> tasks["migrate_clickhouse"]
            >> tasks["load_market_signals_clickhouse"]
            >> tasks["validate"]
        )
    return dag


dag = build_dag(os.getenv("DSP_AIRFLOW_SCHEDULE") or None)
