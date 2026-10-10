"""Processing-only admitted Threads vertical. Go exclusively acquires sources."""

import os
from datetime import UTC, datetime, timedelta

from airflow.sdk import DAG, task

DAG_ID = "dsp391m_threads_research_refresh"


def build_dag():
    with DAG(
        DAG_ID,
        schedule=os.environ.get("DSP_THREADS_AIRFLOW_SCHEDULE") or None,
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        catchup=False,
        max_active_runs=1,
        max_active_tasks=1,
        dagrun_timeout=timedelta(hours=1),
        default_args={"retries": 2, "retry_delay": timedelta(seconds=15)},
        tags=["dsp391m", "threads", "research"],
        doc_md="Admitted Kafka references → Spark Silver/Gold → inactive partial ClickHouse generation. No source acquisition.",
    ) as dag:

        @task(
            task_id="process_admitted_threads", execution_timeout=timedelta(minutes=45)
        )
        def process_admitted():
            import json
            import subprocess
            from pathlib import Path

            from airflow.sdk import get_current_context

            conf = get_current_context()["dag_run"].conf
            conf = conf or {}
            if conf and set(conf) != {"config_path", "manifest_uri"}:
                raise ValueError(
                    "Supply only a Threads config_path and admitted manifest_uri"
                )
            root = Path(os.environ["DSP_AIRFLOW_REPO_ROOT"]).resolve()
            config = Path(
                conf.get("config_path")
                or os.environ["DSP_THREADS_PROCESSING_CONFIG_PATH"]
            ).resolve()
            if not config.is_relative_to(root) or not config.is_file():
                raise ValueError("Config must be inside the mounted project")
            # All Spark stages execute in one process and one resource ownership
            # context. Retries reconcile immutable manifests, offsets and writes.
            python = os.environ.get(
                "DSP_THREADS_PROCESSING_PYTHON", "/opt/dsp-python/bin/python"
            )
            value = json.loads(config.read_text())
            if value.get("processing_mode") == "databricks":
                if not conf:
                    raise ValueError(
                        "Managed submit requires an explicit admitted manifest URI"
                    )
                command = [
                    python,
                    str(root / "infra/databricks/submit_threads.py"),
                    "--control-base-uri",
                    value["control_base_uri"],
                    "--config-uri",
                    value["databricks_config_uri"],
                    "--manifest-uri",
                    conf["manifest_uri"],
                    "--attempt-key",
                    str(get_current_context()["ti"].try_number),
                ]
            else:
                command = [
                    python,
                    "-m",
                    "orchestration.local.threads_vertical",
                    "process" if conf else "pending",
                    "--config",
                    str(config),
                ]
                if conf:
                    command += ["--manifest-uri", conf["manifest_uri"]]
            subprocess.run(command, cwd=root, check=True, timeout=2800)

        process_admitted()
    return dag


dag = build_dag()
