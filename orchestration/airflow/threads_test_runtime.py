"""Run the real Threads DAG with isolated metadata and declared fixture inputs."""

import json
import os
import subprocess
from pathlib import Path


def main():
    conf = json.loads(Path(os.environ["DSP_THREADS_FIXTURE_CONF"]).read_text())
    config = json.loads(Path(conf["config_path"]).read_text())
    root = Path(os.environ["DSP_AIRFLOW_REPO_ROOT"]) / ".local/threads-vertical"
    if config.get("mode") != "fixture" or not Path(conf["config_path"]).is_relative_to(
        root
    ):
        raise ValueError(
            "This integration profile accepts only isolated synthetic fixtures"
        )
    subprocess.run(["airflow", "db", "migrate"], check=True)
    subprocess.run(
        [
            "airflow",
            "dags",
            "test",
            "dsp391m_threads_research_refresh",
            "2026-10-10T00:00:00+00:00",
            "--conf",
            json.dumps(conf),
        ],
        check=True,
    )


if __name__ == "__main__":
    main()
