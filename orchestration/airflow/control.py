"""Small local Airflow API client; structured inputs never become shell code."""

import json
import os
import subprocess
import urllib.error
import urllib.request
from datetime import UTC, datetime

from runtime import ROOT

DAG_ID = "dsp391m_market_signal_refresh"


def compose(*arguments):
    result = subprocess.run(
        ["docker", "compose", "--profile", "airflow", *arguments],
        cwd=ROOT,
        env={
            **os.environ,
            "DSP_AIRFLOW_REPO_ROOT": str(ROOT),
            "AIRFLOW_UID": str(os.getuid()),
        },
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout


class AirflowAPI:
    def __init__(self):
        self.config = json.loads(compose("config", "--format", "json"))
        address = compose("port", "airflow-api-server", "8080").strip()
        self.base = "http://" + address
        environment = self.config["services"]["airflow-scheduler"]["environment"]
        username = environment["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS"].split(":")[0]
        passwords = json.loads(
            (ROOT / ".local/airflow/config/passwords.json").read_text()
        )
        self.token = None
        result = self.request(
            "POST",
            "/auth/token",
            {"username": username, "password": passwords[username]},
        )
        self.token = result["access_token"]

    def request(self, method, path, payload=None):
        headers = {"Content-Type": "application/json"}
        if self.token is not None:
            headers["Authorization"] = "Bearer " + self.token
        data = json.dumps(payload).encode() if payload is not None else None
        request = urllib.request.Request(
            self.base + path, data=data, headers=headers, method=method
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(request, timeout=15) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            raise RuntimeError(
                f"Airflow {method} {path}: HTTP {error.code}: {error.read().decode()[:1500]}"
            ) from error

    def trigger(self, conf, run_id=None):
        run_id = run_id or "manual__" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")
        self.request("PATCH", f"/api/v2/dags/{DAG_ID}", {"is_paused": False})
        return self.request(
            "POST",
            f"/api/v2/dags/{DAG_ID}/dagRuns",
            {
                "dag_run_id": run_id,
                "logical_date": None,
                "conf": conf,
            },
        )


def main():
    conf = json.loads(os.getenv("DSP_AIRFLOW_TRIGGER_CONF", "{}"))
    if not isinstance(conf, dict):
        raise TypeError("AIRFLOW_CONF must be a JSON object")
    response = AirflowAPI().trigger(
        conf, os.getenv("DSP_AIRFLOW_TRIGGER_RUN_ID") or None
    )
    print(
        json.dumps(
            {
                "dag_id": DAG_ID,
                "run_id": response["dag_run_id"],
                "state": response["state"],
            }
        )
    )


if __name__ == "__main__":
    main()
