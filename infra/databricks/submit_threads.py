"""One admitted request on configured compute, with idempotent submit and terminal polling.

Interfaces: https://docs.databricks.com/api/jobs/v2/submit-run and /get-run.
No clusters, buckets, credentials or publications are provisioned here.
"""

import argparse
import os
import time
from urllib.parse import urlsplit

import httpx
from dsp_collector.normalizers.observation import canonical_json, digest


def render(
    *, cluster_id, wheel_uri, control_uri, config_uri, manifest_uri, attempt_key
):
    if (
        not cluster_id
        or not wheel_uri.endswith(".whl")
        or not wheel_uri.startswith(("/Workspace/", "/Volumes/", "s3://"))
    ):
        raise ValueError("Configure an existing cluster and explicit managed wheel")
    if not all(
        uri.startswith("s3a://") and not urlsplit(uri).query
        for uri in (control_uri, config_uri, manifest_uri)
    ):
        raise ValueError("Managed control inputs must be explicit S3A references")
    return {
        "run_name": "DSP391m admitted Threads research",
        "idempotency_token": digest([manifest_uri, cluster_id, wheel_uri, attempt_key]),
        "timeout_seconds": 2700,
        "tasks": [
            {
                "task_key": "threads_vertical",
                "existing_cluster_id": cluster_id,
                "python_wheel_task": {
                    "package_name": "dsp391_spark_jobs",
                    "entry_point": "threads_vertical",
                    "parameters": [
                        "--control-base-uri",
                        control_uri,
                        "--config-uri",
                        config_uri,
                        "--manifest-uri",
                        manifest_uri,
                    ],
                },
                "libraries": [{"whl": wheel_uri}],
                "max_retries": 0,
                "timeout_seconds": 2400,
            }
        ],
    }


def submit_and_wait(client, body, *, timeout_seconds=2800, sleeper=time.sleep):
    response = client.post("/api/2.2/jobs/runs/submit", json=body)
    response.raise_for_status()
    run_id = response.json()["run_id"]
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        response = client.get("/api/2.2/jobs/runs/get", params={"run_id": run_id})
        response.raise_for_status()
        value = response.json()
        state = value["state"]
        if (
            state.get("life_cycle_state") == "TERMINATED"
            and state.get("result_state") == "SUCCESS"
        ):
            return {
                "run_id": run_id,
                "result_state": "SUCCESS",
                "run_page_url": value.get("run_page_url"),
            }
        if state.get("life_cycle_state") in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            raise RuntimeError("Managed Threads job ended without terminal success")
        sleeper(10)
    # Leave the remote run identifiable; do not assume a timed-out client stopped
    # Spark or release its cloud writer fence on behalf of another process.
    raise TimeoutError(
        f"Managed run {run_id} is still uncertain; reconcile before retry"
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("control-base-uri", "config-uri", "manifest-uri", "attempt-key"):
        parser.add_argument("--" + flag, required=True)
    parser.add_argument("--render-only", action="store_true")
    args = parser.parse_args(argv)
    body = render(
        cluster_id=os.environ.get("DATABRICKS_CLUSTER_ID", ""),
        wheel_uri=os.environ.get("DSP_DATABRICKS_WHEEL_URI", ""),
        control_uri=args.control_base_uri,
        config_uri=args.config_uri,
        manifest_uri=args.manifest_uri,
        attempt_key=args.attempt_key,
    )
    if args.render_only:
        print(canonical_json(body))
        return
    host = os.environ.get("DATABRICKS_HOST", "")
    parsed = urlsplit(host)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.query:
        raise ValueError("Configure a trusted HTTPS Databricks workspace")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not token:
        raise ValueError("Use a configured server-side Databricks token")
    with httpx.Client(
        base_url=host, headers={"Authorization": "Bearer " + token}, timeout=30
    ) as client:
        print(canonical_json(submit_and_wait(client, body)))


if __name__ == "__main__":
    main()
