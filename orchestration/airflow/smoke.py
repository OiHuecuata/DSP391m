"""Trigger real scheduler runs with saved PBPlus; verify rerun and failure gates."""

import hashlib
import json
import time
from datetime import UTC, datetime
from urllib.parse import quote

from control import DAG_ID, AirflowAPI, compose
from runtime import ROOT, write_report

EDGES = (
    ("preflight", "collect_pbplus"),
    ("collect_pbplus", "ingest_pbplus_bronze"),
    ("ingest_pbplus_bronze", "ingest_pbplus_delta"),
    ("ingest_pbplus_delta", "build_court_supply_gold"),
    ("preflight", "check_social_delta"),
    ("check_social_delta", "build_social_activity_gold"),
    ("build_court_supply_gold", "build_market_signal_snapshot"),
    ("build_social_activity_gold", "build_market_signal_snapshot"),
    ("build_market_signal_snapshot", "ensure_clickhouse_ready"),
    ("ensure_clickhouse_ready", "migrate_clickhouse"),
    ("migrate_clickhouse", "load_market_signals_clickhouse"),
    ("load_market_signals_clickhouse", "validate"),
)


def require(condition, message):
    if not condition:
        raise RuntimeError("Airflow smoke failed: " + message)


def wait_run(api, run_id):
    path = f"/api/v2/dags/{DAG_ID}/dagRuns/{quote(run_id, safe='')}"
    deadline = time.monotonic() + 3600
    previous = None
    while time.monotonic() < deadline:
        run = api.request("GET", path)
        tasks = api.request("GET", path + "/taskInstances")["task_instances"]
        states = {t["task_id"]: t["state"] for t in tasks}
        if states != previous:
            print(
                json.dumps({"run_id": run_id, "state": run["state"], "tasks": states}),
                flush=True,
            )
            previous = states
        if run["state"] in ("success", "failed"):
            return {"run_id": run_id, "state": run["state"], "tasks": tasks}
        time.sleep(5)
    raise TimeoutError(f"Airflow run {run_id} did not finish")


def sql(api, query):
    database = api.config["services"]["airflow-scheduler"]["environment"][
        "CLICKHOUSE_DATABASE"
    ]
    return [
        json.loads(line)
        for line in compose(
            "exec",
            "-T",
            "clickhouse",
            "clickhouse-client",
            "--query",
            query.replace("{database}", database)
            + " SETTINGS output_format_json_quote_64bit_integers=0 FORMAT JSONEachRow",
        ).splitlines()
        if line
    ]


def validation(run_id):
    directory = (
        ROOT / ".local/airflow/runs" / hashlib.sha256(run_id.encode()).hexdigest()
    )
    return json.loads((directory / "validation.json").read_text())


def successful(api, conf, run_id):
    api.trigger(conf, run_id)
    run = wait_run(api, run_id)
    require(run["state"] == "success", f"run {run_id} failed; inspect task logs")
    tasks = {t["task_id"]: t for t in run["tasks"]}
    require(
        len(tasks) == 12 and all(t["state"] == "success" for t in tasks.values()),
        "not all batch tasks succeeded",
    )
    for parent, child in EDGES:
        require(
            tasks[parent]["end_date"] <= tasks[child]["start_date"],
            f"dependency executed out of order: {parent} -> {child}",
        )
    run["validation"] = validation(run_id)
    return run


def main():
    api = AirflowAPI()
    require(
        api.request("GET", "/api/v2/monitor/health")["metadatabase"]["status"]
        == "healthy",
        "metadata DB health",
    )
    errors = api.request("GET", "/api/v2/importErrors")
    require(not errors["import_errors"], "DAG import errors")
    require(
        api.request("GET", f"/api/v2/dags/{DAG_ID}")["dag_id"] == DAG_ID,
        "DAG not registered",
    )
    environment = api.config["services"]["airflow-scheduler"]["environment"]
    replay = environment.get("DSP_AIRFLOW_PBPLUS_REPLAY")
    require(
        bool(replay),
        "set DSP_AIRFLOW_PBPLUS_REPLAY to a saved Bronze/crawler artifact before smoke",
    )
    conf = {"pbplus_mode": "replay", "replay_path": replay}
    if geography := environment.get("DSP_AIRFLOW_PBPLUS_GEOGRAPHY"):
        conf["geography_path"] = geography
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")
    first = successful(api, conf, "smoke__" + stamp + "__first")
    second = successful(api, conf, "smoke__" + stamp + "__rerun")
    require(
        first["validation"]["content_sha256"] == second["validation"]["content_sha256"],
        "rerun changed analytical content",
    )
    summary_query = "SELECT count() AS rows, uniqExact(tuple(snapshot_date, location_id)) AS identities, countIf(has_social_signal AND has_supply_signal) AS both_signal, countIf(NOT has_social_signal AND has_supply_signal) AS supply_only, countIf(has_social_signal AND NOT has_supply_signal) AS social_only FROM {database}.location_market_signal_snapshot"
    summary = sql(api, summary_query)[0]
    require(
        summary["rows"] == summary["identities"] == second["validation"]["source_rows"],
        "serving duplicates/count mismatch",
    )
    rows_query = "SELECT * EXCEPT (gold_generated_at), formatDateTime(gold_generated_at, '%Y-%m-%dT%H:%i:%S.%fZ', 'UTC') AS gold_generated_at FROM {database}.location_market_signal_snapshot ORDER BY location_id"
    before = sql(api, rows_query)
    require(
        before == second["validation"]["rows"], "SQL differs from validated source Gold"
    )
    failed_id = "smoke__" + stamp + "__missing_input"
    api.trigger(
        {
            "pbplus_mode": "replay",
            "replay_path": str(ROOT / ".local/airflow/absent-smoke-input.json"),
        },
        failed_id,
    )
    failed = wait_run(api, failed_id)
    states = {t["task_id"]: t["state"] for t in failed["tasks"]}
    require(
        failed["state"] == "failed" and states["preflight"] == "failed",
        "missing input did not fail",
    )
    require(
        all(
            state == "upstream_failed"
            for name, state in states.items()
            if name != "preflight"
        ),
        "upstream failure allowed downstream execution",
    )
    require(sql(api, rows_query) == before, "failed DAG altered serving")
    report = {
        "status": "success",
        "dag_id": DAG_ID,
        "pbplus_mode": "offline_replay",
        "first": first,
        "rerun": second,
        "failure_gate": failed,
        "sql_query": summary_query.replace(
            "{database}", environment["CLICKHOUSE_DATABASE"]
        ),
        "sql_result": summary,
        "failed_run_preserved_serving": True,
        "streaming_lifecycle_managed": False,
    }
    output = ROOT / ".local/airflow/smoke.json"
    write_report(output, report)
    print(
        json.dumps(
            {"status": "success", "evidence": str(output), "sql_result": summary}
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
