"""Reproducible full-stack fixture verification; never reads operational lakehouse data."""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [
    str(ROOT),
    str(ROOT / "apps/collector/src"),
    str(ROOT / "pipelines/spark/src"),
]


def environment():
    value = dict(os.environ)
    value.update(
        JAVA_HOME=str(ROOT / ".local/tools/java17"),
        PYSPARK_PYTHON=str(ROOT / ".venv/bin/python"),
        PYSPARK_DRIVER_PYTHON=str(ROOT / ".venv/bin/python"),
        SPARK_LOCAL_IP="127.0.0.1",
        PYTHONPATH=os.pathsep.join(sys.path[:3]),
        DSP_SPARK_JARS=",".join(
            str(p)
            for p in sorted((ROOT / "pipelines/spark/.local/ivy/jars").glob("*.jar"))
        ),
        DSP_THREADS_REPO_ROOT=str(ROOT),
        DSP_THREADS_UID=str(os.getuid()),
        PIPELINE_ENV_FILE="/dev/null",
    )
    if not value["DSP_SPARK_JARS"]:
        raise RuntimeError(
            "Resolve the repository's locked Spark/Delta/Kafka JARs before running offline integration"
        )
    (Path("/tmp") / f"dsp391m-analytical-ownership-{os.getuid()}").mkdir(
        mode=0o700, exist_ok=True
    )
    return value


def command(args, *, env, directory=ROOT, log=None, timeout=600):
    if log:
        with Path(log).open("w") as output:
            subprocess.run(
                args,
                cwd=directory,
                env=env,
                stdout=output,
                stderr=subprocess.STDOUT,
                check=True,
                timeout=timeout,
            )
    else:
        subprocess.run(args, cwd=directory, env=env, check=True, timeout=timeout)


def integration(*, output=None):
    import httpx
    from dsp_collector.normalizers.observation import canonical_json

    from tests.threads_vertical.fixtures import POLICY

    directory = (
        Path(output).resolve()
        if output
        else ROOT
        / ".local/threads-vertical"
        / datetime.now(UTC).strftime("e2e-%Y%m%dT%H%M%SZ")
    )
    if not directory.is_relative_to(ROOT / ".local/threads-vertical"):
        raise ValueError("Fixture output must stay inside .local/threads-vertical")
    directory.mkdir(parents=True, exist_ok=True)
    env = environment()
    config = {
        "mode": "fixture",
        "source_policy": POLICY,
        "acquisition": {
            "raw_base_uri": (directory / "raw").as_uri(),
            "unique_target": 120,
            "request_budget": 8,
            "queries": ["pickleball", "pickleball giá thuê"],
            "concurrency": 1,
            "pacing_seconds": 0.5,
            "max_public_pages": 2,
        },
        "control_base_uri": (directory / "control").as_uri(),
        "lakehouse_base_uri": (directory / "lakehouse").as_uri(),
        "kafka_bootstrap_servers": "127.0.0.1:19092",
        "clickhouse": {
            "host": "127.0.0.1",
            "port": 18123,
            "database": "threads_fixture",
            "user": "threads_fixture",
            "password": "isolated-fixture-only",
        },
        "admin_aliases": [],
    }
    config_path = directory / "threads-config.json"
    config_path.write_text(canonical_json(config))
    controller_path = directory / "controller-config.json"
    controller_path.write_text(
        canonical_json({"minimum_free_bytes": 0, "threads_vertical": config})
    )
    compose = [
        "docker",
        "compose",
        "-p",
        "dsp-threads-fixture",
        "-f",
        str(ROOT / "infra/threads/compose.integration.yaml"),
    ]
    command(compose + ["up", "-d", "--wait"], env=env)
    command(
        compose
        + [
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "127.0.0.1:19092",
            "--create",
            "--if-not-exists",
            "--topic",
            "dsp.social.observations.v2",
            "--partitions",
            "2",
            "--replication-factor",
            "1",
        ],
        env=env,
    )
    backend = directory / "backend"
    command(
        ["go", "build", "-o", str(backend), "./cmd/server"],
        env=env,
        directory=ROOT / "apps/backend",
        log=directory / "go-build.log",
    )
    env.update(
        DSP_REPO_ROOT=str(ROOT),
        SERVER_PORT="19080",
        COLLECTION_STATE_DIR=str(directory / "controller"),
        COLLECTION_CONFIG=str(controller_path),
        COLLECTION_PYTHON=str(ROOT / ".venv/bin/python"),
        DSP_LAKEHOUSE_BASE_URI=config["lakehouse_base_uri"],
        CLICKHOUSE_HOST="127.0.0.1",
        CLICKHOUSE_PORT="18123",
        CLICKHOUSE_DATABASE="threads_fixture",
        CLICKHOUSE_USER="threads_fixture",
        CLICKHOUSE_PASSWORD="isolated-fixture-only",
        BACKEND_URL="http://127.0.0.1:19080",
    )
    backend_log = (directory / "backend.log").open("w")
    server = subprocess.Popen(
        [str(backend)], cwd=ROOT, env=env, stdout=backend_log, stderr=subprocess.STDOUT
    )
    frontend = None
    frontend_log = None
    try:
        with httpx.Client(
            base_url="http://127.0.0.1:19080", timeout=20, trust_env=False
        ) as client:
            for _ in range(40):
                try:
                    if client.get("/health").status_code == 200:
                        break
                except httpx.TransportError:
                    pass
                time.sleep(0.5)
            if (directory / "go-acquisition.json").is_file():
                run = json.loads((directory / "go-acquisition.json").read_text())
            else:
                result = client.post(
                    "/api/v1/collection/run", json={"sources": ["threads"]}
                )
                result.raise_for_status()
                deadline = time.monotonic() + 600
                while time.monotonic() < deadline:
                    status = client.get("/api/v1/collection/status").json()
                    runs = status["runs"]
                    if runs and runs[0]["status"] in ("SUCCESS", "PARTIAL", "FAILED"):
                        run = runs[0]
                        break
                    time.sleep(1)
                else:
                    raise TimeoutError("Go acquisition worker did not finish")
            (directory / "go-acquisition.json").write_text(canonical_json(run))
            assert run["status"] == "PARTIAL", run
            assert run["metrics"]["unique_posts"] == 120, run
            summary = json.loads(
                (
                    directory / "controller/runs" / run["run_id"] / "summary.json"
                ).read_text()
            )
            manifest_uri = summary["handoff"]["artifact_uri"]
            conf = {"config_path": str(config_path), "manifest_uri": manifest_uri}
            conf_path = directory / "airflow-conf.json"
            conf_path.write_text(canonical_json(conf))
            env["DSP_THREADS_FIXTURE_CONF"] = str(conf_path)
            print(
                "Go acquisition and durable Kafka handoff verified; executing the real Airflow DAG",
                flush=True,
            )
            command(
                compose + ["--profile", "airflow-test", "run", "--rm", "airflow-test"],
                env=env,
                log=directory / "airflow.log",
                timeout=1800,
            )
            response = client.get("/api/v2/research/threads/eda")
            response.raise_for_status()
            data = response.json()
            assert data["overview"]["unique_posts"] == 120, data
            assert data["overview"]["replies"] == 6, data
            assert data["overview"]["observations"] == 126, data
            assert data["generation"]["status"] == "INACTIVE_PARTIAL", data
            generation = data["generation"]["generation_id"]
            assert all(
                v == "BLOCKED_DEPENDENCY"
                for v in data["generation"]["dataset_status"].values()
            ), data
            assert data["overview"]["resolved_content"] == 0, data
            assert len(data["intents"]) > 1 and data["geography"], data
            assert (
                client.get(
                    "/api/v2/research/threads/eda",
                    params={"generation": generation, "metric": "shares"},
                ).json()["overview"]["content_with_metric"]
                == 0
            )
            assert (
                client.get(
                    "/api/v2/research/threads/eda",
                    params={"generation": generation, "intent": "SEEK_COURT"},
                ).json()["overview"]["unique_posts"]
                == 24
            )
            assert (
                client.get(
                    "/api/v2/research/threads/eda",
                    params={"metric": "likes; DROP TABLE"},
                ).status_code
                == 400
            )
            (directory / "api-eda.json").write_text(canonical_json(data))
            print(
                "Kafka → Spark/Delta → ClickHouse → Go API verified; checking replay and browser",
                flush=True,
            )
            command(
                [
                    str(ROOT / ".venv/bin/python"),
                    "-m",
                    "orchestration.local.threads_vertical",
                    "process",
                    "--config",
                    str(config_path),
                    "--manifest-uri",
                    manifest_uri,
                ],
                env=env,
                log=directory / "replay.log",
                timeout=900,
            )
            repeated = client.get(
                "/api/v2/research/threads/eda", params={"generation": generation}
            )
            repeated.raise_for_status()
            assert repeated.json() == data
            latest = client.get("/api/v2/research/threads/generation").json()
            assert latest["generation_id"] == generation
        command(
            ["npm", "run", "build", "--", "--webpack"],
            env=env,
            directory=ROOT / "apps/frontend",
            log=directory / "frontend-build.log",
        )
        frontend_log = (directory / "frontend.log").open("w")
        frontend = subprocess.Popen(
            [
                "node",
                str(ROOT / "apps/frontend/node_modules/next/dist/bin/next"),
                "start",
                "--hostname",
                "127.0.0.1",
                "--port",
                "13100",
            ],
            cwd=ROOT / "apps/frontend",
            env=env,
            stdout=frontend_log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        from playwright.sync_api import sync_playwright

        with httpx.Client(trust_env=False) as client:
            for _ in range(50):
                try:
                    if (
                        client.get(
                            "http://127.0.0.1:13100/research/threads"
                        ).status_code
                        == 200
                    ):
                        break
                except httpx.TransportError:
                    pass
                time.sleep(0.5)
        if frontend.poll() is not None:
            raise RuntimeError(
                "The owned fixture frontend did not start; check for a port conflict"
            )
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 1100})
            page.goto("http://127.0.0.1:13100/research/threads")
            page.get_by_test_id("unique-posts").filter(has_text="120").wait_for(
                timeout=30000
            )
            assert page.get_by_test_id("replies-collected").inner_text() == "6"
            assert page.get_by_text("Synthetic fixture data", exact=False).count() == 1
            page.screenshot(path=str(directory / "dashboard.png"), full_page=True)
            page.get_by_label("Metric", exact=True).select_option("shares")
            page.get_by_role("button", name="Apply filters").click()
            page.get_by_test_id("content-with-selected-metric").filter(has_text="0").wait_for()
            from playwright.sync_api import expect
            expect(page.get_by_test_id("content-with-selected-metric")).to_have_text("0")
            page.get_by_text("Generation and evidence limits", exact=True).click()
            assert page.get_by_test_id("generation").inner_text() == generation
            assert page.get_by_text("Unavailable", exact=True).count() > 0
            browser.close()
        report = {
            "verification_level": "LOCAL_INTEGRATION_VERIFIED",
            "fixture": True,
            "real_source_records_collected": 0,
            "cloud_e2e_verified": False,
            "unique_posts": 120,
            "replies": 6,
            "observations": 126,
            "generation_id": generation,
            "airflow_dag": "dsp391m_threads_research_refresh",
            "checks": [
                "Go acquisition",
                "multi-query deduplication",
                "raw/native parity",
                "real Kafka transport",
                "real Spark Delta Silver/Gold",
                "real Airflow DAG",
                "ClickHouse full-row parity",
                "typed Go API filters",
                "pinned Next.js browser data",
                "rerun stable generation",
            ],
        }
        (directory / "verification.json").write_text(canonical_json(report))
        (ROOT / ".local/threads-vertical/latest-evidence.txt").write_text(
            str(directory)
        )
        print(json.dumps(report, indent=2), flush=True)
    finally:
        if frontend:
            os.killpg(frontend.pid, signal.SIGTERM)
            frontend.wait(timeout=20)
        server.terminate()
        server.wait(timeout=30)
        backend_log.close()
        if frontend_log:
            frontend_log.close()
    return directory


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("integration", "faults", "legacy-regression"))
    parser.add_argument("--output")
    args = parser.parse_args()
    if args.action == "legacy-regression":
        env = environment()
        for pattern in ("test_golden_baseline.py", "test_observation_*.py"):
            command([str(ROOT/".venv/bin/python"), "-m", "unittest", "discover", "-v", "-s", "pipelines/spark/test", "-p", pattern], env=env)
    elif args.action == "faults":
        directory = (
            Path(args.output).resolve()
            if args.output
            else ROOT
            / ".local/threads-vertical"
            / datetime.now(UTC).strftime("faults-%Y%m%dT%H%M%SZ")
        )
        if not directory.is_relative_to(ROOT / ".local/threads-vertical"):
            raise ValueError("Fault fixtures require isolated local output")
        directory.mkdir(parents=True, exist_ok=True)
        command(
            [
                str(ROOT / ".venv/bin/python"),
                "-m",
                "tests.threads_vertical.spark_faults",
                str(directory),
            ],
            env=environment(),
            log=directory / "spark-faults.log",
        )
        print((directory / "fault-verification.json").read_text())
    else:
        integration(output=args.output)


if __name__ == "__main__":
    main()
