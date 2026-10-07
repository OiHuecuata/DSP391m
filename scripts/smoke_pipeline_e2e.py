"""Acceptance: invoke the real Make entrypoint on isolated replay and empty state."""

import json
import os
import subprocess
import tempfile
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/pipeline"


def require(condition, message):
    if not condition:
        raise RuntimeError("Pipeline acceptance failed: " + message)


def run(
    root,
    state,
    threads,
    courts,
    name,
    *,
    end_date="2026-10-05",
    overrides=None,
    success=True,
):
    environment = dict(os.environ)
    # A fresh root must not inherit table overrides or a normal checkpoint.
    for key in list(environment):
        if key.startswith("DSP_DELTA_") or key in (
            "DSP_SPARK_SOCIAL_CHECKPOINT_PATH",
            "KAFKA_SOCIAL_POST_TOPIC",
            "KAFKA_BOOTSTRAP_SERVERS",
            "DSP_KAFKA_SOCIAL_POST_TOPIC",
            "DSP_KAFKA_BOOTSTRAP_SERVERS",
        ):
            environment.pop(key)
    environment.update(
        {
            "PIPELINE_MODE": "replay",
            "PIPELINE_STATE_DIR": str(state / "control"),
            "PIPELINE_ENV_FILE": str(root / "empty.env"),
            "THREADS_REPLAY_PATH": str(threads),
            "PBPLUS_REPLAY_PATH": str(courts),
            "DSP_LAKEHOUSE_BASE_URI": (state / "lake").as_uri(),
            "DSP_SPARK_SOCIAL_CHECKPOINT_PATH": (
                state / "lake/checkpoints/social"
            ).as_uri(),
            "DSP_KAFKA_SOCIAL_POST_TOPIC": "dsp391.e2e." + state.name + ".v1",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "DSP_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "DSP_MARKET_SIGNAL_WINDOW_DAYS": "30",
            "CLICKHOUSE_HOST": "localhost",
            "CLICKHOUSE_DATABASE": "dsp_e2e_" + state.name,
        }
    )
    if end_date is None:
        environment.pop("DSP_MARKET_SIGNAL_END_DATE", None)
    else:
        environment["DSP_MARKET_SIGNAL_END_DATE"] = end_date
    # Explicit overrides beat both Make defaults and .env file values.
    environment.update(overrides or {})
    output = root / (name + ".stdout.log")
    error = root / (name + ".stderr.log")
    with output.open("w") as stdout, error.open("w") as stderr:
        completed = subprocess.run(
            ["make", "--no-print-directory", "pipeline-e2e"],
            cwd=ROOT,
            env=environment,
            stdout=stdout,
            stderr=stderr,
            timeout=1800,
            check=False,
        )
    summary = json.loads(output.read_text().strip().splitlines()[-1])
    if summary.get("summary_path"):
        summary = json.loads(Path(summary["summary_path"]).read_text())
    require(
        (completed.returncode == 0) == success,
        f"{name} exit={completed.returncode}; see {error}",
    )
    require(summary["status"] == ("success" if success else "failed"), f"{name} status")
    print(
        json.dumps(
            {
                "scenario": name,
                "exit_code": completed.returncode,
                "status": summary["status"],
                "summary": summary.get("summary_path"),
                "delta": summary.get("delta"),
            }
        ),
        flush=True,
    )
    return summary


def main():
    # Retain isolated evidence. Nothing in the developer's normal datasets is deleted.
    root = Path(tempfile.mkdtemp(prefix="dsp-pipeline-acceptance-"))
    (root / "empty.env").write_text("")
    populated = root / ("populated_" + uuid4().hex[:12])
    empty = root / ("empty_" + uuid4().hex[:12])
    first = run(
        root,
        populated,
        FIXTURES / "threads.json",
        FIXTURES / "pbplus.json",
        "fresh_replay",
    )
    expected = {
        "social_posts": 2,
        "courts": 2,
        "social_activity_daily": 5,
        "court_supply_by_location": 2,
        "location_market_signal_snapshot": 3,
    }
    require(first["delta"] == expected, "fresh replay counts")
    require(
        first["market_snapshot"]["unresolved_social_location_rows"] == 3,
        "unresolved evidence not preserved/reported",
    )
    second = run(
        root,
        populated,
        FIXTURES / "threads.json",
        FIXTURES / "pbplus.json",
        "identical_replay",
    )
    require(
        second["delta"] == first["delta"], "replay increased observation/output counts"
    )
    require(
        second["fingerprints"] == first["fingerprints"],
        "replay changed analytical values",
    )
    require(
        second["clickhouse"] == first["clickhouse"],
        "replay changed serving count/equality",
    )
    zero = run(
        root,
        empty,
        FIXTURES / "threads-empty.json",
        FIXTURES / "pbplus-empty.json",
        "empty_replay",
    )
    require(
        all(count == 0 for count in zero["delta"].values())
        and zero["clickhouse"]["rows"] == 0,
        "empty publication",
    )
    missing_date = run(
        root,
        empty,
        FIXTURES / "threads-empty.json",
        FIXTURES / "pbplus-empty.json",
        "missing_analysis_date",
        end_date=None,
        success=False,
    )
    require(
        missing_date["stage"] == "market_snapshot",
        "empty social without end date should fail Gold",
    )
    conflict = run(
        root,
        populated,
        FIXTURES / "threads.json",
        FIXTURES / "pbplus.json",
        "conflicting_topic",
        overrides={"KAFKA_SOCIAL_POST_TOPIC": "different.v1"},
        success=False,
    )
    require(
        conflict["stage"] == "configuration", "conflict should fail before collection"
    )
    malformed = json.loads((FIXTURES / "threads.json").read_text())
    malformed["posts"][0]["has_media"] = "not-a-boolean"
    bad_input = root / "invalid-schema.json"
    bad_input.write_text(json.dumps(malformed))
    schema_failure = run(
        root,
        populated,
        bad_input,
        FIXTURES / "pbplus.json",
        "schema_failure",
        success=False,
    )
    require(
        schema_failure["stage"] == "collect_threads",
        "invalid schema should stop artifact handoff",
    )
    report = {
        "status": "success",
        "root": str(root),
        "fresh": first,
        "rerun": second,
        "empty": zero,
        "failure_cases": [missing_date, conflict, schema_failure],
        "live": "NOT RUN",
    }
    (root / "acceptance.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    )
    print(
        json.dumps({"status": "success", "evidence": str(root / "acceptance.json")}),
        flush=True,
    )


if __name__ == "__main__":
    main()
