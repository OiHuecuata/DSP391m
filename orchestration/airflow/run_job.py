"""Finite command adapter; delegates data processing to existing job CLIs."""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from runtime import ROOT, RunConfig, read_collection, write_report

sys.path[:0] = [str(ROOT / "pipelines/spark/src"), str(ROOT / "apps/collector/src")]

MODULES = {
    "check_social_delta": (
        "dsp_spark.jobs.batch.inspect_delta",
        "--table",
        "social_posts",
    ),
    "build_social_activity_gold": ("dsp_spark.jobs.batch.build_social_activity_daily",),
    "build_court_supply_gold": ("dsp_spark.jobs.batch.build_court_supply_by_location",),
    "build_market_signal_snapshot": (
        "dsp_spark.jobs.batch.build_location_market_signal_snapshot",
    ),
    "migrate_clickhouse": (
        "dsp_spark.jobs.batch.load_market_signals_clickhouse",
        "--migrate-only",
    ),
    "load_market_signals_clickhouse": (
        "dsp_spark.jobs.batch.load_market_signals_clickhouse",
    ),
}


def invoke(config, module, *arguments, capture=False):
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(
            [
                str(config.root / "pipelines/spark/src"),
                str(config.root / "apps/collector/src"),
            ]
        ),
    }
    result = subprocess.run(
        [sys.executable, "-m", module, *arguments],
        cwd=config.root,
        env=env,
        text=True,
        capture_output=capture,
        check=False,
    )
    if capture:
        print(result.stdout, end="", flush=True)
        print(result.stderr, end="", file=sys.stderr, flush=True)
    result.check_returncode()
    return result


def collect(config):
    if (config.directory / "collection.json").exists():
        return read_collection(config)
    inputs = config.input_identity()
    config.directory.mkdir(parents=True, exist_ok=True)
    # A failed attempt may have committed immutable collector artifacts. A new
    # attempt gets a new directory; replay still retains the original run_id.
    artifact_root = tempfile.mkdtemp(prefix="collection-", dir=config.directory)
    arguments = ["--artifact-root", artifact_root, "--no-status"]
    if config.mode == "replay":
        arguments += ["--replay", str(config.replay)]
        if config.geography is not None:
            arguments += ["--geography", str(config.geography)]
    result = invoke(
        config, "dsp_collector.jobs.collect_courts", *arguments, capture=True
    )
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    if payload["status"] != "success":
        raise ValueError(
            "PBPlus collection did not succeed; downstream ingestion blocked"
        )
    hashes = {}
    for name in ("bronze_path", "silver_path"):
        hashes[name] = hashlib.sha256(Path(payload[name]).read_bytes()).hexdigest()
    write_report(
        config.directory / "collection.json",
        {"inputs": inputs, "result": payload, "hashes": hashes},
    )
    return read_collection(config)


def execute(stage, config):
    if stage == "preflight":
        from checks import preflight

        return preflight(config)
    if stage == "collect_pbplus":
        return collect(config)
    if stage in ("ingest_pbplus_bronze", "ingest_pbplus_delta"):
        artifacts = read_collection(config)
        if stage == "ingest_pbplus_bronze":
            return invoke(
                config,
                "dsp_spark.jobs.batch.ingest_bronze",
                "--source",
                "pbplus",
                "--artifact",
                artifacts["bronze_path"],
            )
        return invoke(
            config,
            "dsp_spark.jobs.batch.ingest_pbplus",
            "--artifact",
            artifacts["silver_path"],
        )
    if stage == "ensure_clickhouse_ready":
        from checks import clickhouse_ready

        return clickhouse_ready()
    if stage == "validate":
        from checks import validate_serving

        return validate_serving(config)
    if stage in MODULES:
        module, *arguments = MODULES[stage]
        return invoke(config, module, *arguments)
    raise ValueError(f"Unknown orchestration stage: {stage}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "stage",
        choices=[
            "preflight",
            "collect_pbplus",
            "ingest_pbplus_bronze",
            "ingest_pbplus_delta",
            "ensure_clickhouse_ready",
            "validate",
            *MODULES,
        ],
    )
    args = parser.parse_args()
    result = execute(args.stage, RunConfig.from_env())
    if isinstance(result, dict):
        print(json.dumps(result, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
