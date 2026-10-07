"""Offline rendering or explicit one-time submission on configured compute."""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from dsp_spark.runtime.config import LakehousePaths, MarketSignalSettings


def render_job(environment, job_name="social_activity_daily"):
    if job_name not in (
        "social_activity_daily",
        "court_supply_by_location",
        "location_market_signal_snapshot",
    ):
        raise ValueError("Unsupported Gold job")
    required = (
        "DATABRICKS_CLUSTER_ID",
        "DSP_DATABRICKS_WHEEL_URI",
        "DSP_LAKEHOUSE_BASE_URI",
    )
    missing = [name for name in required if not environment.get(name)]
    if missing:
        raise ValueError("Missing configuration: " + ", ".join(missing))
    paths = LakehousePaths(
        environment["DSP_LAKEHOUSE_BASE_URI"],
        courts=environment.get("DSP_DELTA_COURTS_PATH", ""),
        court_supply_by_location=environment.get(
            "DSP_DELTA_COURT_SUPPLY_BY_LOCATION_PATH", ""
        ),
        unresolved_court_locations=environment.get(
            "DSP_DELTA_UNRESOLVED_COURT_LOCATIONS_PATH", ""
        ),
        social_posts=environment.get("DSP_DELTA_SOCIAL_POSTS_PATH", ""),
        social_activity_daily=environment.get(
            "DSP_DELTA_SOCIAL_ACTIVITY_DAILY_PATH", ""
        ),
        location_market_signal_snapshot=environment.get(
            "DSP_DELTA_LOCATION_MARKET_SIGNAL_SNAPSHOT_PATH", ""
        ),
    )
    targets = (
        (
            paths.base,
            paths.social_activity_daily,
            paths.court_supply_by_location,
            paths.unresolved_court_locations,
            paths.location_market_signal_snapshot,
        )
        if job_name == "location_market_signal_snapshot"
        else (
            (
                paths.base,
                paths.courts,
                paths.court_supply_by_location,
                paths.unresolved_court_locations,
            )
            if job_name == "court_supply_by_location"
            else (paths.base, paths.social_posts, paths.social_activity_daily)
        )
    )
    if not all(value.startswith("s3a://") for value in targets):
        raise ValueError("Configure explicit S3A lakehouse, Silver and Gold targets")
    wheel = environment["DSP_DATABRICKS_WHEEL_URI"]
    if (
        not wheel.endswith(".whl")
        or not wheel.startswith(("/Workspace/", "/Volumes/", "s3://"))
        or "?" in wheel
        or "@" in wheel
    ):
        raise ValueError(
            "Wheel must be an explicit .whl in Workspace files, a Volume or S3; no credentials/query strings"
        )
    values = {
        "DATABRICKS_CLUSTER_ID": environment["DATABRICKS_CLUSTER_ID"],
        "DSP_DATABRICKS_WHEEL_URI": wheel,
        "DSP_LAKEHOUSE_BASE_URI": paths.base,
        "DSP_DELTA_SOCIAL_POSTS_PATH": paths.social_posts,
        "DSP_DELTA_COURTS_PATH": paths.courts,
        "DSP_DELTA_COURT_SUPPLY_BY_LOCATION_PATH": paths.court_supply_by_location,
        "DSP_DELTA_UNRESOLVED_COURT_LOCATIONS_PATH": paths.unresolved_court_locations,
        "DSP_DELTA_SOCIAL_ACTIVITY_DAILY_PATH": paths.social_activity_daily,
        "DSP_DELTA_LOCATION_MARKET_SIGNAL_SNAPSHOT_PATH": paths.location_market_signal_snapshot,
        "DSP_MARKET_SIGNAL_WINDOW_DAYS": environment.get(
            "DSP_MARKET_SIGNAL_WINDOW_DAYS", "30"
        ),
    }
    template = json.loads(
        (Path(__file__).parent / "jobs" / f"{job_name}.json").read_text()
    )

    def substitute(value):
        if isinstance(value, dict):
            return {key: substitute(child) for key, child in value.items()}
        if isinstance(value, list):
            return [substitute(child) for child in value]
        if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            return values[value[2:-1]]
        return value

    rendered = substitute(template)
    if job_name == "location_market_signal_snapshot":
        raw_days = values["DSP_MARKET_SIGNAL_WINDOW_DAYS"]
        if not isinstance(raw_days, str) or not raw_days.isdecimal():
            raise ValueError("DSP_MARKET_SIGNAL_WINDOW_DAYS must be a positive integer")
        market = MarketSignalSettings(
            int(raw_days), environment.get("DSP_MARKET_SIGNAL_END_DATE")
        )
        if market.end_date is not None:
            rendered["tasks"][0]["python_wheel_task"]["parameters"] += [
                "--end-date",
                market.end_date,
            ]
    return rendered


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate/render locally; no CLI, credentials or remote requests required",
    )
    parser.add_argument(
        "--job",
        choices=(
            "social_activity_daily",
            "court_supply_by_location",
            "location_market_signal_snapshot",
        ),
        default="social_activity_daily",
    )
    args = parser.parse_args(argv)
    try:
        job = render_job(os.environ, args.job)
        if args.dry_run:
            print(json.dumps(job, ensure_ascii=False, indent=2))
            print(
                "Local configuration validated; no remote validation or submission occurred.",
                file=sys.stderr,
            )
            return 0
        if not (os.getenv("DATABRICKS_HOST") or os.getenv("DATABRICKS_CONFIG_PROFILE")):
            raise ValueError(
                "Set DATABRICKS_HOST or DATABRICKS_CONFIG_PROFILE for CLI authentication"
            )
        if not shutil.which("databricks"):
            raise ValueError(
                "Install/configure Databricks CLI 0.205+ before submission"
            )
        with tempfile.TemporaryDirectory(prefix="dsp-databricks-submit-") as directory:
            body = Path(directory) / "job.json"
            body.write_text(json.dumps(job), encoding="utf-8")
            command = [
                "databricks",
                "jobs",
                "submit",
                "--json",
                "@" + str(body),
                "--no-wait",
            ]
            if profile := os.getenv("DATABRICKS_CONFIG_PROFILE"):
                command += ["--profile", profile]
            subprocess.run(command, check=True)
        print(
            "Submission request accepted; inspect the returned run ID for execution status."
        )
        return 0
    except (ValueError, subprocess.CalledProcessError) as exc:
        print(
            (
                str(exc)
                if isinstance(exc, ValueError)
                else "Databricks submission failed; inspect CLI diagnostics."
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
