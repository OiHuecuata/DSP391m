"""Readiness and equality checks; all analytical computations stay in Spark jobs."""

import hashlib
import json
import os
import urllib.request
from pathlib import Path
from urllib.parse import unquote, urlsplit

from runtime import write_report


def preflight(config):
    from dsp_spark.runtime.config import LakehousePaths, MarketSignalSettings

    MarketSignalSettings.from_env()  # Do not invent an Airflow execution-date override.
    for artifact in (config.replay, config.geography):
        if artifact is not None and not artifact.is_file():
            raise FileNotFoundError(
                f"Required saved PBPlus artifact is missing: {artifact}"
            )
    paths = LakehousePaths.from_env()
    for name in (
        "social_posts",
        "pbplus_bronze",
        "courts",
        "social_activity_daily",
        "court_supply_by_location",
        "unresolved_court_locations",
        "location_market_signal_snapshot",
    ):
        uri = getattr(paths, name)
        parts = urlsplit(uri)
        if parts.scheme != "file":
            raise ValueError(
                f"Local Airflow requires a mounted local Delta path: {name}={uri}"
            )
        path = Path(unquote(parts.path))
        if name == "social_posts":
            if not (path / "_delta_log").is_dir():
                raise FileNotFoundError(
                    f"Required social Delta input missing: {path}; run the independent streaming path first"
                )
        else:
            parent = path
            while not parent.exists():
                parent = parent.parent
            if not os.access(parent, os.W_OK):
                raise PermissionError(
                    f"Delta output is not writable through its shared mount: {path}"
                )
    config.directory.mkdir(parents=True, exist_ok=True)
    return {
        "status": "success",
        "pbplus_mode": config.mode,
        "social_delta": paths.social_posts,
    }


def clickhouse_ready():
    from dsp_spark.runtime.clickhouse import ClickHouseSettings

    settings = ClickHouseSettings.from_env()
    host = settings.host if ":" not in settings.host else f"[{settings.host}]"
    # /ping checks the service; the existing migration/loader checks auth/schema.
    request = urllib.request.Request(f"http://{host}:{settings.port}/ping")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=5) as response:
        if response.status != 200 or response.read().strip() != b"Ok.":
            raise RuntimeError("ClickHouse /ping is not ready")
    return {"status": "success", "clickhouse_ready": True}


def validate_serving(config):
    from dsp_spark.io.clickhouse import ClickHouseClient, read_gold_rows
    from dsp_spark.runtime.clickhouse import ClickHouseSettings
    from dsp_spark.runtime.config import SparkSettings
    from dsp_spark.runtime.spark import create_spark

    settings = SparkSettings.from_env()
    spark = create_spark(settings)
    try:
        source = read_gold_rows(spark, settings.paths.location_market_signal_snapshot)
        with ClickHouseClient(ClickHouseSettings.from_env()) as client:
            served = client.read_rows()
        if served != source:
            raise ValueError("Served rows differ from Delta Gold after refresh")
        # Content fingerprint omits only rebuild metadata for rerun comparison.
        metrics = [
            {k: v for k, v in row.items() if k != "gold_generated_at"} for row in source
        ]
        report = {
            "status": "success",
            "run_id": config.run_id,
            "source_delta_path": settings.paths.location_market_signal_snapshot,
            "source_rows": len(source),
            "served_rows": len(served),
            "all_fields_match": True,
            "content_sha256": hashlib.sha256(
                json.dumps(metrics, sort_keys=True, allow_nan=False).encode()
            ).hexdigest(),
            "rows": served,
        }
        write_report(config.directory / "validation.json", report)
        return report
    finally:
        spark.stop()
