"""Polite live accumulation using existing collection and Delta/warehouse jobs."""

import argparse
import json
import os
import shutil
import sys
import time
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [
    str(ROOT),
    str(ROOT / "apps/collector/src"),
    str(ROOT / "pipelines/spark/src"),
]
from orchestration.local import pipeline


def progress_json(progress):
    """Spark 4 progress exposes native UUIDs; its JSON form is wire-safe."""
    return [
        (
            json.loads(item.json)
            if hasattr(item, "json")
            else json.loads(json.dumps(item, default=str))
        )
        for item in progress
    ]


def capture_baseline(config, root):
    """Record existing state once before a new campaign changes the lakehouse."""
    path = root / "baseline.json"
    if path.is_file():
        return
    from delta.tables import DeltaTable
    from dsp_spark.io.delta import read_delta
    from dsp_spark.runtime.spark import create_spark

    spark = create_spark(config.spark)
    result = {
        "tables": {},
        "free_bytes": shutil.disk_usage(root).free,
        "lakehouse_uri": config.spark.paths.base,
    }
    try:
        for name in config.spark.paths.__dataclass_fields__:
            if name in ("base", "social_checkpoint"):
                continue
            uri = getattr(config.spark.paths, name)
            row = {"rows": 0, "exists": DeltaTable.isDeltaTable(spark, uri)}
            if row["exists"]:
                frame = read_delta(spark, uri)
                row["rows"] = frame.count()
                if "run_id" in frame.columns:
                    row["runs"] = [
                        r[0]
                        for r in frame.select("run_id").distinct().collect()
                        if not r[0].startswith(
                            ("kafka-smoke-", "pipeline-fixture-", "offline-replay-")
                        )
                    ]
            result["tables"][name] = row
    finally:
        spark.stop()
    pipeline.write_report(path, result)


def content(record, columns):
    return {
        k: sorted(record[k]) if isinstance(record.get(k), list) else record.get(k)
        for k in columns
    }


def latest_map(spark, settings, source):
    from delta.tables import DeltaTable
    from dsp_spark.io.delta import read_delta
    from dsp_spark.processing.aggregate.court_supply_by_location import latest_courts
    from dsp_spark.processing.aggregate.social_activity_daily import latest_social_posts

    uri = settings.paths.social_posts if source == "threads" else settings.paths.courts
    if not DeltaTable.isDeltaTable(spark, uri):
        return {}
    f = read_delta(spark, uri)
    f = latest_social_posts(f) if source == "threads" else latest_courts(f)
    return {r.record_id: r.asDict(recursive=True) for r in f.collect()}


def collect_one(root, cycle, source, configuration):
    from dsp_collector.jobs.collect_courts import collect_pbplus
    from dsp_collector.jobs.collect_social import collect_threads

    directory = root / f"cycle-{cycle}" / source
    directory.mkdir(parents=True, exist_ok=True)
    result_path = directory / "result.json"
    started = time.monotonic()
    if result_path.is_file():
        result = json.loads(result_path.read_text())
    elif (
        source == "pbplus"
        and (directory / "result.log").is_file()
        and (directory / "result.log").read_text().strip()
    ):
        # Exact result of the first live CLI run started during source inspection.
        result = json.loads(
            (directory / "result.log").read_text().strip().splitlines()[-1]
        )
        result["collection_seconds"] = (
            directory.joinpath("result.log").stat().st_mtime
            - directory.joinpath("result.log").stat().st_birthtime
            if hasattr(directory.joinpath("result.log").stat(), "st_birthtime")
            else None
        )
        if result["collection_seconds"] is None:
            import subprocess

            birth = float(
                subprocess.check_output(
                    ["stat", "-c", "%W", str(directory / "result.log")], text=True
                )
            )
            result["collection_seconds"] = (
                directory / "result.log"
            ).stat().st_mtime - birth
    else:
        if shutil.disk_usage(root).free < configuration["minimum_free_bytes"]:
            raise RuntimeError("Disk capacity guard stopped collection")
        if source == "threads":
            c = configuration[source]
            options = {
                k: c[k]
                for k in (
                    "query_seeds",
                    "max_results_per_query",
                    "detail_limit",
                    "query_delay",
                    "retries",
                )
            }
            if cycle > 1:
                options["query_seeds"] = c["second_cycle_seeds"]
            collected = collect_threads(
                query="pickleball",
                limit=c["limit"],
                sort=c["cycle_sorts"][(cycle - 1) % len(c["cycle_sorts"])],
                timeout_ms=c["timeout_ms"],
                per_query_cap=c["per_query_cap"],
                artifact_root=directory,
                crawl_options=options,
            )
        else:
            collected = collect_pbplus(artifact_root=directory, **configuration[source])
        result = json.loads(json.dumps(asdict(collected), default=str))
        result["collection_seconds"] = time.monotonic() - started
    result["live"] = True
    pipeline.write_report(result_path, result)
    manifest = {
        "result": result,
        "exit_code": {"success": 0, "partial": 2, "failed": 1}[result["status"]],
        "hashes": {
            k: pipeline.digest(result[k]) for k in ("bronze_path", "silver_path")
        },
        "input": None,
        "live": True,
    }
    pipeline.check_artifacts(source, manifest, directory)
    manifest_path = directory / "manifest.json"
    pipeline.write_report(manifest_path, manifest)
    return {**result, "manifest_path": str(manifest_path)}


def process(config, collections, report):
    with pipeline.lakehouse_lock(config):
        return _process_locked(config, collections, report)


def _process_locked(config, collections, report):
    from delta.tables import DeltaTable
    from dsp_collector.publishers.kafka import (
        publish_bronze_batch,
        publish_social_batch,
    )
    from dsp_spark.io.clickhouse import (
        ClickHouseClient,
        read_gold_rows,
        replace_serving,
    )
    from dsp_spark.io.delta import read_delta
    from dsp_spark.jobs.batch.build_court_supply_by_location import (
        build_court_supply_by_location,
    )
    from dsp_spark.jobs.batch.build_location_market_signal_snapshot import (
        build_location_market_signal_snapshot,
    )
    from dsp_spark.jobs.batch.build_social_activity_daily import (
        build_social_activity_daily,
    )
    from dsp_spark.jobs.batch.ingest_bronze import ingest_bronze
    from dsp_spark.jobs.batch.ingest_pbplus import ingest_pbplus
    from dsp_spark.jobs.streaming.social_kafka_to_delta import start_social_stream
    from dsp_spark.runtime.spark import create_spark

    def stage(name, operation, input_count=None):
        t = time.monotonic()
        value = operation()
        elapsed = time.monotonic() - t
        report["stages"][name] = {
            "seconds": elapsed,
            "input_records": input_count,
            "records_per_second": (
                input_count / elapsed if input_count is not None and elapsed else None
            ),
            "result": value,
        }
        return value

    t = time.monotonic()
    spark = create_spark(config.spark, kafka=True)
    report["stages"]["spark_runtime"] = {"seconds": time.monotonic() - t}
    query = None
    try:
        for source, result in collections.items():
            before = latest_map(spark, config.spark, source)
            rows = json.loads(Path(result["silver_path"]).read_text())["records"]
            columns = (
                (
                    "text",
                    "published_at",
                    "author_username",
                    "like_count",
                    "reply_count",
                    "repost_count",
                    "quote_count",
                    "has_media",
                    "detected_locations",
                    "categories",
                )
                if source == "threads"
                else (
                    "court_name",
                    "number_of_courts",
                    "has_media",
                    "address",
                    "latitude",
                    "longitude",
                    "address_source",
                    "geo_status",
                )
            )
            new = sum(r["record_id"] not in before for r in rows)
            unchanged = sum(
                r["record_id"] in before
                and content(r, columns) == content(before[r["record_id"]], columns)
                for r in rows
            )
            engagement = (
                sum(
                    r["record_id"] in before
                    and any(
                        r[k] != before[r["record_id"]].get(k)
                        for k in (
                            "like_count",
                            "reply_count",
                            "repost_count",
                            "quote_count",
                        )
                    )
                    for r in rows
                )
                if source == "threads"
                else None
            )
            table = "social_posts" if source == "threads" else "courts"
            from pyspark.sql import functions as F

            uri = getattr(config.spark.paths, table)
            existing_observations = (
                read_delta(spark, uri)
                .filter(F.col("run_id") == result["run_id"])
                .count()
                if DeltaTable.isDeltaTable(spark, uri)
                else 0
            )
            report["identity_changes"][source] = {
                "new_identities": new,
                "existing_identities": len(rows) - new,
                "unchanged_observations": unchanged,
                "updated_observations": len(rows) - new - unchanged,
                "engagement_updated": engagement,
                "new_observations": len(rows) - existing_observations,
            }
            stage(
                "bronze_" + source,
                lambda source=source, result=result: ingest_bronze(
                    spark, result["bronze_path"], config.spark, source=source
                ),
                1,
            )
        if "pbplus" in collections:
            stage(
                "court_silver",
                lambda: ingest_pbplus(
                    spark, collections["pbplus"]["silver_path"], config.spark
                ),
                collections["pbplus"]["count"],
            )
        if "threads" in collections:
            silver = Path(collections["threads"]["silver_path"])
            raw_publication = stage(
                "bronze_kafka_publish",
                lambda: asdict(
                    publish_bronze_batch(
                        collections["threads"]["bronze_path"], settings=config.kafka
                    )
                ),
            )
            if raw_publication["flush_error"] or any(
                e.get("event_id") or e.get("observation_id")
                for e in raw_publication["errors"]
            ):
                raise RuntimeError("Bronze Kafka publication failed")
            publication = stage(
                "kafka_publish",
                lambda: asdict(publish_social_batch(silver, settings=config.kafka)),
                collections["threads"]["count"],
            )
            if publication["status"] != "success":
                raise RuntimeError("Kafka publication not fully acknowledged")
            t = time.monotonic()
            query = start_social_stream(spark, config.spark, available_now=True)
            if not query.awaitTermination(config.stream_timeout):
                raise TimeoutError("Stream did not finish")
            elapsed = time.monotonic() - t
            consumed = sum(p["numInputRows"] for p in query.recentProgress)
            from dsp_collector.publishers.kafka import build_social_post_event

            event_bytes = sum(
                len(
                    json.dumps(
                        build_social_post_event(r), ensure_ascii=False, allow_nan=False
                    ).encode("utf-8")
                )
                for r in json.loads(silver.read_text())["records"]
            )
            report["stages"]["social_stream"] = {
                "seconds": elapsed,
                "input_records": consumed,
                "records_per_second": consumed / elapsed,
                "progress": progress_json(query.recentProgress),
                "published_event_value_bytes": event_bytes,
            }
        social_input = read_delta(spark, config.spark.paths.social_posts).count()
        court_input = read_delta(spark, config.spark.paths.courts).count()
        stage(
            "social_gold",
            lambda: build_social_activity_daily(spark, config.spark),
            social_input,
        )
        stage(
            "court_gold",
            lambda: build_court_supply_by_location(spark, config.spark),
            court_input,
        )
        stage(
            "market_snapshot",
            lambda: build_location_market_signal_snapshot(
                spark, config.spark, market_settings=config.market
            ),
            read_delta(spark, config.spark.paths.social_activity_daily).count()
            + read_delta(spark, config.spark.paths.court_supply_by_location).count(),
        )
        with ClickHouseClient(config.warehouse) as client:
            rows = read_gold_rows(
                spark, config.spark.paths.location_market_signal_snapshot
            )
            stage("clickhouse_load", lambda: replace_serving(client, rows), len(rows))
        report["verification"] = pipeline.verify(spark, config, collections)
    finally:
        try:
            if query is not None and query.isActive:
                query.stop()
        finally:
            spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default=os.getenv("LIVE_EDA_CONFIG", "configs/collection/live_eda.v1.json"),
    )
    parser.add_argument(
        "--output", default=os.getenv("LIVE_EDA_OUTPUT", ".local/live-eda")
    )
    parser.add_argument(
        "--cycles", type=int, default=int(os.getenv("LIVE_EDA_CYCLES", "2"))
    )
    parser.add_argument(
        "--ingest-only",
        action="store_true",
        help="Reprocess saved live runs without recrawling",
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=("threads", "pbplus"),
        default=["threads", "pbplus"],
    )
    args = parser.parse_args()
    root = Path(args.output).resolve()
    root.mkdir(parents=True, exist_ok=True)
    c = json.loads(Path(args.config).read_text())
    os.environ.update(
        PIPELINE_MODE="live",
        THREADS_REPLAY_PATH="",
        PBPLUS_REPLAY_PATH="",
        KAFKA_ENABLED="false",
    )
    config = pipeline.resolve_config()
    pipeline.preflight(config)
    pipeline.ensure_infrastructure(config)
    capture_baseline(config, root)
    reports = []
    blocked_sources = set()
    for cycle in range(1, args.cycles + 1):
        report = {
            "cycle": cycle,
            "started_at": datetime.now(UTC).isoformat(),
            "free_before": shutil.disk_usage(root).free,
            "collections": {},
            "errors": {},
            "identity_changes": {},
            "stages": {},
        }
        report_path = root / f"cycle-{cycle}" / "processing.json"
        if report_path.is_file() and not args.ingest_only:
            reports.append(json.loads(report_path.read_text()))
            continue
        for source in args.sources:
            if source in blocked_sources:
                report["errors"][
                    source
                ] = "Access was blocked in previous cycle; not retried"
                continue
            try:
                result = collect_one(root, cycle, source, c)
                report["collections"][source] = result
                bronze = json.loads(Path(result["bronze_path"]).read_text())
                if bronze.get("collection_stopping_reason") == "access_blocked" or any(
                    q.get("blocked") for q in bronze["query_runs"]
                ):
                    blocked_sources.add(source)
            except Exception as exc:  # noqa: BLE001
                report["errors"][source] = str(exc)
                from dsp_collector.sources.crawler.pbplus.crawler import (
                    PBPlusAccessBlocked,
                )
                from dsp_collector.sources.crawler.threads.crawler import (
                    ThreadsAccessBlocked,
                )

                if isinstance(exc, (ThreadsAccessBlocked, PBPlusAccessBlocked)):
                    blocked_sources.add(source)
        if report["collections"]:
            try:
                process(config, report["collections"], report)
                report["status"] = (
                    "partial"
                    if report["errors"]
                    or any(
                        x["status"] == "partial" for x in report["collections"].values()
                    )
                    else "success"
                )
            except Exception as exc:  # noqa: BLE001
                report.update(status="failed", processing_error=str(exc))
        else:
            report["status"] = "failed"
        report["free_after"] = shutil.disk_usage(root).free
        pipeline.write_report(report_path, report)
        reports.append(report)
        print(
            json.dumps(
                {
                    "cycle": cycle,
                    "status": report["status"],
                    "report": str(report_path),
                    "identities": report["identity_changes"],
                }
            ),
            flush=True,
        )
        if (
            report["status"] == "failed"
            or report["free_after"] < c["minimum_free_bytes"]
        ):
            break
        if cycle < args.cycles and not args.ingest_only:
            time.sleep(c["between_cycles_seconds"])
    pipeline.write_report(
        root / "collection-ledger.json",
        {"cycles": reports, "live": True, "configuration": c},
    )
    return 1 if any(r["status"] == "failed" for r in reports) else 0


if __name__ == "__main__":
    raise SystemExit(main())
