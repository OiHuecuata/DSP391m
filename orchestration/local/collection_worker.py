"""One backend-owned source cycle, serialized through local Delta and serving."""

import argparse
import fcntl
import json
import os
import sys
import time
import traceback
from dataclasses import asdict, replace
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [
    str(ROOT),
    str(ROOT / "apps/collector/src"),
    str(ROOT / "pipelines/spark/src"),
]
from orchestration.local import pipeline


def load_environment():
    from dotenv import dotenv_values

    values = dotenv_values(
        os.getenv("PIPELINE_ENV_FILE", ROOT / ".env"), interpolate=False
    )
    os.environ.update(
        {**{k: v for k, v in values.items() if v is not None}, **os.environ}
    )
    os.environ.update(
        PIPELINE_MODE="live",
        THREADS_REPLAY_PATH="",
        PBPLUS_REPLAY_PATH="",
        KAFKA_ENABLED="false",
    )
    os.environ.setdefault("JAVA_HOME", str(ROOT / ".local/tools/java17"))
    os.environ["PYTHONPATH"] = os.pathsep.join(sys.path[:3])


def cycle_status(collection_status, error_count):
    """Successful refresh cannot turn failed acquisition into partial success."""
    if collection_status == "failed":
        return "FAILED"
    return "PARTIAL" if collection_status == "partial" or error_count else "SUCCESS"


def execute(source, run_id, directory, policy, report, update, *, ownership=None):
    config = pipeline.resolve_config()
    with pipeline.lakehouse_lock(config, ownership=ownership):
        return _execute_locked(
            source, run_id, directory, policy, report, update, config=config
        )


def _execute_locked(source, run_id, directory, policy, report, update, *, config=None):
    from delta.tables import DeltaTable
    from dsp_collector.jobs.collect_courts import process_pbplus_envelope
    from dsp_collector.jobs.collect_social import process_threads_envelope
    from dsp_collector.jobs.collect_youtube import collect as youtube_collect
    from dsp_collector.jobs.collect_youtube import process_youtube_envelope
    from dsp_collector.publishers.kafka import (
        build_social_post_event,
        publish_social_batch,
    )
    from dsp_collector.runtime.status import CrawlerStatusTracker
    from dsp_collector.sources.api.youtube import YouTubeConfig
    from dsp_collector.sources.crawler.pbplus.crawler import crawl_pbplus_courts
    from dsp_collector.sources.crawler.threads.crawler import crawl_threads_posts
    from dsp_spark.io.clickhouse import (
        ClickHouseClient,
        migrate,
        read_gold_rows,
        replace_serving,
    )
    from dsp_spark.io.delta import read_delta
    from dsp_spark.io.source_activity_clickhouse import (
        read_gold as read_source_gold,
    )
    from dsp_spark.io.source_activity_clickhouse import (
        replace_source_serving,
    )
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
    from dsp_spark.jobs.streaming.social_kafka_to_delta import (
        initialize_social_silver,
        start_social_stream,
    )
    from dsp_spark.runtime.spark import create_spark
    from pyspark.sql import functions as F

    config = pipeline.resolve_config() if config is None else config

    def stage(name, operation):
        report["stage"] = name
        update({"stage": name})
        t = time.monotonic()
        value = operation()
        report["stages"][name] = {
            "seconds": time.monotonic() - t,
            "result": None if name == "spark_start" else value,
        }
        update({"stage": name, "stages": report["stages"]})
        return value

    class ProgressTracker(CrawlerStatusTracker):
        def start(self, **kwargs):
            super().start(**kwargs)
            self._status.run_id = run_id
            self._persist()

        def _persist(self):
            super()._persist()
            s = self.snapshot()
            update(
                {
                    "stage": "collecting",
                    "queries_processed": s["queries_processed"],
                    "query_plan_count": s["query_plan_count"],
                    "discovered": s["raw_count"],
                    "accepted": s["collected_count"],
                    "query": (s.get("current_query") or {}).get("query"),
                }
            )

    manifest = directory / "collection.json"
    if manifest.is_file():
        result = json.loads(manifest.read_text())
        result["reused_collection"] = True
    else:
        update({"stage": "collecting"})
        report["stage"] = "collecting"
        tracker = ProgressTracker(source, directory / "crawler.json")
        t = time.monotonic()
        saved_bronze = directory / "bronze" / source / f"{run_id}.json"
        saved_silver = (
            directory
            / "silver"
            / ("courts" if source == "pbplus" else "social_posts")
            / f"{run_id}.json"
        )
        replayed = saved_bronze.is_file()
        if replayed and saved_silver.is_file():
            batch = json.loads(saved_silver.read_text())
            from dsp_collector.jobs.collect_social import CollectionResult

            if batch["run_id"] != run_id or batch["source"] != source:
                raise ValueError("Saved batch identity mismatch")
            collected = CollectionResult(
                run_id,
                batch["status"],
                saved_bronze,
                saved_silver,
                batch["count"],
                batch["rejected_count"],
            )
        elif replayed:
            envelope = json.loads(saved_bronze.read_text())
            processors = {
                "threads": process_threads_envelope,
                "youtube": process_youtube_envelope,
                "pbplus": process_pbplus_envelope,
            }
            collected = processors[source](
                envelope, run_id=run_id, artifact_root=directory
            )
        elif source == "threads":
            envelope = crawl_threads_posts(status_tracker=tracker, **policy[source])
            collected = process_threads_envelope(
                envelope, run_id=run_id, artifact_root=directory
            )
        elif source == "pbplus":
            envelope = crawl_pbplus_courts(**policy[source])
            collected = process_pbplus_envelope(
                envelope, run_id=run_id, artifact_root=directory
            )
        else:
            collected = youtube_collect(
                os.getenv("YOUTUBE_API_KEY"),
                YouTubeConfig(**policy[source]),
                run_id=run_id,
                artifact_root=directory,
                progress=update,
            )
        if source == "threads" and not replayed:
            if collected.status == "success":
                tracker.succeed(
                    collected_count=collected.count, output_path=collected.silver_path
                )
            else:
                tracker.fail("Collection partially normalized; see saved batch")
        result = json.loads(json.dumps(asdict(collected), default=str))
        result["collection_seconds"] = time.monotonic() - t
        result["reused_collection"] = replayed
        pipeline.write_report(manifest, result)
    report["collection"] = result
    bronze = json.loads(Path(result["bronze_path"]).read_text())
    records = json.loads(Path(result["silver_path"]).read_text())["records"]
    errors = (
        len(bronze.get("errors", [])) + result["rejected_count"]
        if source == "youtube"
        else sum(q.get("status") == "error" for q in bronze.get("query_runs", []))
        + sum(p.get("detail_status") == "error" for p in bronze.get("posts", []))
        + result["rejected_count"]
    )
    report["metrics"] = {
        "accepted": len(records),
        "errors": errors,
        "discovered": bronze.get(
            "search_exposures",
            sum(q.get("raw_count", 0) or 0 for q in bronze.get("query_runs", [])),
        ),
        "videos": len(bronze.get("videos", [])),
        "comments": len(bronze.get("comments", [])),
        "queries": len(bronze.get("query_runs", [])),
        "requests": bronze.get("requests"),
        "api_requests_this_attempt": (
            0
            if result.get("reused_collection")
            else bronze.get("requests", {}).get("used", 0)
        ),
        "stopping_reason": bronze.get(
            "stopping_reason", bronze.get("collection_stopping_reason")
        ),
    }
    update({"stage": "collected", "metrics": report["metrics"]})
    spark = stage("spark_start", lambda: create_spark(config.spark, kafka=True))
    # SparkSession is not a wire result.
    report["stages"]["spark_start"]["result"] = None
    query = None
    try:
        table_path = (
            config.spark.paths.courts
            if source == "pbplus"
            else config.spark.paths.social_posts
        )
        before = (
            read_delta(spark, table_path)
            if DeltaTable.isDeltaTable(spark, table_path)
            else None
        )
        previous_ids = (
            {r[0] for r in before.select("record_id").distinct().collect()}
            if before is not None
            else set()
        )
        report["metrics"].update(
            new_identities=sum(r["record_id"] not in previous_ids for r in records),
            existing_identities=sum(r["record_id"] in previous_ids for r in records),
        )
        stage(
            "bronze",
            lambda: ingest_bronze(
                spark, result["bronze_path"], config.spark, source=source
            ),
        )
        if source == "pbplus":
            stage(
                "court_silver",
                lambda: ingest_pbplus(spark, result["silver_path"], config.spark),
            )
            if not DeltaTable.isDeltaTable(spark, config.spark.paths.social_posts):
                initialize_social_silver(spark, config.spark)
        else:
            pub = stage(
                "kafka",
                lambda: asdict(
                    publish_social_batch(result["silver_path"], settings=config.kafka)
                ),
            )
            report["metrics"].update(
                published=pub["published_count"], acknowledged=pub["published_count"]
            )
            if pub["status"] != "success":
                raise RuntimeError("kafka_delivery_failed")
            update({"stage": "spark_ingestion", "metrics": report["metrics"]})
            report["stage"] = "spark_ingestion"
            t = time.monotonic()
            query = start_social_stream(spark, config.spark, available_now=True)
            if not query.awaitTermination(config.stream_timeout):
                raise RuntimeError("spark_timeout")
            consumed = sum(p["numInputRows"] for p in query.recentProgress)
            report["metrics"]["consumed"] = consumed
            report["stages"]["spark_ingestion"] = {
                "seconds": time.monotonic() - t,
                "input_records": consumed,
            }
        actual = (
            read_delta(spark, table_path).filter(F.col("run_id") == run_id).collect()
        )
        actual_map = {r.record_id: r.asDict(recursive=True) for r in actual}
        if len(actual) != len(records) or any(
            any(actual_map.get(r["record_id"], {}).get(k) != v for k, v in r.items())
            for r in records
        ):
            raise RuntimeError("silver_verification_failed")
        if source != "pbplus" and any(
            actual_map[r["record_id"]]["event_id"]
            != build_social_post_event(r)["event_id"]
            for r in records
        ):
            raise RuntimeError("event_identity_mismatch")
        if not DeltaTable.isDeltaTable(spark, config.spark.paths.courts):
            empty_path = directory / "empty-courts.json"
            pipeline.write_report(
                empty_path,
                {
                    "schema_version": "1",
                    "source": "pbplus",
                    "run_id": run_id,
                    "count": 0,
                    "records": [],
                },
            )
            ingest_pbplus(spark, empty_path, config.spark)
        stage("social_gold", lambda: build_social_activity_daily(spark, config.spark))
        stage("court_gold", lambda: build_court_supply_by_location(spark, config.spark))
        market_settings = config.market
        if (
            market_settings.end_date is None
            and read_delta(spark, config.spark.paths.social_activity_daily).isEmpty()
        ):
            # A YouTube/PBPlus-only fresh lake has no Threads publication date.
            market_settings = replace(
                market_settings, end_date=datetime.now(UTC).date().isoformat()
            )
        stage(
            "market_gold",
            lambda: build_location_market_signal_snapshot(
                spark, config.spark, market_settings=market_settings
            ),
        )
        with ClickHouseClient(config.warehouse) as client:
            stage("clickhouse_migration", lambda: migrate(client))
            serving = read_gold_rows(
                spark, config.spark.paths.location_market_signal_snapshot
            )
            stage("clickhouse", lambda: replace_serving(client, serving))
            if client.read_rows() != serving:
                raise RuntimeError("clickhouse_verification_failed")
            source_rows = read_source_gold(
                spark, config.spark.paths.social_source_activity_daily
            )
            stage(
                "clickhouse_sources",
                lambda: replace_source_serving(client, source_rows),
            )
        rejected = (
            read_delta(spark, config.spark.paths.rejected_social).count()
            if DeltaTable.isDeltaTable(spark, config.spark.paths.rejected_social)
            else 0
        )
        report["metrics"].update(
            rejected=rejected,
            serving_rows=len(serving),
            source_serving_rows=len(source_rows),
            all_mapped_fields_match=True,
        )
        report["status"] = cycle_status(result["status"], errors)
        if report["status"] == "FAILED":
            report["error"] = (
                "Source acquisition failed; inspect saved source artifacts"
            )
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source", choices=("threads", "youtube", "pbplus"), required=True
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument(
        "--config", type=Path, default=ROOT / "configs/collection/data_ops.v1.json"
    )
    args = parser.parse_args(argv)
    load_environment()
    directory = args.state / "runs" / args.run_id
    directory.mkdir(parents=True, exist_ok=True)
    report = {
        "run_id": args.run_id,
        "source": args.source,
        "started_at": datetime.now(UTC).isoformat(),
        "status": "RUNNING",
        "stages": {},
        "metrics": {},
    }
    current_progress = {}

    def update(value):
        # Only controlled structured metrics enter the API; no log scraping.
        current_progress.update(value)
        pipeline.write_report(
            directory / "progress.json",
            {
                "run_id": args.run_id,
                "updated_at": datetime.now(UTC).isoformat(),
                **current_progress,
            },
        )

    with (args.state / "pipeline.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            policy = json.loads(args.config.read_text())
            import shutil

            if shutil.disk_usage(args.state).free < policy.get(
                "minimum_free_bytes", 2 * 1024**3
            ):
                raise RuntimeError("disk_capacity_guard")
            execute(args.source, args.run_id, directory, policy, report, update)
        except Exception as exc:  # noqa: BLE001 -- persist a safe terminal run result
            # Exception text can include request URLs/credentials from dependencies.
            report.update(
                status="FAILED",
                error=f"{type(exc).__name__}: {report.get('stage', 'cycle')} failed; inspect server-side artifacts",
            )
            detail = traceback.format_exc()
            for name, value in os.environ.items():
                if value and any(
                    word in name.upper()
                    for word in ("KEY", "SECRET", "TOKEN", "PASSWORD")
                ):
                    detail = detail.replace(value, "[REDACTED]")
            (directory / "error.txt").write_text(detail)
        finally:
            report["completed_at"] = datetime.now(UTC).isoformat()
            pipeline.write_report(directory / "summary.json", report)
            update(
                {
                    "stage": "finished",
                    "status": report["status"],
                    "metrics": report["metrics"],
                }
            )
    return 1 if report["status"] == "FAILED" else 0


if __name__ == "__main__":
    raise SystemExit(main())
