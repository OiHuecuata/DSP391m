"""Read local Delta directly, measure live-data coverage, and export EDA snapshots."""

import argparse
import json
import os
import shutil
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [
    str(ROOT),
    str(ROOT / "apps/collector/src"),
    str(ROOT / "pipelines/spark/src"),
]
from collect_live_dataset import content

from orchestration.local import pipeline


def sizes(path):
    files = [p for p in path.rglob("*") if p.is_file()] if path.is_dir() else []
    return {
        "files": len(files),
        "bytes": sum(p.stat().st_size for p in files),
        "allocated_bytes": sum(p.stat().st_blocks * 512 for p in files),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", default=os.getenv("LIVE_EDA_OUTPUT", ".local/live-eda")
    )
    parser.add_argument("--export", action="store_true")
    args = parser.parse_args()
    root = Path(args.output).resolve()
    from delta.tables import DeltaTable
    from dsp_spark.io.delta import read_delta
    from dsp_spark.processing.aggregate.court_supply_by_location import latest_courts
    from dsp_spark.processing.aggregate.social_activity_daily import latest_social_posts
    from dsp_spark.runtime.config import SparkSettings
    from dsp_spark.runtime.spark import create_spark
    from pyspark.sql import functions as F

    settings = SparkSettings.from_env()
    spark = create_spark(settings)
    baseline = json.loads((root / "baseline.json").read_text())
    reports = [
        json.loads(p.read_text()) for p in sorted(root.glob("cycle-*/processing.json"))
    ]
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "baseline": baseline,
        "storage": {},
        "runs": [],
        "query_metrics": [],
        "quality": {},
        "exports": {},
    }
    frames = {}
    try:
        for name in settings.paths.__dataclass_fields__:
            if name in ("base", "social_checkpoint"):
                continue
            uri = getattr(settings.paths, name)
            path = Path(unquote(urlsplit(uri).path))
            row = {
                **sizes(path),
                "uri": uri,
                "rows": 0,
                "exists": DeltaTable.isDeltaTable(spark, uri),
            }
            if row["exists"]:
                frame = read_delta(spark, uri)
                frames[name] = frame
                row["rows"] = frame.count()
                detail = DeltaTable.forPath(spark, uri).detail().first()
                row["active_data_bytes"] = detail.sizeInBytes
            row["row_growth"] = row["rows"] - baseline["tables"].get(name, {}).get(
                "rows", 0
            )
            report["storage"][name] = row
        report["lakehouse_storage"] = sizes(
            Path(unquote(urlsplit(settings.paths.base).path))
        )
        report["disk_free_after"] = shutil.disk_usage(root).free
        state = {}
        for source, table, selector in (
            ("threads", "social_posts", latest_social_posts),
            ("pbplus", "courts", latest_courts),
        ):
            previous_runs = baseline["tables"][table].get("runs", [])
            previous = selector(
                frames[table].filter(F.col("run_id").isin(previous_runs))
            )
            state[source] = {
                r.record_id: r.asDict(recursive=True) for r in previous.collect()
            }
        rawbytes = 0
        normbytes = 0
        for run in reports:
            for source, result in run["collections"].items():
                bpath = Path(result["bronze_path"])
                spath = Path(result["silver_path"])
                bronze = json.loads(bpath.read_text())
                silver = json.loads(spath.read_text())
                rawbytes += bpath.stat().st_size
                normbytes += spath.stat().st_size
                rows = silver["records"]
                before = state[source]
                cols = (
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
                    and content(r, cols) == content(before[r["record_id"]], cols)
                    for r in rows
                )
                updated = len(rows) - new - unchanged
                if source == "threads":
                    discovered = sum(
                        q.get("raw_count", 0) for q in bronze["query_runs"]
                    )
                    failures = sum(
                        q.get("status") == "error" for q in bronze["query_runs"]
                    )
                    duplicates = sum(
                        q.get("duplicate_discovered_count", 0)
                        for q in bronze["query_runs"]
                    )
                    detail_failures = sum(
                        q.get("detail_failed_count", 0) for q in bronze["query_runs"]
                    )
                    detail_success = sum(
                        q.get("detail_success_count", 0) for q in bronze["query_runs"]
                    )
                    report["query_metrics"].extend(
                        [{"cycle": run["cycle"], **q} for q in bronze["query_runs"]]
                    )
                else:
                    q = bronze["query_runs"][0]
                    discovered = q.get("unique_links_discovered", q["raw_count"])
                    failures = q["failed_count"]
                    duplicates = q.get("duplicate_links", 0)
                    detail_failures = failures
                    detail_success = len(rows)
                report["runs"].append(
                    {
                        "cycle": run["cycle"],
                        "source": source,
                        "run_id": result["run_id"],
                        "requested": 1000,
                        "discovered": discovered,
                        "accepted": len(rows),
                        "rejected": result["rejected_count"],
                        "new_identities": new,
                        "existing_identities": len(rows) - new,
                        "unchanged_observations": unchanged,
                        "updated_observations": updated,
                        "duplicates_discovered": duplicates,
                        "failed": failures,
                        "detail_successes": detail_success,
                        "detail_failures": detail_failures,
                        "collection_seconds": result.get("collection_seconds"),
                        "raw_bytes": bpath.stat().st_size,
                        "normalized_bytes": spath.stat().st_size,
                        "bronze_path": str(bpath),
                        "silver_path": str(spath),
                    }
                )
                state[source].update({r["record_id"]: r for r in rows})
        report["raw_live_bytes"] = rawbytes
        report["normalized_live_bytes"] = normbytes
        for source, table, selector, columns in (
            (
                "threads",
                "social_posts",
                latest_social_posts,
                (
                    "text",
                    "published_at",
                    "author_id",
                    "author_username",
                    "like_count",
                    "reply_count",
                    "repost_count",
                    "quote_count",
                    "detail_status",
                ),
            ),
            (
                "pbplus",
                "courts",
                latest_courts,
                (
                    "court_name",
                    "number_of_courts",
                    "address",
                    "latitude",
                    "longitude",
                    "geo_status",
                ),
            ),
        ):
            all_frame = frames[table]
            campaign_runs = [
                r["run_id"] for r in report["runs"] if r["source"] == source
            ]
            verified_runs = baseline["tables"][table].get("runs", []) + campaign_runs
            frame = all_frame.filter(F.col("run_id").isin(verified_runs))
            latest = selector(frame)
            count = latest.count()
            nulls = (
                latest.agg(
                    *(
                        F.sum(F.when(F.col(k).isNull(), 1).otherwise(0)).alias(k)
                        for k in columns
                    )
                )
                .first()
                .asDict()
            )
            values = {
                "all_stored_observations": all_frame.count(),
                "excluded_unverified_observations": all_frame.count() - frame.count(),
                "observations": frame.count(),
                "unique_identities": count,
                "null_counts": nulls,
                "null_rates": {
                    k: v / count if count else None for k, v in nulls.items()
                },
                "observation_repeat_fraction": (
                    1 - count / frame.count() if frame.count() else None
                ),
            }
            keys = ["event_id"] if source == "threads" else ["run_id", "record_id"]
            values["incorrect_duplicate_keys"] = (
                frame.groupBy(*keys).count().filter(F.col("count") > 1).count()
            )
            if source == "threads":
                times = (
                    latest.agg(
                        F.min("published_at").alias("publication_min"),
                        F.max("published_at").alias("publication_max"),
                    )
                    .first()
                    .asDict()
                )
                values.update(times)
                cutoff = (datetime.now(UTC) - timedelta(days=90)).isoformat()
                known = latest.filter(F.col("published_at").isNotNull()).count()
                stale = latest.filter(F.col("published_at") < cutoff).count()
                values.update(
                    stale_threshold_days=90,
                    stale_posts=stale,
                    known_publication_dates=known,
                    stale_rate=stale / known if known else None,
                    empty_location_mentions=latest.filter(
                        F.size("detected_locations") == 0
                    ).count(),
                    detail_errors=latest.filter(
                        F.col("detail_status") == "error"
                    ).count(),
                    mentioned_locations=[
                        r[0]
                        for r in latest.select(F.explode("detected_locations"))
                        .distinct()
                        .collect()
                    ],
                )
            else:
                values["known_zero_court_counts"] = latest.filter(
                    F.col("number_of_courts") == 0
                ).count()
            report["quality"][source] = values
            live_runs = campaign_runs
            report["quality"][source]["milestone_unique_live_identities"] = (
                frame.filter(F.col("run_id").isin(live_runs))
                .select("record_id")
                .distinct()
                .count()
            )
        report["quality"]["unresolved_court_locations"] = report["storage"][
            "unresolved_court_locations"
        ]["rows"]
        report["quality"]["unresolved_social_daily_rows"] = (
            frames["social_activity_daily"]
            .filter(F.col("location_id").isNull())
            .count()
        )
        report["gold_rows"] = {
            name: [r.asDict(recursive=True) for r in frame.collect()]
            for name, frame in frames.items()
            if name
            in (
                "social_activity_daily",
                "court_supply_by_location",
                "location_market_signal_snapshot",
            )
        }
        report["processing"] = [
            {
                "cycle": r["cycle"],
                "stages": r["stages"],
                "verification": r.get("verification"),
            }
            for r in reports
        ]
        if args.export:
            folder = root / "eda-parquet" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
            for name in (
                "social_posts",
                "courts",
                "social_activity_daily",
                "court_supply_by_location",
                "location_market_signal_snapshot",
            ):
                frame = frames[name]
                if name in ("social_posts", "courts"):
                    source = "threads" if name == "social_posts" else "pbplus"
                    run_ids = [
                        r["run_id"] for r in report["runs"] if r["source"] == source
                    ]
                    frame = frame.filter(F.col("run_id").isin(run_ids))
                target = folder / name
                frame.write.mode("errorifexists").parquet(str(target))
                report["exports"][name] = str(target)
    finally:
        spark.stop()
    pipeline.write_report(root / "dataset-report.json", report)
    print(
        json.dumps(
            {
                "report": str(root / "dataset-report.json"),
                "storage": report["lakehouse_storage"],
                "exports": report["exports"],
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
