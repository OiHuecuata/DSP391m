"""Replay source-level Bronze into existing Silver contracts, without scraping.

This adapter supports historical rebuild and source-event-only Kafka ingestion.
Existing event IDs deduplicate observations already ingested through normalized
Kafka events. Transport metadata stays null for this direct batch path.
"""

import argparse
import json
from datetime import UTC, datetime, timedelta

from delta.tables import DeltaTable
from dsp_collector.normalizers.social_post import normalize_social_post
from dsp_collector.normalizers.youtube import normalize_youtube
from dsp_collector.publishers.kafka import build_social_post_event
from dsp_collector.runtime.bronze import timestamp
from dsp_collector.sources.crawler.threads.vietnam_terms import (
    _detect_locations,
    _detect_topics,
)
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ...io.delta import read_delta
from ...processing.clean.bronze import reject_bronze
from ...runtime.config import SparkSettings
from ...runtime.spark import create_spark
from ..streaming.social_kafka_to_delta import (
    migrate_social_comments,
    migrate_social_schema,
    persist_social_batch,
)

TRANSPORT_SCHEMA = T.StructType(
    [
        T.StructField("key", T.BinaryType()),
        T.StructField("value", T.BinaryType()),
        T.StructField("topic", T.StringType()),
        T.StructField("partition", T.IntegerType()),
        T.StructField("offset", T.LongType()),
        T.StructField("timestamp", T.TimestampType()),
    ]
)


def ingest_social_bronze(spark, settings, *, run_id=None):
    migrate_social_schema(spark, settings)
    migrate_social_comments(spark, settings)
    accepted = rejected = 0
    for dataset in ("bronze_social_posts", "bronze_social_comments"):
        path = getattr(settings.paths, dataset)
        if not DeltaTable.isDeltaTable(spark, path):
            continue
        records = read_delta(spark, path)
        if run_id:
            records = records.filter(records.collection_run_id == run_id)
        timestamp_fields = {
            field.name
            for field in records.schema
            if isinstance(field.dataType, T.TimestampType)
        }
        records = records.select(
            *(
                (
                    F.unix_micros(F.col(field.name)).alias(field.name)
                    if isinstance(field.dataType, T.TimestampType)
                    else F.col(field.name)
                )
                for field in records.schema
            )
        )
        batch = []
        for observation in records.toLocalIterator():
            row = observation.asDict()
            for field in timestamp_fields:
                row[field] = (
                    (
                        datetime(1970, 1, 1, tzinfo=UTC)
                        + timedelta(microseconds=row[field])
                    )
                    if row[field] is not None
                    else None
                )
            collected = timestamp(row["collected_at"])
            try:
                payload = json.loads(row["raw_payload"])
                queries = json.loads(row["query_metadata"])
                if row["source_platform"] == "youtube":
                    record = normalize_youtube(
                        payload,
                        run_id=row["collection_run_id"],
                        collected_at=collected,
                        queries=queries,
                        video_id=(
                            row.get("native_post_id")
                            if dataset == "bronze_social_comments"
                            else None
                        ),
                    )
                elif row["source_platform"] == "threads":
                    text = row.get("text_raw", row.get("comment_text_raw")) or ""
                    info = payload.get("text_post_app_info") or {}
                    publication = row["source_created_at"]
                    projected = {
                        **payload,
                        "id": row["source_record_id"],
                        "text": text,
                        "taken_at": (publication.timestamp() if publication else None),
                        "is_reply": dataset == "bronze_social_comments",
                        "url": row["source_url"],
                        "matched_queries": [
                            q["query"] if isinstance(q, dict) else q for q in queries
                        ],
                        "query_contexts": [q for q in queries if isinstance(q, dict)],
                        "detected_locations": _detect_locations(text),
                        "categories": _detect_topics(text)[0],
                    }
                    for key, native in (
                        ("reply_count", "direct_reply_count"),
                        ("repost_count", "repost_count"),
                        ("quote_count", "quote_count"),
                    ):
                        projected[key] = info.get(native, payload.get(key))
                    record = normalize_social_post(
                        projected,
                        run_id=row["collection_run_id"],
                        collected_at=collected,
                        normalized_at=collected,
                    )
                    if dataset == "bronze_social_comments":
                        record["parent_record_id"] = "threads:" + row["native_post_id"]
                else:
                    raise ValueError("no Silver adapter for source platform")
                event = build_social_post_event(record)
                batch.append(
                    (
                        record["record_id"].encode(),
                        json.dumps(event, ensure_ascii=False).encode(),
                        None,
                        None,
                        None,
                        None,
                    )
                )
                accepted += 1
            except (ValueError, TypeError, KeyError) as exc:
                reject_bronze(
                    spark,
                    settings,
                    row["source_platform"],
                    row["collection_run_id"],
                    row["artifact_uri"],
                    [
                        {
                            "observation_id": row["observation_id"],
                            "error": str(exc),
                            "stage": "silver_adapter",
                        }
                    ],
                )
                rejected += 1
            if len(batch) >= 500:
                persist_social_batch(
                    spark.createDataFrame(batch, TRANSPORT_SCHEMA), 0, settings=settings
                )
                batch = []
        if batch:
            persist_social_batch(
                spark.createDataFrame(batch, TRANSPORT_SCHEMA), 0, settings=settings
            )
    return {
        "accepted_observations": accepted,
        "rejected_observations": rejected,
        "status": (
            "partial" if rejected and accepted else "failed" if rejected else "success"
        ),
    }


def main(argv=None, *, spark=None, settings=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id")
    args = parser.parse_args(argv)
    settings = settings or SparkSettings.from_env()
    owned = spark is None
    spark = create_spark(settings) if owned else spark
    try:
        print(json.dumps(ingest_social_bronze(spark, settings, run_id=args.run_id)))
    finally:
        if owned:
            spark.stop()


if __name__ == "__main__":
    main()
