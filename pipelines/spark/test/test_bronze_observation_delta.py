"""Real Delta record tables, lifecycle evidence, backfill and mixed Kafka batches."""

import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from dsp_collector.normalizers.youtube import normalize_youtube
from dsp_collector.publishers.kafka import build_social_post_event
from dsp_collector.runtime.bronze import DATASETS, encode, extract_observations
from dsp_spark.io.delta import read_delta
from dsp_spark.jobs.batch.ingest_bronze import (
    BRONZE_SCHEMA,
    backfill_bronze,
    ingest_bronze,
)
from dsp_spark.jobs.streaming.social_kafka_to_delta import (
    migrate_social_comments,
    persist_social_batch,
    split_social_events,
)
from dsp_spark.runtime.config import LakehousePaths, SparkSettings
from dsp_spark.runtime.spark import create_spark
from pyspark.sql import types as T

AT = "2026-10-10T00:00:00Z"
KAFKA_SCHEMA = T.StructType(
    [
        T.StructField("key", T.BinaryType()),
        T.StructField("value", T.BinaryType()),
        T.StructField("topic", T.StringType()),
        T.StructField("partition", T.IntegerType()),
        T.StructField("offset", T.LongType()),
        T.StructField("timestamp", T.TimestampType()),
    ]
)


def youtube(run="yt1", at=AT):
    return {
        "schema_version": "1",
        "source": "youtube",
        "run_id": run,
        "collected_at": at,
        "query_runs": [],
        "responses": [],
        "videos": [
            {
                "id": "v1",
                "snippet": {"title": "pickleball Hà Nội", "publishedAt": AT},
                "statistics": {"viewCount": "0"},
            }
        ],
        "comments": [
            {
                "video_id": "v1",
                "payload": {
                    "id": "c1",
                    "snippet": {"textOriginal": "hello Hà Nội", "likeCount": 0},
                },
            },
            {
                "video_id": "v1",
                "payload": {
                    "id": "c2.reply",
                    "snippet": {"parentId": "c1", "textOriginal": "reply"},
                },
            },
        ],
        "video_queries": {},
        "requests": {},
        "errors": [],
    }


class BronzeDeltaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(
            SparkSettings(LakehousePaths("file:///tmp/dsp-bronze-schema-test"))
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        d = tempfile.TemporaryDirectory(prefix="dsp-bronze-record-test-")
        self.addCleanup(d.cleanup)
        self.root = Path(d.name)
        self.settings = SparkSettings(LakehousePaths((self.root / "lake").as_uri()))

    def ingest(self, raw):
        p = self.root / (raw["run_id"] + ".json")
        p.write_text(json.dumps(raw, ensure_ascii=False))
        return ingest_bronze(self.spark, p, self.settings, source=raw["source"])

    def table(self, name):
        return read_delta(self.spark, getattr(self.settings.paths, name))

    def kafka(self, events):
        return self.spark.createDataFrame(
            [
                (
                    str(e.get("record_id", e["event_id"])).encode(),
                    json.dumps(e).encode(),
                    "existing-topic",
                    0,
                    i,
                    datetime.now(UTC),
                )
                for i, e in enumerate(events)
            ],
            KAFKA_SCHEMA,
        )

    def test_six_tables_replay_changes_null_metrics_and_missing_search(self):
        a = youtube()
        self.ingest(a)
        self.ingest(a)
        self.assertEqual([self.table(n).count() for n in DATASETS], [0, 1, 2, 3, 1, 3])
        self.assertEqual(self.table("youtube_bronze").count(), 1)
        metric = (
            self.table("bronze_engagement_snapshots")
            .filter("source_record_id = 'v1'")
            .first()
        )
        self.assertEqual(metric.view_count, 0)
        self.assertIsNone(metric.like_count)
        reply = (
            self.table("bronze_social_comments")
            .filter("source_record_id = 'c2.reply'")
            .first()
        )
        self.assertEqual(reply.parent_comment_id, "c1")
        b = youtube("yt2", "2026-10-10T01:00:00Z")
        b["videos"][0]["snippet"]["title"] = "changed"
        self.ingest(b)
        self.assertEqual(self.table("bronze_engagement_snapshots").count(), 6)
        changes = (
            self.table("bronze_source_changes")
            .filter("change_type = 'changed'")
            .collect()
        )
        self.assertEqual(len(changes), 1)
        self.assertIsNotNone(changes[0].previous_payload_hash)
        c = youtube("empty", "2026-10-10T02:00:00Z")
        c.update(videos=[], comments=[])
        self.ingest(c)
        self.assertEqual(
            self.table("bronze_source_changes")
            .filter("change_type = 'deleted'")
            .count(),
            0,
        )
        self.assertEqual(self.table("bronze_collection_runs").count(), 3)

    def test_backfill_uses_saved_payload_when_original_artifact_is_missing(self):
        raw = youtube()
        row = (
            "1",
            "youtube",
            "yt1",
            AT,
            datetime.now(UTC),
            "file:///missing-original-artifact.json",
            json.dumps(raw),
        )
        self.spark.createDataFrame([row], BRONZE_SCHEMA).write.format("delta").save(
            self.settings.paths.youtube_bronze
        )
        before = self.table("youtube_bronze").collect()
        for _ in range(2):
            result = backfill_bronze(self.spark, self.settings, sources=("youtube",))
            self.assertEqual(len(result), 1)
        self.assertEqual(before, self.table("youtube_bronze").collect())
        self.assertEqual(self.table("bronze_social_comments").count(), 2)

    def test_record_bronze_to_silver_and_retention_dependency_plan(self):
        from dsp_spark.jobs.batch.bronze_retention import retention_plan
        from dsp_spark.jobs.batch.ingest_social_bronze import ingest_social_bronze

        self.ingest(youtube())
        for _ in range(2):
            result = ingest_social_bronze(self.spark, self.settings)
            self.assertEqual(result["accepted_observations"], 3)
            self.assertEqual(result["rejected_observations"], 0)
        self.assertEqual(self.table("social_posts").count(), 1)
        self.assertEqual(self.table("social_comments").count(), 2)
        self.assertIsNone(self.table("social_posts").first().kafka_offset)
        plan = retention_plan(self.spark, self.settings)
        self.assertEqual(plan["policy_registry_status"], "unreviewed")
        self.assertEqual(
            plan["tables"]["bronze_social_comments"]["expired_observations"], 0
        )
        self.assertIn("ClickHouse", plan["derived_dependencies"])
        reviewed = retention_plan(
            self.spark,
            self.settings,
            now=datetime(2026, 10, 12, tzinfo=UTC),
            policies={
                "youtube": {
                    "retention_policy_id": "reviewed-test-policy",
                    "storage_permission": True,
                    "retention_days": 1,
                }
            },
        )
        self.assertEqual(
            reviewed["tables"]["bronze_social_comments"]["expired_observations"], 2
        )
        self.assertEqual(self.table("bronze_social_comments").count(), 2)

    def test_court_record_adapter_preserves_venue_counts_and_unknown_zero(self):
        from dsp_spark.jobs.batch.ingest_pbplus import ingest_court_bronze

        raw = {
            "schema_version": "1",
            "source": "pbplus",
            "run_id": "courts",
            "seed_query": "pickleball",
            "sort": "directory",
            "collected_at": AT,
            "query_runs": [],
            "count": 3,
            "posts": [
                {
                    "id": "https://pickleballplus.vn/san-tap/one",
                    "court_name": "one",
                    "number_of_courts": None,
                },
                {
                    "id": "https://pickleballplus.vn/san-tap/two",
                    "court_name": "two",
                    "number_of_courts": 0,
                },
                {
                    "id": "https://pickleballplus.vn/san-tap/three",
                    "court_name": "three",
                    "number_of_courts": 6,
                },
            ],
        }
        self.ingest(raw)
        for _ in range(2):
            result = ingest_court_bronze(self.spark, self.settings)
            self.assertEqual(result["accepted_observations"], 3)
        self.assertEqual(self.table("courts").count(), 3)
        self.assertEqual(
            sorted(
                r.number_of_courts
                for r in self.table("courts").collect()
                if r.number_of_courts is not None
            ),
            [0, 6],
        )

    def test_mixed_kafka_source_observations_and_legacy_silver_are_idempotent(self):
        raw = youtube()
        datasets, _ = extract_observations(raw)
        events = [
            {
                "schema_version": "1",
                "event_type": "bronze.observed",
                "event_id": r["observation_id"],
                "dataset": n,
                "record_json": encode(r),
            }
            for n, rows in datasets.items()
            for r in rows
        ]
        for payload, parent in [
            (raw["videos"][0], None),
            (raw["comments"][0]["payload"], "v1"),
        ]:
            events.append(
                build_social_post_event(
                    normalize_youtube(
                        payload, run_id="yt1", collected_at=AT, video_id=parent
                    )
                )
            )
        frame = self.kafka(events)
        persist_social_batch(frame, 0, settings=self.settings)
        persist_social_batch(frame, 0, settings=self.settings)
        self.assertEqual(self.table("bronze_social_posts").count(), 1)
        stored = self.table("bronze_social_posts").first()
        self.assertIsNotNone(stored.ingested_at)
        self.assertEqual(
            json.loads(stored.ingestion_metadata)["kafka_topic"], "existing-topic"
        )
        self.assertEqual(self.table("bronze_social_comments").count(), 2)
        self.assertEqual(self.table("social_posts").count(), 1)
        self.assertEqual(self.table("social_comments").count(), 1)

    def test_threads_search_evidence_and_capture_times_survive_bronze_ingestion(self):
        captured = "2026-10-10T00:00:00Z"
        raw = {
            "schema_version": "1",
            "source": "threads",
            "run_id": "threads-discovery",
            "seed_query": "pickleball",
            "sort": "recent",
            "started_at": captured,
            "collected_at": "2026-10-10T01:00:00Z",
            "count": 1,
            "posts": [
                {
                    "id": "123",
                    "text": "Tìm sân pickleball Hà Nội",
                    "taken_at": 1791590400,
                    "like_count": 0,
                    "_observed_at": captured,
                }
            ],
            "query_runs": [
                {
                    "query": "pickleball Hà Nội",
                    "kind": "city",
                    "geo_name": "Hà Nội",
                    "status": "partial",
                    "raw_count": 2,
                    "accepted_count": 1,
                    "rejected_count": 1,
                }
            ],
            "discovery": {
                "planned_queries": 2,
                "attempted_queries": 1,
                "remaining_queries": 1,
                "has_more": True,
            },
            "collection_stopping_reason": "request_budget",
            "search_evidence": [
                {
                    "query_id": "query",
                    "source_run_id": "threads-discovery",
                    "observed_at": captured,
                    "records": [
                        {"id": "123", "text": "accepted"},
                        {"id": "456", "text": "source rejection retained"},
                    ],
                    "decisions": [
                        {
                            "source_record_id": "456",
                            "outcome": "rejected",
                            "reason": "missing_market_topic",
                        }
                    ],
                }
            ],
        }
        result = self.ingest(raw)
        self.assertEqual(result["status"], "partial")
        for _ in range(2):
            self.ingest(raw)
        post = self.table("bronze_social_posts").first()
        snapshot = self.table("bronze_engagement_snapshots").first()
        from pyspark.sql import functions as F

        actual = (
            self.table("bronze_social_posts")
            .select(F.date_format("collected_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
            .first()[0]
        )
        self.assertEqual(actual, captured)
        self.assertEqual(snapshot.observed_at, post.collected_at)
        self.assertEqual(snapshot.like_count, 0)
        self.assertEqual(self.table("bronze_social_posts").count(), 1)
        run = self.table("bronze_collection_runs").first()
        self.assertEqual(
            json.loads(run.raw_payload)["search_evidence"], raw["search_evidence"]
        )

    def test_failed_collection_run_recovery_keeps_one_run_and_delta_history(self):
        from delta.tables import DeltaTable
        from dsp_collector.runtime.bronze import extract_collection_status
        from dsp_spark.processing.clean.bronze import persist_observations

        status = {
            "source": "youtube",
            "run_id": "failed_then_recovered",
            "status": "FAILED",
            "started_at": AT,
            "completed_at": AT,
            "stage": "collecting",
            "error": "API unavailable",
            "metrics": {},
        }
        datasets, _ = extract_collection_status(status)
        persist_observations(self.spark, datasets, self.settings)
        version = (
            DeltaTable.forPath(self.spark, self.settings.paths.bronze_collection_runs)
            .history(1)
            .select("version")
            .first()[0]
        )
        recovered = youtube("failed_then_recovered", "2026-10-10T01:00:00Z")
        self.ingest(recovered)
        self.assertEqual(self.table("bronze_collection_runs").count(), 1)
        self.assertEqual(
            self.table("bronze_collection_runs").first().execution_status, "success"
        )
        persist_observations(self.spark, datasets, self.settings)
        self.assertEqual(
            self.table("bronze_collection_runs").first().execution_status, "success"
        )
        historical = (
            self.spark.read.format("delta")
            .option("versionAsOf", version)
            .load(self.settings.paths.bronze_collection_runs)
        )
        self.assertEqual(historical.first().execution_status, "failed")

    def test_explicit_source_deletion_preserves_observations_and_evidence(self):
        self.ingest(youtube())
        deletion = youtube("deleted", "2026-10-10T02:00:00Z")
        deletion.update(
            videos=[],
            comments=[],
            source_changes=[
                {
                    "source_record_id": "v1",
                    "record_type": "Video",
                    "change_type": "deleted",
                    "evidence": {
                        "verified_endpoint": "https://source.example/video/v1",
                        "http_status": 410,
                    },
                }
            ],
        )
        self.ingest(deletion)
        evidence = (
            self.table("bronze_source_changes")
            .filter("change_type = 'deleted'")
            .first()
        )
        self.assertEqual(evidence.source_record_id, "v1")
        self.assertEqual(json.loads(evidence.evidence_json)["http_status"], 410)
        self.assertEqual(self.table("bronze_social_posts").count(), 1)

    def test_malformed_bronze_kafka_event_has_offset_based_rejection_path(self):
        raw = youtube()
        datasets, _ = extract_observations(raw)
        r = datasets["bronze_social_posts"][0]
        event = {
            "schema_version": "1",
            "event_type": "bronze.observed",
            "event_id": r["observation_id"],
            "dataset": "bronze_social_posts",
            "record_json": encode({**r, "payload_hash": "wrong"}),
        }
        persist_social_batch(self.kafka([event]), 0, settings=self.settings)
        self.assertEqual(self.table("rejected_social").count(), 1)
        self.assertEqual(self.table("social_posts").count(), 0)

    def test_partial_source_records_remain_readable_with_rejection_metadata(self):
        raw = youtube()
        raw["videos"].append({"snippet": {"title": "no id"}})
        result = self.ingest(raw)
        self.assertEqual(result["status"], "partial")
        self.assertEqual(self.table("bronze_social_posts").count(), 1)
        self.assertEqual(self.table("rejected_bronze").count(), 1)
        self.assertEqual(
            self.table("bronze_collection_runs").first().records_rejected, 1
        )

    def test_historical_comments_move_safely_and_remain_available_in_delta_history(
        self,
    ):
        raw = youtube()
        event = build_social_post_event(
            normalize_youtube(
                raw["comments"][0]["payload"],
                run_id="yt1",
                collected_at=AT,
                video_id="v1",
            )
        )
        valid, _ = split_social_events(self.kafka([event]))
        valid.write.format("delta").save(self.settings.paths.social_posts)
        migrate_social_comments(self.spark, self.settings)
        migrate_social_comments(self.spark, self.settings)
        self.assertEqual(self.table("social_posts").count(), 0)
        self.assertEqual(self.table("social_comments").count(), 1)
        self.assertEqual(
            self.spark.read.format("delta")
            .option("versionAsOf", 0)
            .load(self.settings.paths.social_posts)
            .count(),
            1,
        )
