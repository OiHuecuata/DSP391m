"""Actual local Spark/Delta tests; neither Kafka nor AWS is required."""

import copy
import json
import sys
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "pipelines/spark/src"), str(ROOT / "apps/collector/src")]

from dsp_collector.normalizers.social_post import normalize_social_post
from dsp_collector.publishers.kafka import build_social_post_event
from dsp_collector.sources.crawler.threads.vietnam_terms import QuerySpec, adapt_post
from dsp_spark.io.delta import merge_delta, read_delta, write_delta
from dsp_spark.jobs.batch.build_court_supply_by_location import (
    build_court_supply_by_location,
)
from dsp_spark.jobs.batch.build_location_market_signal_snapshot import (
    build_location_market_signal_snapshot,
)
from dsp_spark.jobs.batch.build_social_activity_daily import build_social_activity_daily
from dsp_spark.jobs.batch.ingest_bronze import ingest_bronze
from dsp_spark.jobs.batch.ingest_pbplus import ingest_pbplus
from dsp_spark.jobs.streaming.social_kafka_to_delta import (
    initialize_social_silver,
    migrate_social_schema,
    persist_social_batch,
    split_social_events,
    start_social_stream,
)
from dsp_spark.processing.clean.contracts import spark_schema
from dsp_spark.runtime.config import LakehousePaths, MarketSignalSettings, SparkSettings
from dsp_spark.runtime.spark import create_spark
from jsonschema import ValidationError
from pyspark.sql import types as T
from test_contracts import AT, court, social

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


class DeltaIntegrationTests(unittest.TestCase):
    def test_social_empty_initialization_needs_no_kafka_and_preserves_rows(self):
        initialize_social_silver(self.spark, self.settings)
        self.assertEqual(
            read_delta(self.spark, self.settings.paths.social_posts).count(), 0
        )
        persist_social_batch(
            self.kafka_frame([build_social_post_event(self.youtube_record())]),
            0,
            settings=self.settings,
        )
        initialize_social_silver(self.spark, self.settings)
        self.assertEqual(
            read_delta(self.spark, self.settings.paths.social_posts).count(), 1
        )

    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(
            SparkSettings(LakehousePaths("file:///tmp/dsp-spark-runtime-test"))
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="dsp-delta-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.settings = SparkSettings(LakehousePaths((self.root / "lake").as_uri()))

    def artifact(self, value, name="input.json"):
        path = self.root / name
        path.write_text(
            json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        return path

    def bronze(self, source, run_id="bronze-a"):
        return {
            "schema_version": "1",
            "source": source,
            "run_id": run_id,
            "seed_query": "pickleball",
            "sort": "directory" if source == "pbplus" else "recent",
            "collected_at": AT,
            "count": 1,
            "query_runs": [],
            "posts": [
                {
                    "id": "123",
                    "unknown_raw": {"nested": ["Hà Nội", None, {"media": "unchanged"}]},
                }
            ],
            "extra_envelope": {"preserved": True},
        }

    def batch(self, run_id="run-a", records=None):
        records = [court(run_id)] if records is None else records
        return {
            "schema_version": "1",
            "source": "pbplus",
            "run_id": run_id,
            "count": len(records),
            "records": records,
        }

    def kafka_frame(self, events, *, key=None):
        rows = []
        for index, event in enumerate(events):
            raw = (
                event
                if isinstance(event, str)
                else json.dumps(event, ensure_ascii=False)
            )
            record_key = (
                key
                if key is not None
                else (event["record_id"] if isinstance(event, dict) else "invalid")
            )
            rows.append(
                (
                    record_key.encode("utf-8"),
                    raw.encode("utf-8"),
                    self.settings.social_post_topic,
                    1,
                    index,
                    datetime(2026, 10, 5, tzinfo=UTC),
                )
            )
        return self.spark.createDataFrame(rows, KAFKA_SCHEMA)

    def youtube_record(self, run_id="youtube-a", comment=False):
        from dsp_collector.normalizers.youtube import normalize_youtube

        payload = {
            "id": "comment_1" if comment else "video_00001",
            "snippet": {
                "title": "Sân Hà Nội",
                "description": "pickleball",
                "textOriginal": "Sân Hà Nội",
                "publishedAt": AT,
                "likeCount": 3,
            },
            "statistics": {"viewCount": "100", "likeCount": "5", "commentCount": "2"},
        }
        return normalize_youtube(
            payload,
            run_id=run_id,
            collected_at=AT,
            video_id="video_00001" if comment else None,
        )

    def test_youtube_stream_observations_and_exact_replay(self):
        events = [
            build_social_post_event(self.youtube_record()),
            build_social_post_event(self.youtube_record(comment=True)),
        ]
        frame = self.kafka_frame(events)
        persist_social_batch(frame, 0, settings=self.settings)
        persist_social_batch(frame, 1, settings=self.settings)
        self.assertEqual(
            read_delta(self.spark, self.settings.paths.social_posts).count(), 2
        )
        another = build_social_post_event(self.youtube_record(run_id="youtube-b"))
        persist_social_batch(self.kafka_frame([another]), 2, settings=self.settings)
        stored = read_delta(self.spark, self.settings.paths.social_posts)
        self.assertEqual(stored.count(), 3)
        self.assertEqual(stored.select("record_id").distinct().count(), 2)

    def test_explicit_nullable_social_migration_preserves_old_rows(self):
        event = build_social_post_event(social())
        old, _ = split_social_events(self.kafka_frame([event]))
        old = old.drop("view_count", "parent_record_id", "video_duration", "tags")
        old.write.format("delta").save(self.settings.paths.social_posts)
        migrate_social_schema(self.spark, self.settings)
        stored = read_delta(self.spark, self.settings.paths.social_posts)
        self.assertEqual(stored.count(), 1)
        self.assertIsNone(stored.first().view_count)
        persist_social_batch(
            self.kafka_frame([build_social_post_event(self.youtube_record())]),
            1,
            settings=self.settings,
        )
        self.assertEqual(
            read_delta(self.spark, self.settings.paths.social_posts).count(), 2
        )

    def test_youtube_bronze_replay_is_immutable(self):
        envelope = {
            "schema_version": "1",
            "source": "youtube",
            "run_id": "youtube-a",
            "collected_at": AT,
            "videos": [],
            "comments": [],
            "responses": [],
            "query_runs": [],
            "video_queries": {},
            "requests": {},
            "errors": [],
        }
        path = self.artifact(envelope)
        ingest_bronze(self.spark, path, self.settings, source="youtube")
        ingest_bronze(self.spark, path, self.settings, source="youtube")
        rows = read_delta(self.spark, self.settings.paths.youtube_bronze).collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].payload_json, path.read_text())

    def test_source_gold_never_adds_views_to_threads_engagement(self):
        from dsp_spark.processing.aggregate.social_activity_daily import (
            social_activity_daily,
            social_source_activity_daily,
        )
        from dsp_spark.processing.clean.gold import validated_gold

        threads_record = {
            **social(),
            "published_at": AT.replace("Z", ".000000Z"),
            "detected_locations": ["Hà Nội"],
        }
        events = [
            build_social_post_event(threads_record),
            build_social_post_event(self.youtube_record()),
            build_social_post_event(self.youtube_record(comment=True)),
        ]
        valid, _ = split_social_events(self.kafka_frame(events))
        daily = validated_gold(
            social_source_activity_daily(valid, generated_at=AT),
            "social_source_activity_daily",
        )
        rows = {r.source: r for r in daily.filter("location_name = 'Hà Nội'").collect()}
        self.assertEqual(rows["youtube"].youtube_views, 100)
        self.assertEqual(rows["youtube"].youtube_likes, 5)
        self.assertIsNone(rows["youtube"].threads_engagement)
        self.assertEqual(rows["youtube_comment"].youtube_comment_likes, 3)
        self.assertIsNone(rows["youtube_comment"].youtube_views)
        threads = social_activity_daily(valid, generated_at=AT)
        self.assertEqual(threads.select("post_count").first()[0], 1)

    def test_runtime_and_delta_extensions(self):
        self.assertEqual(self.spark.version, "4.0.2")
        self.assertEqual(
            self.spark.conf.get("spark.sql.catalog.spark_catalog"),
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        self.assertIn(
            "DeltaSparkSessionExtension", self.spark.conf.get("spark.sql.extensions")
        )
        self.assertEqual(self.spark.range(3).count(), 3)

    def test_delta_write_read_and_nullable_unicode(self):
        schema = T.StructType(
            [T.StructField("id", T.LongType()), T.StructField("text", T.StringType())]
        )
        path = (self.root / "generic").as_uri()
        write_delta(
            self.spark.createDataFrame([(1, "Hà Nội 🇻🇳"), (2, None)], schema), path
        )
        rows = read_delta(self.spark, path).orderBy("id").collect()
        self.assertEqual(
            [(row.id, row.text) for row in rows], [(1, "Hà Nội 🇻🇳"), (2, None)]
        )

    def test_threads_bronze_preserves_complete_exact_json_and_replay(self):
        path = self.artifact(self.bronze("threads"))
        # Preserve actual CRLF, whitespace and property ordering too.
        path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))
        original = path.read_bytes()
        for _ in range(2):
            ingest_bronze(self.spark, path, self.settings, source="threads")
        rows = read_delta(self.spark, self.settings.paths.threads_bronze).collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].payload_json.encode("utf-8"), original)
        self.assertEqual(rows[0].run_id, "bronze-a")
        self.assertEqual(path.read_bytes(), original)

    def test_pbplus_bronze_persists_source_and_different_runs(self):
        for run_id in ("a", "b"):
            path = self.artifact(self.bronze("pbplus", run_id), run_id + ".json")
            ingest_bronze(self.spark, path, self.settings, source="pbplus")
        rows = read_delta(self.spark, self.settings.paths.pbplus_bronze).collect()
        self.assertEqual({row.run_id for row in rows}, {"a", "b"})
        self.assertTrue(all(row.source == "pbplus" for row in rows))

    def test_bronze_conflicting_run_payload_fails_without_overwriting_original(self):
        original = self.bronze("threads")
        path = self.artifact(original, "original.json")
        ingest_bronze(self.spark, path, self.settings, source="threads")
        changed = copy.deepcopy(original)
        changed["posts"][0]["text"] = "different synthetic source content"
        conflict = self.artifact(changed, "conflict.json")
        with self.assertRaisesRegex(ValueError, "Bronze run/envelope verification"):
            ingest_bronze(self.spark, conflict, self.settings, source="threads")
        rows = read_delta(self.spark, self.settings.paths.threads_bronze).collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].payload_json.encode(), path.read_bytes())
        self.assertEqual(rows[0].artifact_uri, path.as_uri())

    def test_bronze_identical_bytes_at_another_uri_preserve_original_provenance(self):
        first = self.artifact(self.bronze("threads"), "first.json")
        ingest_bronze(self.spark, first, self.settings, source="threads")
        second = self.root / "copied.json"
        second.write_bytes(first.read_bytes())
        ingest_bronze(self.spark, second, self.settings, source="threads")
        rows = read_delta(self.spark, self.settings.paths.threads_bronze).collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].artifact_uri, first.as_uri())
        self.assertEqual(rows[0].payload_json.encode(), second.read_bytes())

    def test_bronze_missing_commit_readback_cannot_report_ingestion_success(self):
        from dsp_spark.jobs.batch.ingest_bronze import BRONZE_SCHEMA

        path = self.artifact(self.bronze("threads"))
        empty = self.spark.createDataFrame([], BRONZE_SCHEMA)
        with (
            patch("dsp_spark.jobs.batch.ingest_bronze.merge_delta"),
            patch("dsp_spark.jobs.batch.ingest_bronze.read_delta", return_value=empty),
            self.assertRaisesRegex(ValueError, "Bronze run/envelope verification"),
        ):
            ingest_bronze(self.spark, path, self.settings, source="threads")
        self.assertFalse((self.root / "lake/bronze/threads").exists())

    def test_bronze_duplicate_persisted_run_is_not_acknowledged(self):
        path = self.artifact(self.bronze("threads"))
        ingest_bronze(self.spark, path, self.settings, source="threads")
        rows = read_delta(self.spark, self.settings.paths.threads_bronze).collect()
        duplicate = self.spark.createDataFrame(
            rows, read_delta(self.spark, self.settings.paths.threads_bronze).schema
        )
        write_delta(duplicate, self.settings.paths.threads_bronze)
        with self.assertRaisesRegex(ValueError, "Bronze run/envelope verification"):
            ingest_bronze(self.spark, path, self.settings, source="threads")
        self.assertEqual(
            read_delta(self.spark, self.settings.paths.threads_bronze).count(), 2
        )

    def test_invalid_bronze_rejected_before_write(self):
        for change in (
            {"count": 99},
            {"source": "pbplus"},
            {"collected_at": "no-time"},
        ):
            path = self.artifact({**self.bronze("threads"), **change})
            with self.assertRaises((ValueError, ValidationError)):
                ingest_bronze(self.spark, path, self.settings, source="threads")
        self.assertFalse((self.root / "lake/bronze/threads").exists())

    def test_pbplus_silver_preserved_and_exact_replay_idempotent(self):
        record = court()
        path = self.artifact(self.batch(records=[record]))
        original = path.read_bytes()
        for _ in range(2):
            ingest_pbplus(self.spark, path, self.settings)
        rows = read_delta(self.spark, self.settings.paths.courts).collect()
        self.assertEqual(len(rows), 1)
        for key, value in record.items():
            self.assertEqual(rows[0][key], value)
        self.assertIsNone(rows[0].latitude)
        self.assertIsNotNone(rows[0].lakehouse_ingested_at)
        self.assertEqual(path.read_bytes(), original)

    def test_pbplus_same_entity_different_run_preserved(self):
        for run in ("a", "b"):
            ingest_pbplus(
                self.spark, self.artifact(self.batch(run), run + ".json"), self.settings
            )
        rows = read_delta(self.spark, self.settings.paths.courts).collect()
        self.assertEqual(len(rows), 2)
        self.assertEqual(len({row.record_id for row in rows}), 1)
        self.assertEqual({row.run_id for row in rows}, {"a", "b"})

    def test_pbplus_invalid_record_fails_entire_batch_before_write(self):
        record = court()
        invalid = {**record, "latitude": 91}
        with self.assertRaises(ValidationError):
            ingest_pbplus(
                self.spark,
                self.artifact(self.batch(records=[record, invalid])),
                self.settings,
            )
        self.assertFalse((self.root / "lake/silver/courts").exists())

    def test_pbplus_lineage_conflict_rejected(self):
        with self.assertRaisesRegex(ValueError, "lineage"):
            ingest_pbplus(
                self.spark,
                self.artifact(self.batch(records=[court("other")])),
                self.settings,
            )

    def test_pbplus_empty_initialization_rerun_and_later_observations(self):
        empty = self.artifact(self.batch(records=[]), "empty.json")
        for _ in range(2):
            result = ingest_pbplus(self.spark, empty, self.settings)
            self.assertEqual(result["input_count"], 0)
            frame = read_delta(self.spark.newSession(), self.settings.paths.courts)
            self.assertEqual(frame.count(), 0)
            expected = [
                *spark_schema("court").fields,
                T.StructField("lakehouse_ingested_at", T.TimestampType()),
                T.StructField("source_artifact_uri", T.StringType()),
            ]
            self.assertEqual(
                [(f.name, f.dataType) for f in frame.schema],
                [(f.name, f.dataType) for f in expected],
            )
        self.assertEqual(
            build_court_supply_by_location(self.spark, self.settings)["gold_rows"], 0
        )
        nonempty = self.artifact(self.batch(), "nonempty.json")
        ingest_pbplus(self.spark, nonempty, self.settings)
        before = read_delta(self.spark, self.settings.paths.courts).collect()
        ingest_pbplus(self.spark, nonempty, self.settings)
        ingest_pbplus(self.spark, empty, self.settings)
        self.assertEqual(
            read_delta(self.spark, self.settings.paths.courts).collect(), before
        )

    def test_social_empty_initialization_gold_and_later_observations(self):
        empty = self.kafka_frame([])
        expected, _ = split_social_events(empty)
        for batch_id in range(2):
            persist_social_batch(empty, batch_id, settings=self.settings)
            frame = read_delta(
                self.spark.newSession(), self.settings.paths.social_posts
            )
            self.assertEqual(frame.count(), 0)
            self.assertEqual(
                [(f.name, f.dataType) for f in frame.schema],
                [(f.name, f.dataType) for f in expected.schema],
            )
        self.assertEqual(
            build_social_activity_daily(self.spark, self.settings)["gold_rows"], 0
        )
        ingest_pbplus(self.spark, self.artifact(self.batch(records=[])), self.settings)
        build_court_supply_by_location(self.spark, self.settings)
        with self.assertRaisesRegex(ValueError, "Empty social Gold requires"):
            build_location_market_signal_snapshot(self.spark, self.settings)
        result = build_location_market_signal_snapshot(
            self.spark,
            self.settings,
            market_settings=MarketSignalSettings(end_date="2026-10-05"),
        )
        self.assertEqual(result["output_locations"], 0)
        self.assertEqual(result["snapshot_date"], "2026-10-05")
        self.assertEqual(
            read_delta(
                self.spark, self.settings.paths.location_market_signal_snapshot
            ).count(),
            0,
        )
        event = build_social_post_event(social())
        persist_social_batch(self.kafka_frame([event]), 2, settings=self.settings)
        before = read_delta(self.spark, self.settings.paths.social_posts).collect()
        persist_social_batch(self.kafka_frame([event]), 2, settings=self.settings)
        persist_social_batch(empty, 3, settings=self.settings)
        self.assertEqual(
            read_delta(self.spark, self.settings.paths.social_posts).collect(), before
        )

    def test_social_stream_without_microbatch_initializes_silver(self):
        directory = self.root / "empty-stream"
        directory.mkdir()
        stream = self.spark.readStream.schema(KAFKA_SCHEMA).json(str(directory))
        # Replace only transport: the real streaming startup, checkpoint and
        # Delta initialization execute even when no foreachBatch is invoked.
        with patch(
            "dsp_spark.jobs.streaming.social_kafka_to_delta.read_social_stream",
            return_value=stream,
        ):
            query = start_social_stream(self.spark, self.settings, available_now=True)
            try:
                self.assertTrue(query.awaitTermination(30))
                self.assertEqual(
                    sum(p["numInputRows"] for p in query.recentProgress), 0
                )
            finally:
                if query.isActive:
                    query.stop()
        self.assertEqual(
            read_delta(self.spark, self.settings.paths.social_posts).count(), 0
        )

    def test_crawler_mentions_through_delta_preserve_unresolved_quality(self):
        raw = {
            "id": "123",
            "text": "Tìm sân pickleball Việt Nam tại Hà Nội, Cầu Giấy, Bắc Từ Liêm, Nam Từ Liêm, Thủ Đức",
            "taken_at": datetime(2026, 10, 5, tzinfo=UTC).timestamp(),
        }
        adapted = adapt_post(
            raw,
            query_spec=QuerySpec(
                "pickleball Vietnam", "pickleball", "vietnam", "Vietnam", 0, "country"
            ),
        )
        self.assertIsNotNone(adapted)
        record = normalize_social_post(adapted, run_id="geography", collected_at=AT)
        unsupported = {"Vietnam", "Cầu Giấy", "Bắc Từ Liêm", "Nam Từ Liêm", "Thủ Đức"}
        self.assertEqual(set(record["detected_locations"]), unsupported | {"Hà Nội"})
        event = build_social_post_event(record)
        ingest_pbplus(self.spark, self.artifact(self.batch(records=[])), self.settings)
        build_court_supply_by_location(self.spark, self.settings)
        previous = None
        for attempt in range(2):
            persist_social_batch(
                self.kafka_frame([event]), attempt, settings=self.settings
            )
            build_social_activity_daily(self.spark, self.settings)
            daily = read_delta(
                self.spark, self.settings.paths.social_activity_daily
            ).collect()
            self.assertEqual(len(daily), 6)
            self.assertEqual(
                {r.location_name for r in daily if r.location_id is None}, unsupported
            )
            self.assertEqual(
                read_delta(self.spark, self.settings.paths.social_posts).count(), 1
            )
            result = build_location_market_signal_snapshot(self.spark, self.settings)
            self.assertEqual(result["unresolved_social_location_rows"], 5)
            self.assertEqual(result["unresolved_social_location_rows_in_window"], 5)
            row = (
                read_delta(
                    self.spark, self.settings.paths.location_market_signal_snapshot
                )
                .collect()[0]
                .asDict()
            )
            self.assertEqual(row["location_id"], "vn-hanoi")
            self.assertEqual(row["social_post_count"], 1)
            self.assertTrue(row["has_social_signal"])
            self.assertFalse(row["has_supply_signal"])
            self.assertIsNone(row["venue_count"])
            metrics = {k: v for k, v in row.items() if k != "gold_generated_at"}
            if previous is not None:
                self.assertEqual(metrics, previous)
            previous = metrics

    def test_event_payload_event_id_and_kafka_metadata_preserved(self):
        event = build_social_post_event(social())
        valid, rejected = split_social_events(self.kafka_frame([event]))
        self.assertEqual(rejected.count(), 0)
        row = valid.collect()[0]
        self.assertEqual(row.event_id, event["event_id"])
        self.assertEqual(row.emitted_at, event["emitted_at"])
        self.assertEqual(row.kafka_partition, 1)
        self.assertEqual(row.kafka_offset, 0)
        self.assertEqual(row.kafka_topic, self.settings.social_post_topic)
        self.assertIsNotNone(row.kafka_timestamp)
        for key, value in event["payload"].items():
            self.assertEqual(row[key], value)

    def test_invalid_events_preserved_in_rejected_delta(self):
        event = build_social_post_event(social())
        no_field = copy.deepcopy(event)
        del no_field["payload"]["text"]
        events = [
            "{invalid",
            {**event, "event_type": "raw"},
            no_field,
            {**event, "event_id": "missing"},
        ]
        frame = self.kafka_frame(events)
        persist_social_batch(frame, 0, settings=self.settings)
        persist_social_batch(frame, 0, settings=self.settings)
        rows = read_delta(self.spark, self.settings.paths.rejected_social).collect()
        self.assertEqual(len(rows), 4)
        self.assertTrue(
            all(row.raw_value and row.reason and row.rejected_at for row in rows)
        )
        self.assertEqual({row.kafka_offset for row in rows}, set(range(4)))
        self.assertEqual(
            read_delta(self.spark, self.settings.paths.social_posts).count(), 0
        )

    def test_kafka_key_conflict_rejected(self):
        valid, rejected = split_social_events(
            self.kafka_frame([build_social_post_event(social())], key="wrong")
        )
        self.assertEqual(valid.count(), 0)
        self.assertIn("key conflicts", rejected.collect()[0].reason)

    def test_event_replay_same_id_single_row_different_run_separate(self):
        a, b = build_social_post_event(social("a")), build_social_post_event(
            social("b")
        )
        persist_social_batch(self.kafka_frame([a, a]), 0, settings=self.settings)
        persist_social_batch(self.kafka_frame([a, b]), 1, settings=self.settings)
        rows = read_delta(self.spark, self.settings.paths.social_posts).collect()
        self.assertEqual(len(rows), 2)
        self.assertEqual({row.event_id for row in rows}, {a["event_id"], b["event_id"]})
        self.assertEqual(len({row.record_id for row in rows}), 1)

    def test_generic_merge_insert_only_and_schema_drift_rejected(self):
        schema = T.StructType(
            [T.StructField("id", T.LongType()), T.StructField("value", T.StringType())]
        )
        path = (self.root / "generic").as_uri()
        merge_delta(
            self.spark.createDataFrame([(1, "original"), (1, "original")], schema),
            path,
            keys=("id",),
        )
        merge_delta(
            self.spark.createDataFrame([(1, "replay"), (2, "new")], schema),
            path,
            keys=("id",),
        )
        self.assertEqual(
            [
                (r.id, r.value)
                for r in read_delta(self.spark, path).orderBy("id").collect()
            ],
            [(1, "original"), (2, "new")],
        )
        with self.assertRaisesRegex(ValueError, "schema mismatch"):
            merge_delta(
                self.spark.createDataFrame([(3, 42)], "id long, value long"),
                path,
                keys=("id",),
            )

    def test_null_merge_key_rejected(self):
        with self.assertRaisesRegex(ValueError, "nonnull"):
            merge_delta(
                self.spark.createDataFrame([(None,)], "id long"),
                (self.root / "bad").as_uri(),
                keys=("id",),
            )

    def test_table_reopened_in_new_spark_session(self):
        path = (self.root / "reopen").as_uri()
        write_delta(self.spark.createDataFrame([(1,)], "id long"), path)
        self.assertEqual(read_delta(self.spark.newSession(), path).count(), 1)

    def test_empty_merge_still_rejects_existing_schema_drift(self):
        path = (self.root / "schema-drift").as_uri()
        merge_delta(
            self.spark.createDataFrame([], "id long"),
            path,
            keys=("id",),
            initialize_empty=True,
        )
        with self.assertRaisesRegex(ValueError, "schema mismatch"):
            merge_delta(
                self.spark.createDataFrame([], "id string"),
                path,
                keys=("id",),
                initialize_empty=True,
            )


if __name__ == "__main__":
    unittest.main()
