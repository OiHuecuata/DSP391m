"""Gold transformation and actual Delta rebuild tests, without Kafka or AWS."""

import copy
import sys
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "pipelines/spark/src"), str(ROOT / "apps/collector/src")]

from dsp_spark.io.delta import read_delta, replace_delta
from dsp_spark.jobs.batch.build_social_activity_daily import build_social_activity_daily
from dsp_spark.processing.aggregate.social_activity_daily import (
    latest_social_posts,
    social_activity_daily,
)
from dsp_spark.processing.clean.contracts import spark_schema, validate
from dsp_spark.runtime.config import LakehousePaths, SparkSettings
from dsp_spark.runtime.spark import create_spark
from pyspark.sql import types as T
from test_contracts import social

GENERATED_AT = "2026-10-05T12:00:00.000000Z"


class SocialActivityDailyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(
            SparkSettings(LakehousePaths("file:///tmp/dsp-gold-runtime-test"))
        )
        cls.schema = T.StructType(
            spark_schema("social").fields
            + [
                T.StructField("event_id", T.StringType()),
                T.StructField("emitted_at", T.StringType()),
                T.StructField("lakehouse_ingested_at", T.TimestampType()),
            ]
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="dsp-gold-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.settings = SparkSettings(LakehousePaths(self.root.as_uri()))

    def record(self, record_id="threads:123", **changes):
        row = {
            **social(),
            "record_id": record_id,
            "published_at": "2026-10-04T23:59:59.000000Z",
            "detected_locations": ["Hà Nội"],
            "like_count": 2,
            "reply_count": 3,
            "repost_count": 4,
            "quote_count": 5,
            "event_id": "event-a",
            "emitted_at": "2026-10-05T00:00:01.000000Z",
            "lakehouse_ingested_at": datetime(2026, 10, 5, tzinfo=UTC),
        }
        row.update(changes)
        return row

    def frame(self, rows):
        return self.spark.createDataFrame(rows, self.schema)

    def aggregate(self, rows):
        return social_activity_daily(self.frame(rows), generated_at=GENERATED_AT)

    def test_one_post_multiple_distinct_location_mentions(self):
        rows = self.aggregate(
            [self.record(detected_locations=["Hà Nội", "Đà Nẵng", "Hà Nội"])]
        ).collect()
        self.assertEqual({r.location_name for r in rows}, {"Hà Nội", "Đà Nẵng"})
        self.assertTrue(
            all(r.post_count == 1 and r.engagement_total == 14 for r in rows)
        )

    def test_exact_observation_replay_counts_once(self):
        record = self.record()
        row = self.aggregate([record, copy.deepcopy(record)]).collect()[0]
        self.assertEqual(row.post_count, 1)
        self.assertEqual(row.like_count, 2)

    def test_multiple_runs_use_newest_collection_not_latest_replay(self):
        old = self.record(
            run_id="old",
            collected_at="2026-10-03T00:00:00.000000Z",
            emitted_at="2026-10-06T00:00:00.000000Z",
            like_count=99,
            event_id="old-event",
        )
        new = self.record(
            run_id="new",
            collected_at="2026-10-05T00:00:00.000000Z",
            emitted_at="2026-10-05T00:00:00.000000Z",
            like_count=7,
            event_id="new-event",
        )
        row = self.aggregate([old, new]).collect()[0]
        self.assertEqual(row.post_count, 1)
        self.assertEqual(row.like_count, 7)

    def test_ordering_ties_use_emission_ingestion_then_event_id(self):
        rows = [
            self.record(event_id="a", like_count=1),
            self.record(event_id="z", like_count=8),
        ]
        for values in (rows, list(reversed(rows))):
            self.assertEqual(
                latest_social_posts(self.frame(values)).collect()[0].event_id, "z"
            )
        emitted = self.record(
            event_id="b", emitted_at="2026-10-05T00:00:02.000000Z", like_count=9
        )
        self.assertEqual(self.aggregate(rows + [emitted]).collect()[0].like_count, 9)
        ingested = self.record(
            event_id="c",
            lakehouse_ingested_at=datetime(2026, 10, 6, tzinfo=UTC),
            like_count=10,
        )
        self.assertEqual(self.aggregate(rows + [ingested]).collect()[0].like_count, 10)

    def test_different_entities_count_separately_and_engagement_sums(self):
        row = self.aggregate(
            [
                self.record(),
                self.record(
                    "threads:456",
                    like_count=10,
                    reply_count=None,
                    repost_count=0,
                    quote_count=1,
                ),
            ]
        ).collect()[0]
        self.assertEqual(row.post_count, 2)
        self.assertEqual(
            (row.like_count, row.reply_count, row.repost_count, row.quote_count),
            (12, 3, 4, 6),
        )
        self.assertEqual(row.engagement_total, 25)

    def test_utc_publication_date_not_collection_date(self):
        rows = self.aggregate(
            [
                self.record(),
                self.record("threads:456", published_at="2026-10-05T00:00:00.000000Z"),
            ]
        ).collect()
        self.assertEqual({r.activity_date for r in rows}, {"2026-10-04", "2026-10-05"})

    def test_utc_date_independent_of_spark_session_timezone(self):
        try:
            self.spark.conf.set("spark.sql.session.timeZone", "Asia/Bangkok")
            self.assertEqual(
                self.aggregate([self.record()]).collect()[0].activity_date, "2026-10-04"
            )
        finally:
            self.spark.conf.set("spark.sql.session.timeZone", "UTC")

    def test_null_publication_time_excluded_after_latest_selection(self):
        old = self.record(collected_at="2026-10-03T00:00:00.000000Z")
        newest = self.record(
            collected_at="2026-10-05T00:00:00.000000Z",
            published_at=None,
            event_id="new",
        )
        self.assertEqual(self.aggregate([old, newest]).count(), 0)

    def test_empty_locations_create_no_fake_location(self):
        self.assertEqual(
            self.aggregate([self.record(detected_locations=[])]).count(), 0
        )
        self.assertEqual(
            self.aggregate([self.record(detected_locations=[""])]).count(), 0
        )

    def test_empty_input_and_null_counters(self):
        self.assertEqual(self.aggregate([]).count(), 0)
        row = self.aggregate(
            [
                self.record(
                    like_count=None,
                    reply_count=None,
                    repost_count=None,
                    quote_count=None,
                )
            ]
        ).collect()[0]
        self.assertEqual(row.engagement_total, 0)
        self.assertEqual(row.post_count, 1)

    def test_first_last_publication_times_and_generation_metadata(self):
        row = self.aggregate(
            [
                self.record(published_at="2026-10-04T01:00:00.000000Z"),
                self.record("threads:456"),
            ]
        ).collect()[0]
        self.assertEqual(row.first_published_at, "2026-10-04T01:00:00.000000Z")
        self.assertEqual(row.last_published_at, "2026-10-04T23:59:59.000000Z")
        self.assertEqual(row.gold_generated_at, GENERATED_AT)
        self.assertEqual(row.schema_version, "1")

    def test_gold_rows_align_with_contract(self):
        frame = self.aggregate([self.record()])
        schema = spark_schema("social_activity_daily")
        self.assertEqual(set(frame.columns), set(schema.fieldNames()))
        for field in schema:
            self.assertEqual(frame.schema[field.name].dataType, field.dataType)
        for row in frame.collect():
            validate(row.asDict(), "social_activity_daily")
        self.assertNotIn("unique_post_count", frame.columns)
        self.assertNotIn("demand_score", frame.columns)

    def test_aliases_merge_before_counting_each_logical_post(self):
        rows = self.aggregate(
            [
                self.record(
                    detected_locations=["Sài Gòn", "TP HCM", "TP. Hồ Chí Minh"]
                ),
                self.record("threads:456", detected_locations=["HCMC"]),
            ]
        ).collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            (rows[0].location_id, rows[0].location_name),
            ("vn-ho-chi-minh-city", "TP. Hồ Chí Minh"),
        )
        self.assertEqual((rows[0].post_count, rows[0].engagement_total), (2, 28))

    def test_unknown_mentions_keep_original_name_and_contribution(self):
        rows = self.aggregate(
            [self.record(detected_locations=["Cầu Giấy", "Hà Nội"])]
        ).collect()
        mapped = {r.location_name: r for r in rows}
        self.assertIsNone(mapped["Cầu Giấy"].location_id)
        self.assertEqual(mapped["Cầu Giấy"].post_count, 1)
        self.assertEqual(mapped["Hà Nội"].location_id, "vn-hanoi")

    def test_exact_legacy_gold_schema_migrates_but_other_drift_fails(self):
        rows = [self.record()]
        replace_delta(self.frame(rows), self.settings.paths.social_posts)
        schema = spark_schema("social_activity_daily")
        legacy = self.aggregate(rows).select(
            *[f.name for f in schema if f.name != "location_id"]
        )
        replace_delta(legacy, self.settings.paths.social_activity_daily)
        build_social_activity_daily(self.spark, self.settings)
        self.assertEqual(
            read_delta(self.spark, self.settings.paths.social_activity_daily)
            .collect()[0]
            .location_id,
            "vn-hanoi",
        )
        alternate = SparkSettings(
            LakehousePaths(
                self.settings.paths.base,
                social_activity_daily=self.settings.paths.social_activity_daily
                + "_unexpected",
            )
        )
        replace_delta(legacy.drop("quote_count"), alternate.paths.social_activity_daily)
        with self.assertRaisesRegex(ValueError, "schema mismatch"):
            build_social_activity_daily(self.spark, alternate)

    def test_delta_rebuild_replay_updates_and_removes_stale_groups(self):
        old = self.record()
        replace_delta(self.frame([old]), self.settings.paths.social_posts)
        for _ in range(2):
            result = build_social_activity_daily(self.spark, self.settings)
            self.assertEqual(result["logical_posts_used"], 1)
            self.assertEqual(result["gold_rows"], 1)
        reopened = read_delta(
            self.spark.newSession(), self.settings.paths.social_activity_daily
        )
        self.assertEqual(reopened.count(), 1)
        self.assertEqual(reopened.collect()[0].like_count, 2)
        latest = self.record(
            run_id="new-run",
            event_id="new-event",
            collected_at="2026-10-06T00:00:00.000000Z",
            like_count=20,
        )
        replace_delta(self.frame([old, latest]), self.settings.paths.social_posts)
        build_social_activity_daily(self.spark, self.settings)
        self.assertEqual(
            read_delta(self.spark, self.settings.paths.social_activity_daily)
            .collect()[0]
            .like_count,
            20,
        )
        changed = {
            **latest,
            "detected_locations": ["Đà Nẵng"],
            "published_at": "2026-10-05T00:00:00.000000Z",
        }
        replace_delta(self.frame([old, changed]), self.settings.paths.social_posts)
        build_social_activity_daily(self.spark, self.settings)
        rows = read_delta(
            self.spark, self.settings.paths.social_activity_daily
        ).collect()
        self.assertEqual(
            [(r.activity_date, r.location_name) for r in rows],
            [("2026-10-05", "Đà Nẵng")],
        )
        replace_delta(
            self.frame([{**changed, "published_at": None}]),
            self.settings.paths.social_posts,
        )
        build_social_activity_daily(self.spark, self.settings)
        self.assertEqual(
            read_delta(self.spark, self.settings.paths.social_activity_daily).count(), 0
        )


if __name__ == "__main__":
    unittest.main()
