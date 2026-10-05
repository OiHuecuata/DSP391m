"""Actual local Spark/Delta tests; neither Kafka nor AWS is required."""

import copy
import json
import sys
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "pipelines/spark/src"), str(ROOT / "apps/collector/src")]

from dsp_collector.publishers.kafka import build_social_post_event
from dsp_spark.io.delta import merge_delta, read_delta, write_delta
from dsp_spark.jobs.batch.ingest_bronze import ingest_bronze
from dsp_spark.jobs.batch.ingest_pbplus import ingest_pbplus
from dsp_spark.jobs.streaming.social_kafka_to_delta import (
    persist_social_batch,
    split_social_events,
)
from dsp_spark.runtime.config import LakehousePaths, SparkSettings
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

    def test_pbplus_empty_batch_creates_no_rows(self):
        result = ingest_pbplus(
            self.spark, self.artifact(self.batch(records=[])), self.settings
        )
        self.assertEqual(result["input_count"], 0)
        self.assertFalse((self.root / "lake/silver/courts").exists())

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
        self.assertFalse((self.root / "lake/silver/social_posts").exists())

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


if __name__ == "__main__":
    unittest.main()
