"""Native measure isolation and validated source serving publication."""

import copy
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from dsp_spark.io.clickhouse import serving_types
from dsp_spark.io.source_activity_clickhouse import (
    KIND,
    ordered,
    replace_source_serving,
)
from jsonschema import ValidationError


def row():
    return {
        "schema_version": "1",
        "activity_date": "2026-10-01",
        "location_id": "hanoi",
        "location_name": "Hà Nội",
        "source": "youtube",
        "record_count": 2,
        "threads_engagement": None,
        "youtube_views": 100,
        "youtube_likes": 5,
        "youtube_comment_count": None,
        "youtube_comment_likes": None,
        "gold_generated_at": "2026-10-08T00:00:00.000000Z",
    }


class Client:
    settings = SimpleNamespace(database="test")

    def __init__(self):
        self.commands = []

    def execute(self, sql, **kwargs):
        self.commands.append(sql)

    def query(self, sql, **kwargs):
        if "system.databases" in sql:
            return [{"engine": "Atomic"}]
        return [{"name": n, "type": t} for n, t in serving_types(KIND).items()]


class SourceServingTests(unittest.TestCase):
    def test_native_nullable_types_and_date_precision(self):
        types = serving_types(KIND)
        self.assertEqual(types["youtube_views"], "Nullable(Int64)")
        self.assertEqual(types["activity_date"], "Date32")
        self.assertEqual(types["gold_generated_at"], "DateTime64(6, 'UTC')")

    def test_incompatible_cross_source_measures_rejected(self):
        for changes in (
            {"threads_engagement": 100},
            {"source": "youtube_comment", "youtube_views": 100},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                ordered([{**row(), **changes}])

    def test_duplicate_groups_and_date_range_rejected(self):
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            ordered([row(), copy.deepcopy(row())])
        with self.assertRaisesRegex(ValueError, "range"):
            ordered([{**row(), "activity_date": "1800-01-01"}])

    def test_all_fields_checked_before_atomic_exchange(self):
        client = Client()
        with patch(
            "dsp_spark.io.source_activity_clickhouse.read_served", return_value=[row()]
        ):
            result = replace_source_serving(client, [row()])
        self.assertTrue(result["all_fields_match"])
        self.assertTrue(
            any(sql.startswith("EXCHANGE TABLES") for sql in client.commands)
        )

    def test_staging_mismatch_preserves_current_serving(self):
        client = Client()
        with (
            patch(
                "dsp_spark.io.source_activity_clickhouse.read_served",
                return_value=[{**row(), "youtube_views": 101}],
            ),
            self.assertRaisesRegex(ValueError, "mismatch"),
        ):
            replace_source_serving(client, [row()])
        self.assertFalse(
            any(sql.startswith("EXCHANGE TABLES") for sql in client.commands)
        )
        self.assertTrue(client.commands[-1].startswith("DROP TABLE IF EXISTS"))

    def test_empty_replacement_is_published(self):
        client = Client()
        with patch(
            "dsp_spark.io.source_activity_clickhouse.read_served", return_value=[]
        ):
            result = replace_source_serving(client, [])
        self.assertEqual(result["loaded_rows"], 0)
        self.assertFalse(any(sql.startswith("INSERT INTO") for sql in client.commands))
