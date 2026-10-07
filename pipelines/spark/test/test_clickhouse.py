"""Serving boundary validation and publication failures; real reload is in smoke."""

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "pipelines/spark/src"))

import httpx
from dsp_spark.io.clickhouse import (
    ClickHouseClient,
    replace_serving,
    serving_types,
    validate_source_schema,
    validated_rows,
)
from dsp_spark.processing.clean.contracts import spark_schema
from dsp_spark.runtime.clickhouse import ClickHouseSettings
from jsonschema import ValidationError
from pyspark.sql import types as T


def row(**changes):
    value = {
        "schema_version": "1",
        "snapshot_date": "2026-08-13",
        "window_start_date": "2026-07-15",
        "window_end_date": "2026-08-13",
        "window_days": 30,
        "location_id": "vn-hanoi",
        "location_name": "Hà Nội",
        "social_post_count": 1,
        "social_like_count": 2,
        "social_reply_count": 3,
        "social_repost_count": 0,
        "social_quote_count": 0,
        "social_engagement_total": 5,
        "venue_count": None,
        "known_court_count": None,
        "venues_with_known_court_count": None,
        "venues_without_known_court_count": None,
        "court_count_coverage_ratio": None,
        "has_social_signal": True,
        "has_supply_signal": False,
        "social_activity_per_venue": None,
        "social_engagement_per_venue": None,
        "gold_generated_at": "2026-10-06T04:18:12.123456Z",
    }
    value.update(changes)
    return value


class ClickHouseTests(unittest.TestCase):
    def test_exact_mapping_nulls_zero_and_large_integers(self):
        source = row(social_post_count=2**53 + 1)
        self.assertEqual(validated_rows([source]), [source])
        self.assertEqual(len(serving_types()), 23)
        self.assertEqual(serving_types()["known_court_count"], "Nullable(Int64)")
        self.assertEqual(
            serving_types()["social_activity_per_venue"], "Nullable(Float64)"
        )
        self.assertEqual(serving_types()["gold_generated_at"], "DateTime64(6, 'UTC')")
        known_zero = row(known_court_count=0)
        self.assertEqual(validated_rows([known_zero])[0]["known_court_count"], 0)

    def test_source_schema_rejects_casts_and_extra_missing_fields(self):
        schema = spark_schema("location_market_signal_snapshot")
        validate_source_schema(schema)
        validate_source_schema(
            T.StructType([T.StructField(f.name, f.dataType, True) for f in schema])
        )
        for incompatible in (
            T.StructType(schema.fields[:-1]),
            T.StructType([*schema.fields, T.StructField("extra", T.StringType())]),
            T.StructType(
                [
                    (
                        T.StructField("window_days", T.StringType())
                        if f.name == "window_days"
                        else f
                    )
                    for f in schema
                ]
            ),
        ):
            with self.assertRaisesRegex(ValueError, "Incompatible Gold"):
                validate_source_schema(incompatible)

    def test_duplicate_identity_and_mixed_windows_rejected(self):
        with self.assertRaisesRegex(ValueError, "Duplicate Gold"):
            validated_rows([row(), row()])
        with self.assertRaisesRegex(ValueError, "one current snapshot"):
            validated_rows(
                [
                    row(),
                    row(
                        location_id="vn-hue",
                        window_days=1,
                        window_start_date="2026-08-13",
                    ),
                ]
            )
        with self.assertRaisesRegex(ValueError, "metadata disagree"):
            validated_rows([row(window_days=7)])
        self.assertEqual(validated_rows([]), [])

    def test_invalid_contract_values_fail_before_clickhouse(self):
        for changes in (
            {"social_post_count": -1},
            {"social_post_count": 2**63},
            {"social_activity_per_venue": float("nan")},
            {"social_activity_per_venue": float("inf")},
            {"has_supply_signal": 1},
            {"snapshot_date": "2026-02-30"},
            {"location_id": "raw place"},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                validated_rows([row(**changes)])
        with self.assertRaisesRegex(ValueError, "Date32 range"):
            validated_rows([row(window_start_date="1800-07-15")])

    def client(self):
        client = Mock()
        client.settings = ClickHouseSettings()
        client.query.return_value = [{"engine": "Atomic"}]
        return client

    def test_insert_failure_never_publishes_and_cleans_stage(self):
        client = self.client()

        def execute(sql, **_kwargs):
            if sql.startswith("INSERT"):
                raise RuntimeError("insert failed")

        client.execute.side_effect = execute
        with self.assertRaisesRegex(RuntimeError, "insert failed"):
            replace_serving(client, [row()])
        statements = [c.args[0] for c in client.execute.call_args_list]
        self.assertFalse(any(s.startswith("EXCHANGE") for s in statements))
        self.assertTrue(statements[-1].startswith("DROP TABLE IF EXISTS"))

    def test_roundtrip_mismatch_never_publishes(self):
        client = self.client()
        client.read_rows.return_value = [row(known_court_count=0)]
        with self.assertRaisesRegex(ValueError, "staged rows differ"):
            replace_serving(client, [row()])
        self.assertFalse(
            any(c.args[0].startswith("EXCHANGE") for c in client.execute.call_args_list)
        )

    def test_repeated_reload_uses_new_private_stage_and_full_exchange(self):
        client = self.client()
        client.read_rows.return_value = [row()]
        first = replace_serving(client, [row()])
        second = replace_serving(client, [row()])
        self.assertEqual(first, second)
        exchanges = [
            c.args[0]
            for c in client.execute.call_args_list
            if c.args[0].startswith("EXCHANGE")
        ]
        self.assertEqual(len(exchanges), 2)
        self.assertNotEqual(exchanges[0], exchanges[1])
        payloads = [
            c.kwargs["data"]
            for c in client.execute.call_args_list
            if c.args[0].startswith("INSERT")
        ]
        self.assertEqual(payloads[0], payloads[1])
        self.assertEqual(json.loads(payloads[0]), row())

    def test_empty_state_publishes_without_insert(self):
        client = self.client()
        client.read_rows.return_value = []
        self.assertEqual(replace_serving(client, [])["loaded_rows"], 0)
        statements = [c.args[0] for c in client.execute.call_args_list]
        self.assertTrue(any(s.startswith("EXCHANGE") for s in statements))
        self.assertFalse(any(s.startswith("INSERT") for s in statements))

    def test_http_auth_parameters_null_and_error_handling(self):
        with ClickHouseClient(ClickHouseSettings(password="fixture-only")) as client:

            def handle(request):
                self.assertEqual(
                    request.headers["authorization"],
                    "Basic ZHNwMzkxbTpmaXh0dXJlLW9ubHk=",
                )
                self.assertEqual(
                    request.url.params["input_format_null_as_default"], "0"
                )
                self.assertEqual(request.url.params["wait_end_of_query"], "1")
                return httpx.Response(200, text='{"known_court_count":null}\n')

            client.http.close()
            client.http = httpx.Client(
                base_url="http://localhost:8123",
                auth=("dsp391m", "fixture-only"),
                transport=httpx.MockTransport(handle),
            )
            self.assertEqual(
                client.query("SELECT known_court_count"), [{"known_court_count": None}]
            )
            client.http.close()
            client.http = httpx.Client(
                base_url="http://localhost:8123",
                transport=httpx.MockTransport(
                    lambda _r: httpx.Response(400, text="schema error")
                ),
            )
            with self.assertRaisesRegex(RuntimeError, "HTTP 400: schema error"):
                client.execute("invalid SQL")
