"""Fast structural tests: no JVM, Kafka or AWS required."""

import copy
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "pipelines/spark/src"), str(ROOT / "apps/collector/src")]

from dsp_collector.normalizers.court import normalize_court
from dsp_collector.normalizers.social_post import normalize_social_post
from dsp_collector.publishers.kafka import build_social_post_event
from dsp_spark.io.s3 import s3a_options, storage_uri
from dsp_spark.processing.clean.contracts import (
    CONTRACT_FILES,
    contract_documents,
    spark_schema,
    validate,
    validation_reason,
)
from dsp_spark.runtime.config import LakehousePaths, SparkSettings
from dsp_spark.runtime.spark import packages
from pyspark.sql import types as T

AT = "2026-10-05T00:00:00Z"


def social(run_id="run-a"):
    return normalize_social_post(
        {"id": "123", "text": "Tìm sân pickleball Hà Nội 🇻🇳"},
        run_id=run_id,
        collected_at=AT,
    )


def court(run_id="run-a"):
    return normalize_court(
        {
            "id": "https://pickleballplus.vn/san-tap/test",
            "name": "Sân Hà Nội",
            "number_of_courts": 4,
        },
        run_id=run_id,
        collected_at=AT,
    )


class ContractTests(unittest.TestCase):
    def test_all_fields_types_and_nullability_align_recursively(self):
        documents = json.loads(contract_documents())

        def resolve(node, kind):
            while "$ref" in node:
                ref = node["$ref"]
                if ref.startswith("#/"):
                    node = documents[kind]
                    for part in ref[2:].split("/"):
                        node = node[part]
                else:
                    self.assertEqual(ref, "../data/silver/social-post.v1.schema.json")
                    node, kind = documents["social"], "social"
            return node, kind

        def aligned(node, data_type, kind):
            node, kind = resolve(node, kind)
            kinds = node["type"] if isinstance(node["type"], list) else [node["type"]]
            concrete = next(value for value in kinds if value != "null")
            expected = {
                "object": T.StructType,
                "array": T.ArrayType,
                "string": T.StringType,
                "integer": T.LongType,
                "number": T.DoubleType,
                "boolean": T.BooleanType,
            }[concrete]
            self.assertIsInstance(data_type, expected)
            if concrete == "object":
                self.assertEqual(data_type.fieldNames(), list(node["properties"]))
                for field in data_type:
                    child, child_kind = resolve(node["properties"][field.name], kind)
                    self.assertEqual(
                        field.nullable,
                        "null" in child["type"] or field.name not in node["required"],
                    )
                    aligned(child, field.dataType, child_kind)
            if concrete == "array":
                child, child_kind = resolve(node["items"], kind)
                self.assertEqual(data_type.containsNull, "null" in child["type"])
                aligned(child, data_type.elementType, child_kind)

        for kind in (
            "social",
            "court",
            "event",
            "social_activity_daily",
            "court_supply_by_location",
            "location_reference",
        ):
            with self.subTest(kind=kind):
                aligned(documents[kind], spark_schema(kind), kind)

    def test_valid_event_preserves_canonical_payload(self):
        record = social()
        event = build_social_post_event(record)
        before = copy.deepcopy(event)
        validate(event, "event")
        self.assertEqual(event, before)
        self.assertEqual(event["payload"], record)

    def test_invalid_json_and_duplicate_properties_rejected(self):
        for raw in ("{invalid", "null", '{"event_id":1,"event_id":2}', "NaN"):
            self.assertIsNotNone(validation_reason(raw, "event", contract_documents()))

    def test_malformed_event_contract_and_lineage_rejected(self):
        event = build_social_post_event(social())
        variants = []
        for field, value in (
            ("event_type", "raw"),
            ("schema_version", "2"),
            ("event_id", "sha256:" + "0" * 64),
            ("run_id", "other"),
        ):
            variants.append({**event, field: value})
        no_id = copy.deepcopy(event)
        del no_id["event_id"]
        variants.append(no_id)
        no_text = copy.deepcopy(event)
        del no_text["payload"]["text"]
        variants.append(no_text)
        variants.append({**event, "payload": {"raw_payload": {}}})
        for value in variants:
            self.assertIsNotNone(
                validation_reason(json.dumps(value), "event", contract_documents())
            )

    def test_numeric_overflow_and_coercion_rejected(self):
        for value in (2**63, 4.5, "4", True):
            record = {**social(), "like_count": value}
            self.assertIsNotNone(
                validation_reason(json.dumps(record), "social", contract_documents())
            )

    def test_timestamp_timezone_and_date_validation(self):
        for value in ("2026-10-05", "2026-10-05T00:00:00", "2026-13-05T00:00:00Z"):
            record = {**court(), "collected_at": value}
            self.assertIsNotNone(
                validation_reason(json.dumps(record), "court", contract_documents())
            )

    def test_workers_validate_with_serialized_contracts_offline(self):
        documents = contract_documents()
        event = json.dumps(build_social_post_event(social()))
        with (
            patch(
                "pathlib.Path.read_text",
                side_effect=AssertionError("worker read filesystem"),
            ),
            patch("socket.socket", side_effect=AssertionError("worker network")),
        ):
            self.assertIsNone(validation_reason(event, "event", documents))

    def test_all_contracts_are_loaded(self):
        self.assertEqual(set(json.loads(contract_documents())), set(CONTRACT_FILES))


class ConfigurationTests(unittest.TestCase):
    def test_local_and_s3_paths_derive_from_base(self):
        for base in ("file:///tmp/lake", "s3a://explicit-bucket/dsp391"):
            paths = LakehousePaths(base)
            self.assertEqual(paths.social_posts, base + "/silver/social_posts")
            self.assertEqual(paths.courts, base + "/silver/courts")
            self.assertEqual(
                paths.social_checkpoint,
                base + "/checkpoints/social_posts_kafka_to_delta",
            )
            self.assertEqual(paths.threads_bronze, base + "/bronze/threads")

    def test_uri_validation(self):
        self.assertTrue(storage_uri("/tmp/lake").startswith("file:///"))
        for value in (
            "s3://bucket/path",
            "s3a:///missing",
            "file://remote/tmp/path",
            "s3a://user@bucket/key",
            "s3a://bucket/../key",
            "s3a://bucket/key?secret=x",
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                storage_uri(value)

    def test_collision_paths_rejected(self):
        with self.assertRaises(ValueError):
            LakehousePaths(
                "file:///tmp/base",
                social_posts="file:///tmp/same",
                social_checkpoint="file:///tmp/same",
            )

    def test_s3_packages_only_when_required(self):
        local = SparkSettings(LakehousePaths("file:///tmp/base"))
        cloud = SparkSettings(LakehousePaths("s3a://bucket/base"))
        self.assertEqual(packages(local), ["io.delta:delta-spark_2.13:4.0.1"])
        self.assertIn(
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2",
            packages(local, kafka=True),
        )
        self.assertIn("org.apache.hadoop:hadoop-aws:3.4.1", packages(cloud))

    def test_standard_aws_provider_chain_no_embedded_credentials(self):
        options = s3a_options(region="ap-southeast-1")
        self.assertIn(
            "DefaultCredentialsProvider",
            options["spark.hadoop.fs.s3a.aws.credentials.provider"],
        )
        self.assertNotIn("spark.hadoop.fs.s3a.endpoint", options)
        self.assertFalse(
            any("access.key" in key or "secret.key" in key for key in options)
        )
        self.assertEqual(
            s3a_options(endpoint="http://localhost:9000", path_style=True)[
                "spark.hadoop.fs.s3a.connection.ssl.enabled"
            ],
            "false",
        )
        with self.assertRaises(ValueError):
            s3a_options(endpoint="https://user:secret@example.com")

    def test_environment_overrides(self):
        with patch.dict(
            "os.environ",
            {
                "DSP_LAKEHOUSE_BASE_URI": "file:///tmp/env-lake",
                "DSP_KAFKA_SOCIAL_POST_TOPIC": "custom-topic",
                "DSP_SPARK_SOCIAL_CHECKPOINT_PATH": "file:///tmp/checkpoint",
            },
            clear=True,
        ):
            settings = SparkSettings.from_env()
        self.assertEqual(settings.social_post_topic, "custom-topic")
        self.assertEqual(settings.paths.social_checkpoint, "file:///tmp/checkpoint")
        self.assertEqual(settings.paths.courts, "file:///tmp/env-lake/silver/courts")


if __name__ == "__main__":
    unittest.main()
