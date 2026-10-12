"""Serving types and numeric JSON canonicalization regressions."""

import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from dsp_spark.io.clickhouse import serving_types
from dsp_spark.io.intelligence_clickhouse import canonical_served
from dsp_spark.jobs.batch.build_intelligence import assert_generation
from dsp_spark.processing.clean.contracts import contract_documents
from dsp_spark.processing.intelligence.common import digest


class IntelligenceServingTests(unittest.TestCase):
    def test_passed_retention_cutoff_blocks_publication_without_reading_tables(self):
        manifest = {
            "status": "VALIDATED",
            "policy_fingerprint": digest({}),
            "contract_fingerprint": digest(contract_documents()),
            "next_retention_cutoff": "2000-01-01T00:00:00Z",
        }
        with self.assertRaisesRegex(ValueError, "retention cutoff passed"):
            assert_generation(Mock(), Mock(), manifest)

    def test_changed_policy_blocks_publication_without_reading_tables(self):
        from unittest.mock import patch

        manifest = {"status": "VALIDATED", "policy_fingerprint": digest({})}
        with (
            patch(
                "dsp_spark.jobs.batch.build_intelligence.load_policies",
                return_value={"synthetic": {"storage_permission": False}},
            ),
            self.assertRaisesRegex(ValueError, "policies changed"),
        ):
            assert_generation(Mock(), Mock(), manifest)

    def test_array_null_and_temporal_types(self):
        types = serving_types("social_engagement_daily_v1")
        self.assertEqual(types["lineage_ids"], "Array(String)")
        self.assertEqual(types["total_likes"], "Nullable(Int64)")
        self.assertEqual(types["snapshot_as_of"], "Nullable(DateTime64(6, 'UTC'))")
        self.assertEqual(types["activity_date"], "Date32")
        self.assertEqual(
            serving_types("court_supply_by_admin_version_v2")["price_min_vnd"],
            "Nullable(Decimal(20, 2))",
        )

    def test_float64_whole_number_roundtrip_uses_declared_contract_type(self):
        from dsp_spark.processing.intelligence.catalog import contract

        spec = contract("social_engagement_daily_v1")
        row = {n: None for n in spec["properties"]}
        row["avg_likes_per_post"] = 10
        client = Mock(settings=SimpleNamespace(database="synthetic_test"))
        client.query.return_value = [row]
        actual = canonical_served(
            client, "synthetic_stage", "social_engagement_daily_v1"
        )
        self.assertIs(type(actual[0]["avg_likes_per_post"]), float)
        self.assertEqual(actual[0]["avg_likes_per_post"], 10.0)


if __name__ == "__main__":
    unittest.main()
