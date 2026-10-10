"""Shared registered schema and offline executor validation for native Bronze."""

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "pipelines/spark/src"), str(ROOT / "apps/collector/src")]

from dsp_collector.normalizers.observation import (
    prepare_envelope,
    project_envelope,
    run_envelope_context,
)
from dsp_spark.processing.clean.contracts import (
    contract_documents,
    spark_schema,
    validate,
    validation_reason,
)


class ObservationContractTests(unittest.TestCase):
    def row(self):
        envelope = json.loads(
            (
                ROOT / "tests/fixtures/pipeline/native-source-artifacts.v1.json"
            ).read_text()
        )["youtube"]
        context = run_envelope_context(
            permission_reference="fixture:synthetic", retention_policy_id="fixture"
        )
        return project_envelope(
            prepare_envelope(envelope, context),
            artifact_uri="file:///tmp/fixture.json",
            artifact_sha256="0" * 64,
        )[0]

    def test_registered_schema_matches_exact_authoritative_file(self):
        docs = json.loads(contract_documents())
        self.assertEqual(
            docs["observation"],
            json.loads(
                (
                    ROOT / "contracts/data/bronze/record-observation.v1.schema.json"
                ).read_text()
            ),
        )
        self.assertEqual(
            spark_schema("observation").names, docs["observation"]["required"]
        )
        self.assertEqual(
            spark_schema("observation")["view_count"].dataType.simpleString(), "bigint"
        )
        self.assertEqual(
            spark_schema("observation")["metric_availability"].dataType.simpleString(),
            "struct<view_count:string,like_count:string,comment_count:string,share_count:string,repost_count:string,quote_count:string,reply_count:string>",
        )

    def test_serialized_contract_semantics_and_no_external_schema_fetch(self):
        row = self.row()
        docs = contract_documents()
        validate(row, "observation", documents=docs)
        self.assertIsNone(validation_reason(json.dumps(row), "observation", docs))
        for name in row:
            changed = dict(row)
            changed.pop(name)
            with self.subTest(required=name):
                self.assertIsNotNone(
                    validation_reason(json.dumps(changed), "observation", docs)
                )
        self.assertIsNotNone(
            validation_reason(
                json.dumps({**row, "view_count": True}), "observation", docs
            )
        )
        self.assertIsNotNone(
            validation_reason(
                json.dumps({**row, "payload_hash": "f" * 64}), "observation", docs
            )
        )

    def test_unknown_properties_and_duplicate_or_nonfinite_raw_rejected(self):
        row = self.row()
        docs = contract_documents()
        for changed in [
            {**row, "extra": 1},
            {**row, "raw_payload": '{"id":"1","id":"2"}'},
            {**row, "raw_payload": '{"id":"1","x":Infinity}'},
        ]:
            self.assertIsNotNone(
                validation_reason(json.dumps(changed), "observation", docs)
            )


if __name__ == "__main__":
    unittest.main()
