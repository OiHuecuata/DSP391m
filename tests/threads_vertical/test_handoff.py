import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dsp_collector.normalizers.observation import digest
from orchestration.local import threads_manifest, collection_worker
from orchestration.local.threads_vertical import target_config
from tests.threads_vertical.fixtures import POLICY


class HandoffTests(unittest.TestCase):
    def manifest(self):
        collection={"collection_run_id":"fixture","origin":"SYNTHETIC","collection_status":"PARTIAL","historical_window_status":"SYNTHETIC_FIXTURE","unique_content_count":1,"planned_queries":2,"successful_queries":1,"failed_queries":0,"requests_used":1,"artifact_count":1,"last_success_at":"2026-10-10T00:00:00.000000Z","query_terms":["pickleball"]}
        config={"lakehouse_base_uri":"file:///tmp/fixture-handoff/lake","kafka_bootstrap_servers":"fixture:9092","clickhouse":{"host":"127.0.0.1","port":18123,"database":"threads_fixture","user":"fixture","password":""}}
        return threads_manifest.build(inputs=[{"kind":"artifact","source_platform":"threads","collection_run_id":"fixture","artifact_uri":"file:///tmp/fixture-handoff/raw.json","content_sha256":"a"*64,"artifact_schema_version":"threads-fetch.v2","record_count":1,"policy_reference":POLICY["reference"]}],ingestion_refs=[{"kind":"kafka_ack","source_platform":"threads","collection_run_id":"fixture","broker_namespace":"fixture:9092","topic":"dsp.social.observations.v2","partition":0,"first_offset":1,"last_offset":1,"acknowledged_count":1}],
            acquisition={"outcome":"partial","accepted_records":1,"coverage":{"status":"PARTIALLY_COLLECTED"},"runs":[{"source_platform":"threads","collection_run_id":"fixture","outcome":"partial","accepted_records":1}]},
            spec={"analysis_end_date":"2026-10-10","window_days":30,"processing_contract_version":"threads-observation.v2","code_version":"fixture","configuration_hash":"b"*64,"reference_version":None,"feature_version":"fixture","policy_version":"fixture","tombstone_version":None},targets=target_config(config),created_at="2026-10-10T00:00:00.000000Z",metadata={"event_ids":["sha256:"+"c"*64],"collection":collection})

    def test_ack_keeps_collection_outcome_and_publication_scope(self):
        manifest=self.manifest()
        value=threads_manifest.ack(manifest,attempt="fixture-attempt",stage="research_publication",outcome="PARTIAL",generation="sha256:"+"e"*64)
        self.assertEqual(value["collection_outcome"],"partial")
        self.assertEqual(value["publication_generation_reference"],"sha256:"+"e"*64)

    def test_target_or_payload_tampering_rejects_manifest(self):
        manifest=self.manifest();manifest["targets"]["base_uri"]="file:///tmp/other"
        with self.assertRaises(ValueError):threads_manifest.validate(manifest)

    def test_threads_controller_never_invokes_legacy_analytics(self):
        report={"stages":{},"metrics":{}}
        result={"status":"PARTIAL","manifest_id":"fixture","handoff":{"artifact_uri":"file:///tmp/fixture"},"metrics":{"unique_posts":1}}
        with tempfile.TemporaryDirectory() as directory, patch("orchestration.local.threads_vertical.acquire",return_value=result) as acquire, patch.object(collection_worker,"_execute_locked") as legacy:
            value=collection_worker.execute("threads","fixture",Path(directory),{"threads_vertical":{"mode":"fixture"}},report,lambda _value:None)
            self.assertEqual(value,result);acquire.assert_called_once();legacy.assert_not_called()

    def test_unconfigured_threads_controller_does_not_fetch(self):
        with patch("orchestration.local.threads_vertical.acquire") as acquire:
            with self.assertRaises(PermissionError):collection_worker.execute("threads","fixture",Path("/tmp"),{},{},lambda _value:None)
            acquire.assert_not_called()


if __name__=="__main__":unittest.main()
