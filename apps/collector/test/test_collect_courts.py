"""Offline PBPlus collection, enrichment, replay, lineage and failure tests.

No real PBPlus sample exists. Fixtures follow crawl_pbplus_courts/scrape_court
and the optional fields written by geocode_payload.
"""

from __future__ import annotations

import copy
import io
import json
import re
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

COLLECTOR_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = COLLECTOR_ROOT.parents[1]
sys.path.insert(0, str(COLLECTOR_ROOT / "src"))

from dsp_collector.jobs import collect_courts as job
from dsp_collector.jobs import collect_social
from dsp_collector.normalizers.court import normalize_court
from dsp_collector.runtime import artifacts
from dsp_collector.runtime.config import default_artifact_root
from dsp_collector.runtime.status import CrawlerStatusTracker

COLLECTED_AT = "2026-10-05T11:02:10.383825+07:00"
GEOCODED_AT = "2026-10-05T04:03:00+00:00"
LIST_URL = "https://pickleballplus.vn/tim-kiem-san"


class CollectCourtsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bronze_schema = json.loads(
            (
                REPOSITORY_ROOT / "contracts/data/bronze/pbplus-crawl.v1.schema.json"
            ).read_text()
        )
        cls.silver_schema = json.loads(
            (REPOSITORY_ROOT / "contracts/data/silver/court.v1.schema.json").read_text()
        )

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        network = patch(
            "socket.socket", side_effect=AssertionError("unexpected network call")
        )
        network.start()
        self.addCleanup(network.stop)
        self.envelope = {
            "source": "pbplus",
            "seed_query": "pickleball courts",
            "sort": "directory",
            "collected_at": COLLECTED_AT,
            "count": 2,
            "query_runs": [
                {
                    "query": LIST_URL,
                    "kind": "directory",
                    "geo_name": "Vietnam",
                    "status": "ok",
                    "landed_url": LIST_URL,
                    "raw_count": 2,
                    "failed_count": 0,
                    "accepted_count": 2,
                }
            ],
            "posts": [
                {
                    "id": "https://pickleballplus.vn/san-tap/san-ha-noi",
                    "code": "san-ha-noi",
                    "url": "https://pickleballplus.vn/san-tap/san-ha-noi",
                    "has_media": True,
                    "court_name": "Sân pickleball Hà Nội 🇻🇳",
                    "number_of_courts": 4,
                },
                {
                    "id": "https://pickleballplus.vn/san-tap/san-da-nang",
                    "code": "san-da-nang",
                    "url": "https://pickleballplus.vn/san-tap/san-da-nang",
                    "has_media": False,
                    "court_name": None,
                    "number_of_courts": None,
                },
            ],
        }

    def process(self, **kwargs):
        options = {"run_id": "test-run", "artifact_root": self.root}
        options.update(kwargs)
        return job.process_pbplus_envelope(self.envelope, **options)

    def collect(self, **kwargs):
        options = {
            "crawler": Mock(return_value=self.envelope),
            "artifact_root": self.root,
        }
        options.update(kwargs)
        return job.collect_pbplus(**options)

    def read_artifacts(self, result):
        return (
            json.loads(result.bronze_path.read_text(encoding="utf-8")),
            json.loads(result.silver_path.read_text(encoding="utf-8")),
        )

    def supplied_geography(self):
        enriched = copy.deepcopy(self.envelope)
        enriched["geocoded_at"] = GEOCODED_AT
        enriched["posts"][0].update(
            address="Hà Nội, Vietnam",
            address_source="venue_page",
            latitude=21.0285,
            longitude=105.8542,
            geocode_status="ok",
            geocode_matched_address="Hà Nội, Vietnam",
            geocode_country="Vietnam",
            geocode_expected_region="ha noi",
        )
        enriched["posts"][1].update(
            address=None,
            address_source="court_name_fallback",
            latitude=None,
            longitude=None,
            geocode_status="missing_address",
        )
        return enriched

    def assert_contract(self, value, schema, node=None):
        """Check the JSON Schema constraints used here without new dependencies."""
        node = schema if node is None else node
        if "$ref" in node:
            node = schema["$defs"][node["$ref"].removeprefix("#/$defs/")]
        kinds = node["type"]
        kinds = kinds if isinstance(kinds, list) else [kinds]
        matches = {
            "null": value is None,
            "string": isinstance(value, str),
            "integer": type(value) is int,
            "number": type(value) in (int, float),
            "boolean": type(value) is bool,
            "array": isinstance(value, list),
            "object": isinstance(value, dict),
        }
        self.assertTrue(any(matches[kind] for kind in kinds), (value, kinds))
        if "const" in node:
            self.assertEqual(value, node["const"])
        if "enum" in node:
            self.assertIn(value, node["enum"])
        if value is None:
            return
        if isinstance(value, dict):
            self.assertTrue(set(node.get("required", [])).issubset(value))
            properties = node.get("properties", {})
            if node.get("additionalProperties") is False:
                self.assertTrue(set(value).issubset(properties))
            for key, child in value.items():
                if key in properties:
                    self.assert_contract(child, schema, properties[key])
        elif isinstance(value, list):
            for child in value:
                self.assert_contract(child, schema, node["items"])
        elif isinstance(value, str):
            self.assertGreaterEqual(len(value), node.get("minLength", 0))
            if "pattern" in node:
                self.assertIsNotNone(re.search(node["pattern"], value))
            if node.get("format") == "date-time":
                self.assertIsNotNone(datetime.fromisoformat(value).utcoffset())
        elif type(value) in (int, float) and "minimum" in node:
            self.assertGreaterEqual(value, node["minimum"])

    def test_minimal_envelope_writes_run_scoped_bronze_and_silver(self):
        result = self.process()
        bronze, silver = self.read_artifacts(result)
        self.assertEqual(result.status, "success")
        self.assertEqual((result.count, result.rejected_count), (2, 0))
        self.assertEqual(result.bronze_path, self.root / "bronze/pbplus/test-run.json")
        self.assertEqual(result.silver_path, self.root / "silver/courts/test-run.json")
        self.assertEqual(len(list(self.root.rglob("*.json"))), 2)
        self.assert_contract(bronze, self.bronze_schema)
        for record in silver["records"]:
            self.assert_contract(record, self.silver_schema)

    def test_bronze_preserves_source_fields_and_unknown_extensions(self):
        self.envelope["source_extension"] = {"version": 2}
        self.envelope["posts"][0].update(
            address="Địa chỉ sân",
            phone="0123456789",
            opening_hours=["06:00–22:00"],
            services=["Đỗ xe"],
            source_extension={"nested": [1, 2]},
        )
        bronze, silver = self.read_artifacts(self.process())
        for key, value in self.envelope.items():
            self.assertEqual(bronze[key], value)
        self.assertEqual(bronze["schema_version"], "1")
        self.assert_contract(bronze, self.bronze_schema)
        self.assertNotIn("phone", silver["records"][0])

    def test_run_id_shared_with_all_records_and_identity_independent_of_run(self):
        first = self.process(run_id="first")
        second = self.process(run_id="second")
        bronze, silver = self.read_artifacts(first)
        _, second_silver = self.read_artifacts(second)
        self.assertEqual(bronze["run_id"], first.run_id)
        self.assertEqual(silver["run_id"], first.run_id)
        self.assertTrue(all(r["run_id"] == first.run_id for r in silver["records"]))
        self.assertEqual(
            silver["records"][0]["record_id"], second_silver["records"][0]["record_id"]
        )

    def test_collection_time_preserved_and_passed_to_normalizer(self):
        bronze, silver = self.read_artifacts(self.process())
        self.assertEqual(bronze["collected_at"], COLLECTED_AT)
        self.assertEqual(silver["collected_at"], COLLECTED_AT)
        for record in silver["records"]:
            self.assertEqual(
                datetime.fromisoformat(record["collected_at"]),
                datetime.fromisoformat(COLLECTED_AT),
            )

    def test_non_geocoded_and_missing_count_are_valid_and_nullable(self):
        _, silver = self.read_artifacts(self.process())
        for record in silver["records"]:
            for field in (
                "address",
                "latitude",
                "longitude",
                "geo_status",
                "geocoded_at",
            ):
                self.assertIsNone(record[field])
        self.assertEqual(silver["records"][0]["number_of_courts"], 4)
        self.assertIsNone(silver["records"][1]["number_of_courts"])
        self.assertEqual(silver["enrichment_status_counts"], {"not_requested": 2})

    def test_supplied_geography_preserves_bronze_and_retains_failed_court(self):
        enriched = self.supplied_geography()
        before = copy.deepcopy(enriched)
        bronze, silver = self.read_artifacts(self.process(enriched_envelope=enriched))
        self.assertEqual(bronze["posts"], self.envelope["posts"])
        self.assertNotIn("geocoded_at", bronze)
        self.assertEqual(silver["status"], "success")
        self.assertEqual(silver["count"], 2)
        self.assertEqual(silver["rejected_count"], 0)
        self.assertEqual(silver["records"][0]["latitude"], 21.0285)
        self.assertEqual(silver["records"][0]["geo_status"], "ok")
        self.assertEqual(silver["records"][1]["geo_status"], "missing_address")
        self.assertIsNone(silver["records"][1]["longitude"])
        self.assertEqual(len(silver["enrichment_errors"]), 1)
        for record in silver["records"]:
            self.assertEqual(
                datetime.fromisoformat(record["geocoded_at"]),
                datetime.fromisoformat(GEOCODED_AT),
            )
            self.assert_contract(record, self.silver_schema)
        self.assertEqual(enriched, before)

    def test_all_reported_geo_failures_retain_courts(self):
        for index, outcome in enumerate(sorted(job.GEO_FAILURES)):
            with self.subTest(outcome=outcome):
                enriched = self.supplied_geography()
                enriched["posts"][0].update(
                    latitude=None,
                    longitude=None,
                    geocode_status=outcome,
                    geocode_error="provider outcome",
                )
                result = self.process(run_id=f"geo-{index}", enriched_envelope=enriched)
                _, silver = self.read_artifacts(result)
                self.assertEqual(
                    (result.status, result.count, result.rejected_count),
                    ("success", 2, 0),
                )
                self.assertEqual(silver["records"][0]["geo_status"], outcome)
                self.assertEqual(
                    silver["enrichment_errors"][0]["error"], "provider outcome"
                )

    def test_existing_geography_in_saved_input_is_preserved(self):
        self.envelope = self.supplied_geography()
        bronze, silver = self.read_artifacts(self.process())
        self.assertEqual(bronze["posts"], self.envelope["posts"])
        self.assert_contract(bronze, self.bronze_schema)
        self.assertEqual(silver["records"][0]["longitude"], 105.8542)

    def test_existing_normalizer_is_called_for_every_record(self):
        with patch.object(job, "normalize_court", wraps=normalize_court) as normalizer:
            result = self.process()
        self.assertEqual(normalizer.call_count, 2)
        for call, post in zip(normalizer.call_args_list, self.envelope["posts"]):
            self.assertEqual(call.args, (post,))
            self.assertEqual(
                call.kwargs,
                {
                    "run_id": result.run_id,
                    "collected_at": COLLECTED_AT,
                    "geocoded_at": None,
                },
            )

    def test_vietnamese_text_preserved_in_utf8(self):
        result = self.process()
        for path in (result.bronze_path, result.silver_path):
            self.assertIn("Sân pickleball Hà Nội 🇻🇳".encode(), path.read_bytes())

    def test_two_runs_are_distinct_and_input_is_unchanged(self):
        before = copy.deepcopy(self.envelope)
        first = self.process(run_id="first")
        saved = (first.bronze_path.read_bytes(), first.silver_path.read_bytes())
        second = self.process(run_id="second")
        self.assertNotEqual(first.bronze_path, second.bronze_path)
        self.assertEqual(
            saved, (first.bronze_path.read_bytes(), first.silver_path.read_bytes())
        )
        self.assertEqual(self.envelope, before)
        self.assertEqual(len(list(self.root.rglob("*.json"))), 4)

    def test_existing_silver_and_different_bronze_cannot_be_overwritten(self):
        result = self.process()
        original = result.silver_path.read_bytes()
        with self.assertRaises(FileExistsError):
            self.process()
        self.assertEqual(result.silver_path.read_bytes(), original)
        path = self.root / "bronze/pbplus/other.json"
        artifacts.write_json_atomic(path, {"different": True})
        before = path.read_bytes()
        with self.assertRaisesRegex(FileExistsError, "different Bronze"):
            self.process(run_id="other")
        self.assertEqual(path.read_bytes(), before)

    def test_partial_normalization_preserves_bronze_and_reports_index(self):
        self.envelope["posts"][0].update(id=None, url=None)
        result = self.process()
        bronze, silver = self.read_artifacts(result)
        self.assertEqual(bronze["posts"], self.envelope["posts"])
        self.assertEqual(
            (result.status, result.count, result.rejected_count), ("partial", 1, 1)
        )
        self.assertEqual(silver["input_count"], 2)
        self.assertEqual(silver["normalization_errors"][0]["post_index"], 0)
        self.assertEqual(silver["normalization_errors"][0]["error_type"], "ValueError")

    def test_all_rejected_produces_failed_silver(self):
        for post in self.envelope["posts"]:
            post.update(id=None, url=None)
        result = self.process()
        _, silver = self.read_artifacts(result)
        self.assertEqual(
            (result.status, result.count, result.rejected_count), ("failed", 0, 2)
        )
        self.assertEqual(silver["records"], [])

    def test_empty_directory_is_successful_empty_batch(self):
        self.envelope.update(count=0, posts=[])
        self.envelope["query_runs"][0].update(
            status="empty", raw_count=0, accepted_count=0
        )
        result = self.process()
        self.assertEqual((result.status, result.count), ("success", 0))

    def test_source_query_errors_remain_visible_and_affect_status(self):
        self.envelope["query_runs"][0].update(
            status="partial",
            failed_count=1,
            raw_count=3,
            errors=[
                {
                    "url": "https://pickleballplus.vn/san-tap/failed",
                    "error_type": "Timeout",
                    "error": "timeout",
                }
            ],
        )
        result = self.process(run_id="partial-crawl")
        _, silver = self.read_artifacts(result)
        self.assertEqual((result.status, result.rejected_count), ("partial", 0))
        self.assertEqual(silver["collection_errors"], self.envelope["query_runs"])
        self.envelope.update(posts=[], count=0)
        self.envelope["query_runs"][0].update(status="error", accepted_count=0)
        result = self.process(run_id="failed-crawl")
        self.assertEqual(result.status, "failed")

    def test_bronze_write_failure_visible_without_silver(self):
        with (
            patch.object(
                job, "_write_json_atomic", side_effect=OSError("Bronze denied")
            ),
            self.assertRaisesRegex(OSError, "Bronze denied"),
        ):
            self.process()
        self.assertFalse(list(self.root.rglob("*.json")))

    def test_silver_write_failure_preserves_bronze_and_replay_recovers(self):
        writer = job._write_json_atomic

        def failing_writer(path, payload):
            if "silver" in path.parts:
                raise OSError("Silver denied")
            writer(path, payload)

        with (
            patch.object(job, "_write_json_atomic", side_effect=failing_writer),
            self.assertRaisesRegex(OSError, "Silver denied"),
        ):
            self.process()
        path = self.root / "bronze/pbplus/test-run.json"
        before = path.read_bytes()
        bronze = json.loads(before)
        enriched = copy.deepcopy(bronze)
        enriched["geocoded_at"] = GEOCODED_AT
        enriched["posts"][0].update(self.supplied_geography()["posts"][0])
        with patch.object(job, "uuid4", side_effect=AssertionError("new replay ID")):
            result = job.process_pbplus_envelope(
                bronze, artifact_root=self.root, enriched_envelope=enriched
            )
        _, silver = self.read_artifacts(result)
        self.assertEqual(result.run_id, "test-run")
        self.assertEqual(before, path.read_bytes())
        self.assertEqual(silver["records"][0]["latitude"], 21.0285)

    def test_unexpected_normalizer_bug_explicit_and_bronze_replayable(self):
        with (
            patch.object(
                job, "normalize_court", side_effect=RuntimeError("normalizer bug")
            ),
            self.assertRaisesRegex(RuntimeError, "normalizer bug"),
        ):
            self.process()
        self.assertTrue((self.root / "bronze/pbplus/test-run.json").exists())
        self.assertFalse((self.root / "silver").exists())

    def test_shared_atomic_writer_used_by_both_jobs(self):
        self.assertIs(job._write_json_atomic, artifacts.write_json_atomic)
        self.assertIs(collect_social._write_json_atomic, artifacts.write_json_atomic)

    def test_tracker_starts_before_crawl_and_id_shared_through_enrichment(self):
        tracker = CrawlerStatusTracker("pbplus", self.root / "status.json")

        def crawler(**kwargs):
            self.assertEqual(tracker.status.status, "running")
            self.assertTrue(tracker.status.run_id)
            self.assertNotIn("status_tracker", kwargs)
            return self.envelope

        def geocoder(payload, *, delay):
            run_id = tracker.status.run_id
            self.assertEqual(payload["run_id"], run_id)
            path = self.root / f"bronze/pbplus/{run_id}.json"
            self.assertEqual(json.loads(path.read_text()), payload)
            self.assertFalse((self.root / f"silver/courts/{run_id}.json").exists())
            self.assertEqual(delay, 0)
            payload["posts"][0].update(self.supplied_geography()["posts"][0])
            payload["geocoded_at"] = GEOCODED_AT
            return payload

        before = copy.deepcopy(self.envelope)
        with patch.object(job, "uuid4", side_effect=AssertionError("second run ID")):
            result = self.collect(
                crawler=crawler,
                status_tracker=tracker,
                geocode=True,
                geocoder=geocoder,
                geo_delay=0,
            )
        bronze, silver = self.read_artifacts(result)
        self.assertEqual(result.run_id, tracker.status.run_id)
        self.assertEqual(bronze["posts"], before["posts"])
        self.assertEqual(silver["records"][0]["run_id"], result.run_id)
        self.assertEqual(tracker.status.status, "success")
        self.assertEqual(tracker.status.output_path, str(result.silver_path.resolve()))
        self.assertEqual(before, self.envelope)

    def test_without_tracker_run_id_generated_once_and_crawler_arguments_unchanged(
        self,
    ):
        crawler = Mock(return_value=self.envelope)
        with patch.object(
            job, "uuid4", return_value=Mock(hex="generated-run")
        ) as generator:
            result = self.collect(
                crawler=crawler, limit=5, timeout=7, delay=0, retries=0
            )
        generator.assert_called_once_with()
        crawler.assert_called_once_with(limit=5, timeout=7, delay=0, retries=0)
        self.assertEqual(result.run_id, "generated-run")

    def test_geocoding_opt_in(self):
        geocoder = Mock(side_effect=AssertionError("geocoder called"))
        result = self.collect(geocoder=geocoder)
        geocoder.assert_not_called()
        self.assertEqual(result.status, "success")

    def test_geo_transport_failure_keeps_courts_nullable_and_reports_outcome(self):
        tracker = CrawlerStatusTracker("pbplus", self.root / "status.json")

        def failed_geo(payload, **kwargs):
            payload["posts"][0]["court_name"] = "mutated discarded input"
            payload["posts"][0]["latitude"] = 70
            raise OSError("provider unavailable")

        with self.assertLogs(job.LOGGER, level="WARNING"):
            result = self.collect(
                geocode=True, geocoder=failed_geo, status_tracker=tracker
            )
        bronze, silver = self.read_artifacts(result)
        self.assertEqual(
            (result.status, result.count, result.rejected_count), ("success", 2, 0)
        )
        self.assertEqual(bronze["posts"], self.envelope["posts"])
        self.assertEqual(silver["enrichment_status_counts"], {"error": 2})
        self.assertEqual(len(silver["enrichment_errors"]), 2)
        for record in silver["records"]:
            self.assertIsNone(record["latitude"])
            self.assertIsNone(record["longitude"])
            self.assertEqual(record["geo_status"], "error")
        self.assertEqual(tracker.status.status, "success")

    def test_unexpected_geo_bug_fails_explicitly_preserving_bronze(self):
        tracker = CrawlerStatusTracker("pbplus", self.root / "status.json")
        with self.assertRaisesRegex(RuntimeError, "geo bug"):
            self.collect(
                geocode=True,
                geocoder=Mock(side_effect=RuntimeError("geo bug")),
                status_tracker=tracker,
            )
        self.assertEqual(tracker.status.status, "failed")
        self.assertEqual(len(list((self.root / "bronze/pbplus").glob("*.json"))), 1)
        self.assertFalse((self.root / "silver").exists())

    def test_bronze_write_failure_prevents_geo_call_and_marks_tracker_failed(self):
        tracker = CrawlerStatusTracker("pbplus", self.root / "status.json")
        geocoder = Mock()
        with (
            patch.object(
                job, "_write_json_atomic", side_effect=OSError("write denied")
            ),
            self.assertRaisesRegex(OSError, "write denied"),
        ):
            self.collect(status_tracker=tracker, geocode=True, geocoder=geocoder)
        geocoder.assert_not_called()
        self.assertEqual(tracker.status.status, "failed")

    def test_crawler_failure_marks_tracker_failed_without_artifacts(self):
        tracker = CrawlerStatusTracker("pbplus", self.root / "status.json")
        with self.assertRaisesRegex(RuntimeError, "crawl failed"):
            self.collect(
                crawler=Mock(side_effect=RuntimeError("crawl failed")),
                status_tracker=tracker,
            )
        self.assertTrue(tracker.status.run_id)
        self.assertEqual(tracker.status.status, "failed")
        self.assertFalse((self.root / "bronze").exists())

    def test_tracker_start_failure_never_invokes_crawler_or_generates_fallback_id(self):
        tracker = CrawlerStatusTracker("pbplus", self.root / "status.json")
        crawler = Mock()
        with (
            patch.object(tracker, "start", side_effect=OSError("status denied")),
            patch.object(job, "uuid4", side_effect=AssertionError("fallback ID")),
            self.assertRaisesRegex(OSError, "status denied"),
        ):
            self.collect(crawler=crawler, status_tracker=tracker)
        crawler.assert_not_called()
        self.assertEqual(tracker.status.status, "failed")

    def test_partial_batch_marks_tracker_failed_after_artifacts(self):
        self.envelope["posts"][0].update(id=None, url=None)
        tracker = CrawlerStatusTracker("pbplus", self.root / "status.json")
        result = self.collect(status_tracker=tracker)
        self.assertEqual(result.status, "partial")
        self.assertEqual(tracker.status.status, "failed")
        self.assertTrue(result.bronze_path.exists())
        self.assertTrue(result.silver_path.exists())

    def test_tracker_must_establish_new_run_and_match_source(self):
        tracker = CrawlerStatusTracker("pbplus", self.root / "status.json")
        tracker.start(seed_query="previous", sort="directory", target_count=1)
        crawler = Mock()
        with (
            patch.object(tracker, "start"),
            self.assertRaisesRegex(RuntimeError, "new tracked"),
        ):
            self.collect(crawler=crawler, status_tracker=tracker)
        crawler.assert_not_called()
        with self.assertRaisesRegex(ValueError, "source must be pbplus"):
            self.collect(
                crawler=crawler,
                status_tracker=CrawlerStatusTracker(
                    "threads", self.root / "other.json"
                ),
            )
        crawler.assert_not_called()
        self.assertFalse((self.root / "bronze").exists())

    def test_failure_status_persistence_does_not_mask_primary_error(self):
        tracker = CrawlerStatusTracker("pbplus", self.root / "status.json")
        with (
            patch.object(tracker, "fail", side_effect=OSError("status failed")),
            self.assertLogs(job.LOGGER, level="ERROR"),
            self.assertRaisesRegex(RuntimeError, "primary crawl error"),
        ):
            self.collect(
                crawler=Mock(side_effect=RuntimeError("primary crawl error")),
                status_tracker=tracker,
            )

    def test_safe_run_ids_and_saved_lineage_enforced(self):
        for value in ("../escape", "/absolute", "a/b", "", "..", "a" * 129):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.process(run_id=value)
        self.envelope["run_id"] = "original"
        with self.assertRaisesRegex(ValueError, "conflicts"):
            self.process(run_id="different")
        self.assertFalse(list(self.root.rglob("*.json")))

    def test_invalid_batch_metadata_rejected_before_writes(self):
        for field, value in (
            ("source", "threads"),
            ("count", 3),
            ("count", True),
            ("sort", "recent"),
            ("seed_query", ""),
            ("collected_at", None),
            ("collected_at", "2026-10-05"),
            ("query_runs", {}),
            ("posts", [None]),
            ("schema_version", "2"),
            ("geocoded_at", "2026-10-05"),
        ):
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                job.process_pbplus_envelope(
                    {**self.envelope, field: value},
                    run_id="invalid",
                    artifact_root=self.root,
                )
        self.assertFalse(list(self.root.rglob("*.json")))

    def test_supplied_geo_cannot_change_identity_batch_or_run_lineage(self):
        for index, field in enumerate(("run_id", "collected_at", "id", "order")):
            enriched = self.supplied_geography()
            if field == "run_id":
                enriched[field] = "unrelated"
            elif field == "collected_at":
                enriched[field] = GEOCODED_AT
            elif field == "id":
                enriched["posts"][0][field] = "https://example.com/unrelated"
            else:
                enriched["posts"].reverse()
            with (
                self.subTest(field=field),
                self.assertRaisesRegex(ValueError, "conflicts"),
            ):
                self.process(run_id=f"bad-geo-{index}", enriched_envelope=enriched)
        self.assertEqual(len(list((self.root / "bronze/pbplus").glob("*.json"))), 4)
        self.assertFalse((self.root / "silver").exists())

    def test_supplied_geo_overlays_only_geo_fields(self):
        enriched = self.supplied_geography()
        enriched["posts"][0].update(
            court_name="changed name", number_of_courts=99, province="invented"
        )
        _, silver = self.read_artifacts(self.process(enriched_envelope=enriched))
        self.assertEqual(
            silver["records"][0]["court_name"], self.envelope["posts"][0]["court_name"]
        )
        self.assertEqual(silver["records"][0]["number_of_courts"], 4)
        self.assertNotIn("province", silver["records"][0])

    def test_offline_processing_imports_no_network_or_infrastructure_clients(self):
        original_import = __import__

        def guarded_import(name, *args, **kwargs):
            if any(
                word in name
                for word in (
                    "requests",
                    "geopy",
                    "playwright",
                    "sources.crawler",
                    "publishers",
                    "kafka",
                    "boto",
                    "s3",
                    "delta",
                    "spark",
                    "clickhouse",
                    "airflow",
                )
            ):
                raise AssertionError(f"unexpected import: {name}")
            return original_import(name, *args, **kwargs)

        with (
            patch("builtins.__import__", side_effect=guarded_import),
            patch.object(
                job, "_crawl_pbplus", side_effect=AssertionError("live crawler")
            ),
            patch.object(
                job, "_geocode_pbplus", side_effect=AssertionError("live geocoder")
            ),
        ):
            self.process(enriched_envelope=self.supplied_geography())

    def test_default_root_shared_with_threads(self):
        self.assertEqual(default_artifact_root(), COLLECTOR_ROOT / ".local")

    def test_cli_replay_success_partial_failed_exit_codes_and_supplied_geo(self):
        source = self.root / "input.json"
        geography = self.root / "geo.json"
        for index, expected_exit in enumerate((0, 2, 1)):
            if index:
                self.envelope["posts"][index - 1].update(id=None, url=None)
            source.write_text(
                json.dumps(self.envelope, ensure_ascii=False), encoding="utf-8"
            )
            geography.write_text(
                json.dumps(self.supplied_geography(), ensure_ascii=False),
                encoding="utf-8",
            )
            output = io.StringIO()
            with (
                redirect_stdout(output),
                patch.object(
                    job, "_crawl_pbplus", side_effect=AssertionError("live crawler")
                ),
                patch.object(
                    job, "_geocode_pbplus", side_effect=AssertionError("live geocoder")
                ),
            ):
                code = job.main(
                    [
                        "--replay",
                        str(source),
                        "--geography",
                        str(geography),
                        "--run-id",
                        f"cli-{index}",
                        "--artifact-root",
                        str(self.root),
                    ]
                )
            self.assertEqual(code, expected_exit)
            self.assertEqual(json.loads(output.getvalue())["rejected_count"], index)

    def test_cli_missing_input_returns_failure_and_logs_error(self):
        with self.assertLogs(job.LOGGER, level="ERROR"):
            code = job.main(["--replay", str(self.root / "missing.json")])
        self.assertEqual(code, 1)

    def test_cli_forbids_live_geo_in_replay_and_live_run_relabelling(self):
        for options in (
            ["--replay", "unused", "--geocode"],
            ["--run-id", "unrelated"],
            ["--geography", "unused"],
        ):
            with (
                self.subTest(options=options),
                patch("sys.stderr", new=io.StringIO()),
                self.assertRaises(SystemExit) as caught,
            ):
                job.main(options)
            self.assertEqual(caught.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
