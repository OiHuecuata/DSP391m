"""Offline court.v1 tests using synthetic crawler/geo projection fixtures.

Contract checks cover this schema's structural constraints, not the complete
JSON Schema standard. Runtime validation would require a Draft 2020-12
validator with format checking; no validation/test dependency is added here.
"""

from __future__ import annotations

import copy
import json
import math
import re
import sys
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

COLLECTOR_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = COLLECTOR_ROOT.parents[1]
sys.path.insert(0, str(COLLECTOR_ROOT / "src"))

from dsp_collector.normalizers.court import normalize_court

SCHEMA_PATH = REPOSITORY_ROOT / "contracts/data/silver/court.v1.schema.json"
SAMPLE_DIRECTORY = COLLECTOR_ROOT / ".local/pbplus"
SOURCE_URL = "https://pickleballplus.vn/san-tap/san-pickleball-test"
COLLECTED_AT = "2026-10-01T23:02:10.383825+07:00"
GEOCODED_AT = "2026-10-02T09:00:00+07:00"
NORMALIZED_AT = "2026-10-05T12:00:00+07:00"


class CourtNormalizerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))

    def setUp(self):
        # Field names/types follow scrape_court; values are synthetic.
        self.court = {
            "id": SOURCE_URL,
            "code": "san-pickleball-test",
            "url": SOURCE_URL,
            "has_media": False,
            "court_name": "Sân Pickleball Test",
            "number_of_courts": 6,
        }

    def normalize(self, court=None, **metadata):
        options = {
            "run_id": "court-test-run",
            "collected_at": COLLECTED_AT,
            "normalized_at": NORMALIZED_AT,
        }
        options.update(metadata)
        result = normalize_court(self.court if court is None else court, **options)
        self.assert_contract(result)
        return result

    def add_geography(self, **changes):
        # geocode_payload adds these fields; no geocoder is imported or called.
        self.court.update(
            address="12 Đường mẫu, Hà Nội",
            address_source="court_name_fallback",
            latitude=21.0285,
            longitude=105.8542,
            geocode_status="ok_region_unverified",
        )
        self.court.update(changes)

    def assert_contract(self, record):
        self.assertEqual(set(record), set(self.schema["required"]))
        self.assertEqual(set(record), set(self.schema["properties"]))
        self.assertIs(self.schema["additionalProperties"], False)
        for field, value in record.items():
            node = self.schema["properties"][field]
            if "$ref" in node:
                node = self.schema["$defs"][node["$ref"].removeprefix("#/$defs/")]
            kinds = node["type"]
            kinds = kinds if isinstance(kinds, list) else [kinds]
            matches = {
                "null": value is None,
                "string": isinstance(value, str),
                "integer": type(value) is int,
                "boolean": type(value) is bool,
                "number": type(value) in (int, float),
            }
            self.assertTrue(any(matches[kind] for kind in kinds), (field, value))
            if "const" in node:
                self.assertEqual(value, node["const"])
            if "enum" in node:
                self.assertIn(value, node["enum"])
            if value is None:
                continue
            if isinstance(value, str):
                self.assertGreaterEqual(len(value), node.get("minLength", 0))
                if "pattern" in node:
                    self.assertIsNotNone(re.search(node["pattern"], value))
                if node.get("format") == "date-time":
                    parsed = datetime.fromisoformat(value)
                    self.assertIsNotNone(parsed.utcoffset())
                if node.get("format") == "uri":
                    self.assertRegex(value, r"^https?://[^\s/]+")
            if type(value) in (int, float):
                self.assertTrue(math.isfinite(value))
                if "minimum" in node:
                    self.assertGreaterEqual(value, node["minimum"])
                if "maximum" in node:
                    self.assertLessEqual(value, node["maximum"])

    def test_minimal_crawler_record(self):
        result = self.normalize()
        self.assertEqual(result["schema_version"], "1")
        self.assertEqual(result["source"], "pbplus")
        self.assertEqual(result["court_name"], self.court["court_name"])

    def test_identity_only_record(self):
        result = self.normalize({"id": SOURCE_URL})
        self.assertEqual(result["source_record_id"], SOURCE_URL)
        for field in ("source_url", "court_name", "number_of_courts", "has_media"):
            self.assertIsNone(result[field])

    def test_non_geocoded_court(self):
        result = self.normalize()
        for field in (
            "address",
            "latitude",
            "longitude",
            "geocoded_at",
            "geo_status",
            "address_source",
        ):
            self.assertIsNone(result[field])

    def test_geocoded_court(self):
        self.add_geography()
        result = self.normalize(geocoded_at=GEOCODED_AT)
        self.assertEqual(result["address"], self.court["address"])
        self.assertEqual(result["latitude"], 21.0285)
        self.assertEqual(result["longitude"], 105.8542)
        self.assertEqual(result["geo_status"], "ok_region_unverified")

    def test_failed_geocoding_does_not_invalidate_court(self):
        for status in (
            "missing_address",
            "not_found",
            "outside_vietnam",
            "region_mismatch",
            "error",
        ):
            with self.subTest(status=status):
                self.add_geography(latitude=None, longitude=None, geocode_status=status)
                result = self.normalize(geocoded_at=GEOCODED_AT)
                self.assertEqual(result["geo_status"], status)
                self.assertIsNone(result["latitude"])
                self.assertIsNone(result["longitude"])

    def test_record_id_is_deterministic(self):
        self.assertEqual(self.normalize(), self.normalize())
        self.assertEqual(self.normalize()["record_id"], f"pbplus:{SOURCE_URL}")

    def test_run_id_does_not_change_entity_identity(self):
        first = self.normalize(run_id="first")
        second = self.normalize(run_id="second", normalized_at="2026-10-06T00:00:00Z")
        self.assertEqual(first["record_id"], second["record_id"])
        self.assertNotEqual(first["run_id"], second["run_id"])

    def test_source_record_id_preserved(self):
        self.assertEqual(self.normalize()["source_record_id"], self.court["id"])

    def test_identity_uses_id_before_url(self):
        self.court["url"] = SOURCE_URL + "/different"
        result = self.normalize()
        self.assertEqual(result["source_record_id"], SOURCE_URL)
        self.assertEqual(result["source_url"], self.court["url"])

    def test_identity_falls_back_to_source_url(self):
        for identity in (None, "", "invalid", {}, True, 123):
            with self.subTest(identity=identity):
                self.court["id"] = identity
                self.assertEqual(self.normalize()["source_record_id"], SOURCE_URL)

    def test_url_identity_components_are_not_rewritten(self):
        identity = SOURCE_URL + "/?lang=vi#listing"
        self.court.update(id=identity, url=identity)
        result = self.normalize()
        self.assertEqual(result["source_record_id"], identity)
        self.assertEqual(result["record_id"], "pbplus:" + identity)

    def test_source_url_preserved(self):
        self.assertEqual(self.normalize()["source_url"], SOURCE_URL)

    def test_source_code_preserved(self):
        self.assertEqual(self.normalize()["source_code"], self.court["code"])

    def test_court_name_preserved_and_nullable(self):
        self.assertEqual(self.normalize()["court_name"], self.court["court_name"])
        self.court["court_name"] = None
        self.assertIsNone(self.normalize()["court_name"])

    def test_number_of_courts_integer_preserved(self):
        self.assertEqual(self.normalize()["number_of_courts"], 6)
        self.court["number_of_courts"] = 0
        self.assertEqual(self.normalize()["number_of_courts"], 0)

    def test_missing_number_of_courts_remains_null(self):
        self.court.pop("number_of_courts")
        self.assertIsNone(self.normalize()["number_of_courts"])
        self.court["number_of_courts"] = None
        self.assertIsNone(self.normalize()["number_of_courts"])

    def test_numeric_court_count_conversion(self):
        for value in ("6", " 6 ", 6.0):
            with self.subTest(value=value):
                self.court["number_of_courts"] = value
                result = self.normalize()["number_of_courts"]
                self.assertEqual(result, 6)
                self.assertIs(type(result), int)

    def test_unusable_court_count_becomes_null(self):
        for value in (True, -1, 2.5, "2.5", "six", {}, float("nan"), float("inf")):
            with self.subTest(value=value):
                self.court["number_of_courts"] = value
                self.assertIsNone(self.normalize()["number_of_courts"])

    def test_has_media_boolean_preserved(self):
        for value in (True, False):
            with self.subTest(value=value):
                self.court["has_media"] = value
                self.assertIs(self.normalize()["has_media"], value)

    def test_unavailable_or_nonboolean_media_flag_is_null(self):
        for value in (None, 0, 1, "false", []):
            with self.subTest(value=value):
                self.court["has_media"] = value
                self.assertIsNone(self.normalize()["has_media"])

    def test_address_nullable(self):
        self.court["address"] = None
        self.assertIsNone(self.normalize()["address"])

    def test_supplied_address_is_preserved_without_inference(self):
        self.court["address"] = "12 Đường mẫu, Hà Nội"
        result = self.normalize()
        self.assertEqual(result["address"], self.court["address"])
        self.assertIsNone(result["address_source"])
        self.assertIsNone(result["geo_status"])

    def test_latitude_nullable(self):
        self.court["latitude"] = None
        self.assertIsNone(self.normalize()["latitude"])

    def test_longitude_nullable(self):
        self.court["longitude"] = None
        self.assertIsNone(self.normalize()["longitude"])

    def test_valid_numeric_coordinates(self):
        self.add_geography(latitude="21.0285", longitude="105.8542")
        result = self.normalize()
        self.assertEqual(result["latitude"], 21.0285)
        self.assertEqual(result["longitude"], 105.8542)
        self.assertIs(type(result["latitude"]), float)
        self.assertIs(type(result["longitude"]), float)

    def test_coordinate_boundaries_and_zero(self):
        for latitude, longitude in ((-90, -180), (90, 180), (0, 0)):
            with self.subTest(latitude=latitude, longitude=longitude):
                self.add_geography(latitude=latitude, longitude=longitude)
                result = self.normalize()
                self.assertEqual(result["latitude"], latitude)
                self.assertEqual(result["longitude"], longitude)

    def test_invalid_latitude_becomes_null(self):
        for value in (
            90.1,
            -90.1,
            "bad",
            "",
            True,
            {},
            float("nan"),
            float("inf"),
            10**400,
        ):
            with self.subTest(value=value):
                self.add_geography(latitude=value)
                result = self.normalize()
                self.assertIsNone(result["latitude"])
                self.assertEqual(result["longitude"], 105.8542)
                self.assertEqual(result["geo_status"], "ok_region_unverified")

    def test_invalid_longitude_becomes_null(self):
        for value in (
            180.1,
            -180.1,
            "bad",
            False,
            [],
            "NaN",
            "Infinity",
            float("-inf"),
        ):
            with self.subTest(value=value):
                self.add_geography(longitude=value)
                result = self.normalize()
                self.assertIsNone(result["longitude"])
                self.assertEqual(result["latitude"], 21.0285)

    def test_partial_coordinates_are_not_invented(self):
        self.court["latitude"] = 21.0285
        result = self.normalize()
        self.assertEqual(result["latitude"], 21.0285)
        self.assertIsNone(result["longitude"])
        self.assertIsNone(result["geo_status"])

    def test_collected_at_normalized_to_utc(self):
        self.assertEqual(
            self.normalize()["collected_at"], "2026-10-01T16:02:10.383825Z"
        )

    def test_geocoded_at_is_independent(self):
        result = self.normalize(geocoded_at=GEOCODED_AT)
        self.assertEqual(result["geocoded_at"], "2026-10-02T02:00:00.000000Z")
        self.assertNotEqual(result["geocoded_at"], result["collected_at"])
        self.assertIsNone(result["geo_status"])

    def test_normalized_at_can_be_injected(self):
        self.assertEqual(
            self.normalize()["normalized_at"], "2026-10-05T05:00:00.000000Z"
        )

    def test_aware_datetime_inputs(self):
        value = datetime(2026, 10, 5, tzinfo=UTC)
        result = self.normalize(
            collected_at=value, geocoded_at=value, normalized_at=value
        )
        for field in ("collected_at", "geocoded_at", "normalized_at"):
            self.assertEqual(result[field], "2026-10-05T00:00:00.000000Z")

    def test_default_normalized_at_uses_execution_time(self):
        before = datetime.now(UTC)
        result = normalize_court(self.court, run_id="run", collected_at=COLLECTED_AT)
        after = datetime.now(UTC)
        self.assert_contract(result)
        self.assertLessEqual(before, datetime.fromisoformat(result["normalized_at"]))
        self.assertLessEqual(datetime.fromisoformat(result["normalized_at"]), after)
        self.assertIsNone(result["geocoded_at"])

    def test_naive_or_invalid_external_timestamps_rejected(self):
        # This naive value deliberately tests rejection of missing timezones.
        naive = datetime(2026, 10, 5)  # noqa: DTZ001
        for field in ("collected_at", "geocoded_at", "normalized_at"):
            for value in ("invalid", "2026-10-05", "2026-10-05T12:00:00", naive):
                with (
                    self.subTest(field=field, value=value),
                    self.assertRaises(ValueError),
                ):
                    self.normalize(**{field: value})
        with self.assertRaises(ValueError):
            self.normalize(collected_at=None)

    def test_run_id_preserved(self):
        self.assertEqual(
            self.normalize(run_id="observation-123")["run_id"], "observation-123"
        )

    def test_invalid_run_id_rejected(self):
        for value in (None, "", "   ", True, 123):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.normalize(run_id=value)

    def test_explicit_address_provenance_and_geo_status(self):
        for source in ("venue_page", "court_name_fallback"):
            with self.subTest(source=source):
                self.add_geography(address_source=source, geocode_status="ok")
                result = self.normalize()
                self.assertEqual(result["address_source"], source)
                self.assertEqual(result["geo_status"], "ok")

    def test_unknown_provenance_labels_fail_explicitly(self):
        for field, value in (("geocode_status", "made_up"), ("address_source", "gps")):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.normalize({**self.court, field: value})

    def test_unknown_fields_and_source_payloads_are_ignored(self):
        self.court.update(
            unknown={"nested": object()},
            raw_html="<h1>source</h1>",
            raw_payload={"nested": "source"},
            phone="source-only",
            opening_hours=["source-only"],
            services=["source-only"],
            geocode_country="Vietnam",
            geocode_matched_address="source-only",
            geocoded_at="not observation metadata",
        )
        result = self.normalize()
        self.assertEqual(set(result), set(self.schema["properties"]))
        self.assertIsNone(result["geocoded_at"])
        json.dumps(result, allow_nan=False)

    def test_input_not_mutated(self):
        self.add_geography()
        self.court["unknown"] = {"nested": ["preserved"]}
        before = copy.deepcopy(self.court)
        result = self.normalize()
        self.assertEqual(self.court, before)
        result["court_name"] = "changed"
        self.assertEqual(self.court, before)

    def test_missing_or_unusable_source_identity_fails(self):
        for value in (
            None,
            "",
            {},
            123,
            True,
            "relative/path",
            "ftp://example.org/court",
            "https://",
            "https://bad url",
            "https://[broken",
        ):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(ValueError, "source identity"),
            ):
                self.normalize({"id": value, "url": value})

    def test_non_mapping_input_rejected(self):
        with self.assertRaises(TypeError):
            normalize_court([], run_id="run", collected_at=COLLECTED_AT)

    def test_schema_structure_matches_record(self):
        self.assertEqual(
            self.schema["$schema"], "https://json-schema.org/draft/2020-12/schema"
        )
        self.assertEqual(set(self.normalize()), set(self.schema["properties"]))
        self.assertEqual(set(self.schema["required"]), set(self.schema["properties"]))

    def test_schema_timestamp_formats_do_not_fix_fractional_digits(self):
        result = self.normalize()
        for field in ("collected_at", "geocoded_at", "normalized_at"):
            self.assertEqual(self.schema["properties"][field]["format"], "date-time")
            self.assertNotIn("pattern", self.schema["properties"][field])
        # Valid RFC 3339 timestamps with zero or three fractional digits fit.
        for value in ("2026-10-05T05:00:00Z", "2026-10-05T05:00:00.123Z"):
            self.assert_contract({**result, "normalized_at": value})

    def test_contract_rejects_invalid_shape_or_coordinates(self):
        result = self.normalize()
        for changes in (
            {"extra": "raw"},
            {"latitude": 91},
            {"longitude": -181},
            {"number_of_courts": True},
        ):
            with self.subTest(changes=changes), self.assertRaises(AssertionError):
                self.assert_contract({**result, **changes})

    def test_normalization_performs_no_file_or_network_io(self):
        with (
            patch("builtins.open", side_effect=AssertionError("unexpected file I/O")),
            patch(
                "socket.socket", side_effect=AssertionError("unexpected network I/O")
            ),
        ):
            self.normalize()

    def test_local_pbplus_samples_read_only_when_available(self):
        samples = sorted(SAMPLE_DIRECTORY.glob("pickleball*.json"))
        if not samples:
            self.skipTest(
                "no local PBPlus sample; synthetic fixtures follow crawler/geo code"
            )
        for path in samples:
            original_bytes = path.read_bytes()
            payload = json.loads(original_bytes)
            self.assertEqual(payload["source"], "pbplus")
            self.assertEqual(len(payload["posts"]), payload["count"])
            for court in payload["posts"]:
                with self.subTest(sample=path.name, identity=court.get("id")):
                    before = copy.deepcopy(court)
                    self.normalize(
                        court,
                        collected_at=payload["collected_at"],
                        geocoded_at=payload.get("geocoded_at"),
                    )
                    self.assertEqual(court, before)
            self.assertEqual(path.read_bytes(), original_bytes)


if __name__ == "__main__":
    unittest.main()
