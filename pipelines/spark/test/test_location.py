"""Reference integrity and explicit address/text evidence tests."""

import copy
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "pipelines/spark/src"), str(ROOT / "apps/collector/src")]

from dsp_spark.processing.transform.location import (
    load_alias_map,
    normalize_match_text,
    resolve_alias,
    resolve_court_address,
    validated_alias_map,
)
from jsonschema import ValidationError


class LocationReferenceTests(unittest.TestCase):
    def setUp(self):
        self.catalog = json.loads(
            (ROOT / "configs/reference/vietnam_locations.v1.json").read_text()
        )

    def test_catalog_validates_and_canonical_names_round_trip(self):
        aliases = validated_alias_map(self.catalog)
        self.assertEqual(len({v[0] for v in aliases.values()}), 10)
        for place in self.catalog["locations"]:
            self.assertEqual(
                resolve_alias(place["canonical_name"], aliases),
                (place["location_id"], place["canonical_name"]),
            )

    def test_duplicate_ids_rejected(self):
        self.catalog["locations"].append(copy.deepcopy(self.catalog["locations"][0]))
        with self.assertRaisesRegex(ValueError, "Duplicate location_id"):
            validated_alias_map(self.catalog)

    def test_normalized_alias_conflict_rejected(self):
        self.catalog["locations"][1]["aliases"].append("  HÀ---NỘI  ")
        with self.assertRaisesRegex(ValueError, "Conflicting normalized alias"):
            validated_alias_map(self.catalog)

    def test_empty_names_aliases_and_punctuation_only_rejected(self):
        for field, value in [
            ("canonical_name", ""),
            ("canonical_name", " "),
            ("aliases", []),
            ("aliases", [" "]),
            ("aliases", ["..."]),
        ]:
            with self.subTest(field=field, value=value):
                catalog = copy.deepcopy(self.catalog)
                catalog["locations"][0][field] = value
                with self.assertRaises((ValueError, ValidationError)):
                    validated_alias_map(catalog)

    def test_hanoi_explicit_accented_and_ascii_variants(self):
        for value in [
            "Hà Nội",
            "Ha Noi",
            "Hanoi",
            "  HÀ   NỘI ",
            "Ha-Noi",
            "Hà Nội",
        ]:
            self.assertEqual(resolve_alias(value), ("vn-hanoi", "Hà Nội"))

    def test_ho_chi_minh_variants_preserve_accents(self):
        for value in [
            "TP. Hồ Chí Minh",
            "TP HCM",
            "TP.HCM.",
            "TPHCM",
            "HCMC",
            "Ho Chi Minh",
            "Sài Gòn",
            "Sai Gon",
            "saigon",
        ]:
            self.assertEqual(
                resolve_alias(value), ("vn-ho-chi-minh-city", "TP. Hồ Chí Minh")
            )

    def test_unknown_names_and_no_fuzzy_or_substring_matches(self):
        for value in [
            None,
            "",
            "Hàn Nội",
            "Hanoii",
            "Hanoian",
            "Huế street",
            "Atlantis",
            "Thừa Thiên Huế",
        ]:
            self.assertIsNone(resolve_alias(value))
        self.assertEqual(normalize_match_text(" HuẾ "), "huế")

    def test_explicit_address_components_and_page_address_marker(self):
        for address in [
            "12 Road, TP.HCM.",
            "12 Road, Thành phố Hồ Chí Minh",
            "12 Road, Sài Gòn",
            "A page mentions Hà Nội Địa chỉ : 12 Road, TP HCM",
        ]:
            self.assertEqual(
                resolve_court_address(address, None),
                ("vn-ho-chi-minh-city", "TP. Hồ Chí Minh", "address_alias"),
            )
        self.assertEqual(
            resolve_court_address("12 road, Tỉnh Bình Dương", "venue_page")[0],
            "vn-binh-duong",
        )

    def test_no_substring_street_or_province_to_city_inference(self):
        for address in [
            "12 Phố Huế",
            "12 Hanoi Road",
            "12 Road, Tỉnh Thừa Thiên Huế",
            "12 Road, Tỉnh Khánh Hòa",
        ]:
            self.assertEqual(
                resolve_court_address(address, None), (None, None, "unmapped_address")
            )

    def test_missing_ambiguous_and_court_name_fallback_unresolved(self):
        for address, source, reason in [
            (None, None, "missing_address"),
            ("12 Road, Hà Nội, TP HCM", None, "ambiguous_address"),
            ("Pickleball Hà Nội", "court_name_fallback", "court_name_fallback"),
        ]:
            self.assertEqual(
                resolve_court_address(address, source), (None, None, reason)
            )

    def test_same_common_key_keeps_address_and_mention_evidence_distinct(self):
        social = resolve_alias("Sài Gòn")
        court = resolve_court_address("12 Road, TP HCM", None)
        self.assertEqual(social, court[:2])
        self.assertEqual(court[2], "address_alias")
        self.assertTrue(load_alias_map())


if __name__ == "__main__":
    unittest.main()
