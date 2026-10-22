"""Clearly synthetic fixtures for requirements v1.0; no production metrics."""

import copy
import unittest
from unittest.mock import patch

from dsp_spark.processing.intelligence.common import active_observations, digest
from dsp_spark.processing.intelligence.geography import Geography
from dsp_spark.processing.intelligence.gold import build_gold
from dsp_spark.processing.intelligence.quality import validate_tables
from dsp_spark.processing.intelligence.silver import build_silver
from dsp_spark.processing.intelligence.social import classify
from dsp_spark.processing.intelligence.venues import court_count, price

AT = "2026-10-10T00:00:00Z"


def observation(
    sid, *, platform="threads", kind="Post", at=AT, run="synthetic-test", **fields
):
    return dict(
        observation_id=digest([platform, sid, kind, at, run]),
        source_platform=platform,
        source_record_id=sid,
        record_type=kind,
        source_url=f"https://synthetic.invalid/{sid}",
        collected_at=at,
        collection_run_id=run,
        retention_expires_at=None,
        raw_payload="{}",
        **fields,
    )


def post(
    sid="p1",
    text="Tìm sân pickleball ở Bình Dương, giá bao nhiêu? Nhóm 4 người muốn thuê 2 tiếng tối thứ bảy.",
    **kw,
):
    return observation(
        sid,
        native_post_id=sid,
        text_raw=text,
        title_raw=None,
        description_raw=None,
        published_at=AT,
        content_type="post",
        **kw,
    )


def court(
    sid="v1", platform="pbplus", count="6", address="123 Nguyễn Văn Linh, Đà Nẵng", **kw
):
    return observation(
        sid,
        platform=platform,
        kind="Venue",
        venue_name_raw="ABC Pickleball",
        address_raw=address,
        court_count_raw=count,
        **kw,
    )


def metric(views=100, at=AT, **kw):
    return observation(
        "p1",
        kind="Metric",
        at=at,
        observed_at=at,
        native_content_id="p1",
        content_type="post",
        metric_scope="cumulative",
        metric_availability='{"view_count":"available"}',
        view_count=views,
        like_count=None,
        **kw,
    )


class IntelligenceTests(unittest.TestCase):
    def setUp(self):
        self.geo = Geography()

    def build(self, data, previous=None):
        silver, work = build_silver(data, self.geo, previous=previous)
        validate_tables(silver)
        gold = build_gold(silver, [], self.geo, as_of="2026-10-12T00:00:00Z")
        validate_tables(gold)
        return silver, gold, work

    def test_at01_multisource_venue_preserves_links_unknown_courts(self):
        s, g, _ = self.build(
            {
                "bronze_court_observations": [
                    court(),
                    court("osm1", platform="osm", count=None),
                ]
            }
        )
        self.assertEqual(len(s["silver_court_venues"]), 1)
        self.assertEqual(len(s["silver_venue_source_links"]), 2)
        self.assertEqual(s["silver_court_venues"][0]["court_count"], 6)
        national = next(
            r
            for r in g["court_supply_by_admin_version_v2"]
            if r["admin_unit_id"] == "NATIONAL"
        )
        self.assertEqual(
            (national["venue_count"], national["known_court_count"]), (1, 6)
        )
        unknown, gold, _ = self.build(
            {"bronze_court_observations": [court(count=None)]}
        )
        self.assertIsNone(unknown["silver_court_venues"][0]["court_count"])
        self.assertIsNone(
            gold["court_supply_by_admin_version_v2"][0]["known_court_count"]
        )

    def test_at02_social_discovery_is_unverified_with_traceable_price(self):
        s, g, _ = self.build(
            {
                "bronze_social_posts": [
                    post(
                        text="Khai trương sân ABC Pickleball, 6 sân ngoài trời, giá từ 100k đến 150k/giờ, đường Nguyễn Văn Linh, Đà Nẵng."
                    )
                ]
            }
        )
        candidate = s["silver_venue_candidates"][0]
        self.assertEqual(candidate["court_count"], 6)
        self.assertEqual(candidate["verification_status"], "UNVERIFIED")
        self.assertIsNone(candidate["latitude"])
        self.assertEqual(candidate["price_min_vnd"], 100000.0)
        self.assertEqual(s["silver_venue_evidence"][0]["source_platform"], "threads")
        self.assertEqual(g["court_supply_by_admin_version_v2"], [])

    def test_at03_old_name_and_harmonized_no_reverse_allocation(self):
        old = self.geo.resolve("Bình Dương", "2024-05-01T00:00:00Z")
        self.assertTrue(old["old_id"].endswith("binh-duong"))
        self.assertTrue(old["new_id"].endswith("ho-chi-minh"))
        modern = self.geo.resolve("TP.HCM", AT)
        self.assertIsNone(modern["old_id"])
        self.assertTrue(modern["new_id"])
        s, _, _ = self.build(
            {"bronze_social_posts": [post(text="pickleball Bình Dương")]}
        )
        mention = s["silver_social_geo_mentions"][0]
        self.assertEqual(mention["location_text_raw"], "Bình Dương")
        self.assertIn("HISTORICAL", mention["interpretation_json"])

    def test_at04_comment_does_not_inherit_parent_geo_or_intent(self):
        comments = [
            observation(
                "c1",
                kind="Comment",
                native_comment_id="c1",
                native_post_id="p1",
                parent_comment_id=None,
                comment_text_raw="Tìm sân pickleball ở Huế",
                published_at=AT,
                is_reply=False,
            ),
            observation(
                "r1",
                kind="Comment",
                native_comment_id="r1",
                native_post_id="p1",
                parent_comment_id="c1",
                comment_text_raw="Cảm ơn",
                published_at=AT,
                is_reply=True,
            ),
        ]
        s, _, _ = self.build(
            {
                "bronze_social_posts": [post(text="pickleball Hà Nội")],
                "bronze_social_comments": comments,
            }
        )
        comment = next(c for c in s["silver_social_comments"] if not c["is_reply"])
        self.assertIn("SEEK_COURT", comment["intent_labels"])
        locations = [
            r["location_text_raw"]
            for r in s["silver_social_geo_mentions"]
            if r["content_id"] == comment["content_id"]
        ]
        self.assertEqual(locations, ["Huế"])
        reply = next(c for c in s["silver_social_comments"] if c["is_reply"])
        self.assertEqual(reply["parent_comment_id"], comment["comment_id"])
        self.assertFalse(
            any(
                m["content_id"] == reply["content_id"]
                for m in s["silver_social_geo_mentions"]
            )
        )

    def test_at05_latest_snapshots_missing_metrics_and_negative_delta(self):
        s, g, _ = self.build(
            {
                "bronze_social_posts": [post()],
                "bronze_engagement_snapshots": [
                    metric(),
                    metric(90, at="2026-10-10T02:00:00Z"),
                ],
            }
        )
        latest = max(
            s["silver_social_engagement_snapshots"], key=lambda r: r["observed_at"]
        )
        self.assertEqual(latest["delta_views"], -10)
        self.assertEqual(latest["views_per_hour"], -5.0)
        self.assertEqual(latest["delta_status"], "SOURCE_CORRECTION")
        national = next(
            r
            for r in g["social_engagement_daily_v1"]
            if r["admin_unit_id"] == "NATIONAL"
        )
        self.assertEqual(national["total_views"], 90)
        self.assertIsNone(national["total_likes"])
        self.assertIsNone(national["avg_likes_per_post"])

    def test_at07_lineage_multilabel_and_multiple_locations(self):
        raw = post(text="Tìm sân pickleball Hà Nội và Huế, giá bao nhiêu?")
        _s, g, _ = self.build({"bronze_social_posts": [raw]})
        national = next(
            r
            for r in g["social_activity_by_admin_version_daily"]
            if r["admin_unit_id"] == "NATIONAL"
        )
        self.assertEqual(national["unique_content_count"], 1)
        labels = {
            r["intent_type"]
            for r in g["social_intent_daily_v2"]
            if r["admin_unit_id"] == "NATIONAL"
        }
        self.assertEqual(labels, {"SEEK_COURT", "PRICE_INQUIRY"})
        self.assertIn(raw["observation_id"], national["lineage_ids"])
        self.assertTrue(
            national["metric_definition_id"].endswith(
                "social_activity_by_admin_version_daily"
            )
        )

    def test_replay_duplicate_observations_and_metric_only_nlp_reuse(self):
        raw = post()
        s, g, _ = self.build(
            {"bronze_social_posts": [raw], "bronze_engagement_snapshots": [metric()]}
        )
        again, gold, work = self.build(
            {
                "bronze_social_posts": [raw, copy.deepcopy(raw)],
                "bronze_engagement_snapshots": [metric()],
            },
            previous=s,
        )
        self.assertEqual(s, again)
        self.assertEqual(g, gold)
        self.assertEqual(work["content_classifications_processed"], 0)

    def test_conflicting_sources_and_similar_names_never_arbitrary_merge(self):
        s, _, _ = self.build(
            {
                "bronze_court_observations": [
                    court(),
                    court("b", platform="osm", count="8"),
                    court("other", address="456 Nguyễn Văn Linh, Đà Nẵng"),
                ]
            }
        )
        self.assertEqual(len(s["silver_court_venues"]), 2)
        conflicting = next(
            r
            for r in s["silver_court_venues"]
            if r["court_count_status"] == "CONFLICTING"
        )
        self.assertIsNone(conflicting["court_count"])
        self.assertIn("court_count", conflicting["conflicting_fields"])

    def test_ambiguous_persisted_venue_match(self):
        s, _, _ = self.build({"bronze_court_observations": [court()]})
        other = copy.deepcopy(s["silver_venue_candidates"][0])
        other["venue_id"] = "another-physical-venue"
        s["silver_venue_candidates"].append(other)
        newer, _, _ = self.build(
            {"bronze_court_observations": [court("another-source", platform="osm")]},
            previous=s,
        )
        self.assertEqual(
            newer["silver_venue_candidates"][0]["match_status"], "AMBIGUOUS"
        )
        self.assertIsNone(newer["silver_venue_candidates"][0]["venue_id"])

    def test_advertisement_not_demand_and_conservative_unknown(self):
        labels = classify(
            "Cho thuê sân pickleball, nhận đặt lịch, cần thuê sân liên hệ hotline 01234"
        )[0]["intent_labels"]
        self.assertIn("SUPPLY_ADVERTISEMENT", labels)
        self.assertNotIn("SEEK_COURT", labels)
        self.assertNotIn("BOOKING_INTENT", labels)
        unknown = classify("Có sân nào trống ở Huế không?")[0]
        self.assertIsNone(unknown["is_pickleball_related"])
        self.assertIsNone(unknown["classification_confidence"])

    def test_intent_participants_duration_time_budget_are_traceable(self):
        s, _, _ = self.build(
            {
                "bronze_social_posts": [
                    post(
                        text="Tìm sân pickleball Bình Dương, nhóm 4 người muốn thuê 2 tiếng tối thứ bảy, ngân sách 150k"
                    )
                ]
            }
        )
        row = s["silver_social_intent_evidence"][0]
        self.assertEqual(row["participant_count_mentioned"], 4)
        self.assertEqual(row["requested_duration_minutes"], 120)
        self.assertIsNone(row["requested_start_time"])
        self.assertEqual(row["price_budget_max_vnd"], 150000)
        self.assertFalse(row["actual_booking_confirmed"])

    def test_source_delete_late_event_expiry_preserves_other_source_evidence(self):
        tombstone = observation(
            "v1",
            platform="pbplus",
            kind="Change",
            change_type="deleted",
            entity_record_type="Venue",
            evidence_json='{"source_url":"https://synthetic.invalid/v1","http_status":410}',
        )
        rows, quality = active_observations(
            {
                "bronze_court_observations": [court(), court("osm1", platform="osm")],
                "bronze_source_changes": [tombstone],
            },
            as_of="2026-10-12T00:00:00Z",
        )
        s, _, _ = self.build(rows)
        self.assertEqual(s["silver_court_venues"][0]["source_count"], 1)
        self.assertEqual(quality["deleted_entities"], 1)
        expired = post(retention_expires_at_override="unused")
        expired.pop("retention_expires_at_override")
        expired["retention_expires_at"] = "2026-10-11T00:00:00Z"
        rows, _ = active_observations(
            {"bronze_social_posts": [expired]}, as_of="2026-10-12T00:00:00Z"
        )
        self.assertEqual(rows["bronze_social_posts"], [])

    def test_many_to_many_split_keeps_harmonized_ambiguity(self):
        from dsp_spark.processing.intelligence.geography import load_reference

        ref = load_reference()
        old = next(r for r in ref["locations"] if r["canonical_name"] == "Bình Dương")
        new = next(
            r
            for r in ref["locations"]
            if r["canonical_name"] == "Hà Nội" and r["admin_version"] == "2025_07"
        )
        # Explicitly synthetic mapping tests mechanics, not Vietnamese geography.
        ref["crosswalks"] = [
            {
                "old_admin_unit_id": old["geo_id"],
                "new_admin_unit_id": new["geo_id"],
                "relation_type": "SPLIT",
                "mapping_method": "SYNTHETIC_TEST",
                "source_reference": "synthetic://split-test",
            }
        ]
        geo = Geography(ref)
        r = geo.resolve("Bình Dương", AT)
        self.assertEqual(r["status"], "AMBIGUOUS")
        self.assertIsNone(r["new_id"])
        self.assertEqual(len(r["modes"]["HARMONIZED"]), 2)

    def test_vietnam_date_crosses_utc_day_and_no_historic_supply_fabrication(self):
        raw = post()
        raw["published_at"] = "2026-09-30T18:00:00Z"
        s, g, _ = self.build(
            {"bronze_social_posts": [raw], "bronze_court_observations": [court()]}
        )
        self.assertEqual(s["silver_social_posts"][0]["published_date_vn"], "2026-10-01")
        self.assertTrue(
            all(
                r["supply_as_of"] is None or r["supply_as_of"] <= "2026-10-31"
                for r in g["location_market_research_monthly_v1"]
            )
        )

    def test_parsers_preserve_unknown_and_scope(self):
        self.assertIsNone(court_count(None))
        self.assertIsNone(court_count("giờ 6"))
        self.assertEqual(court_count("0"), 0)
        self.assertEqual(court_count("6.0"), 6)
        self.assertEqual(
            price("100.000đ/giờ"), (100000.0, 100000.0, "VND_PER_COURT_HOUR")
        )
        self.assertEqual(price("100k/người"), (None, None, None))
        self.assertEqual(price("100k/giờ"), (100000.0, 100000.0, "VND_PER_COURT_HOUR"))

    def test_unversioned_locality_and_missing_admin_reference_remain_unresolved(self):
        s, _, _ = self.build(
            {
                "bronze_social_posts": [
                    post(text="Tìm sân pickleball ở Nha Trang, phường XYZ")
                ]
            }
        )
        mentions = s["silver_social_geo_mentions"]
        self.assertEqual(
            [r["location_text_raw"] for r in mentions], ["Nha Trang", "phường XYZ"]
        )
        self.assertTrue(all(r["resolution_status"] == "UNRESOLVED" for r in mentions))
        self.assertTrue(all(r["latitude"] is None for r in mentions))

    def test_venue_replay_preserves_first_candidate_match_status(self):
        data = {"bronze_court_observations": [court(), court("osm1", platform="osm")]}
        first, gold, _ = self.build(data)
        second, replay, _ = self.build(data, previous=first)
        self.assertEqual(first, second)
        self.assertEqual(gold, replay)

    def test_edited_social_venue_preserves_all_observation_evidence(self):
        first = post(
            text="Khai trương sân ABC Pickleball, 6 sân ngoài trời, tại 123 Nguyễn Văn Linh, Đà Nẵng"
        )
        later = post(
            text="Khai trương sân ABC Pickleball, 8 sân ngoài trời, tại 123 Nguyễn Văn Linh, Đà Nẵng",
            at="2026-10-11T00:00:00Z",
            run="synthetic-update",
        )
        s, _, _ = self.build({"bronze_social_posts": [first, later]})
        self.assertEqual(len(s["silver_social_posts"]), 1)
        self.assertEqual(len(s["silver_venue_evidence"]), 2)
        self.assertEqual(len(s["silver_court_venues"]), 1)
        self.assertEqual(s["silver_court_venues"][0]["court_count"], 8)
        self.assertEqual(len(s["silver_court_venues"][0]["evidence_ids"]), 2)

    def test_structured_amenity_false_is_preserved_and_known_ward_is_derived(self):
        s, _, _ = self.build(
            {
                "bronze_court_observations": [
                    court(
                        amenities_raw='{"parking":false,"lighting":true}',
                        address="123 ABC, phường Nghĩa Đô, Hà Nội",
                    )
                ]
            }
        )
        venue = s["silver_court_venues"][0]
        self.assertIs(venue["has_parking"], False)
        self.assertIs(venue["has_lighting"], True)
        self.assertTrue(venue["new_ward_id"].endswith("nghia-do"))

    def test_metric_only_update_does_not_invoke_text_classifier(self):
        data = {
            "bronze_social_posts": [post()],
            "bronze_engagement_snapshots": [metric()],
        }
        first, _, _ = self.build(data)
        data["bronze_engagement_snapshots"].append(
            metric(200, at="2026-10-11T00:00:00Z")
        )
        with patch(
            "dsp_spark.processing.intelligence.social.classify",
            side_effect=AssertionError("unnecessary classifier execution"),
        ):
            second, gold, work = self.build(data, previous=first)
        self.assertEqual(work["content_classifications_processed"], 0)
        self.assertEqual(second["silver_social_posts"], first["silver_social_posts"])
        national = next(
            r
            for r in gold["social_engagement_daily_v1"]
            if r["admin_unit_id"] == "NATIONAL"
        )
        self.assertEqual(national["total_views"], 200)

    def test_interrupted_content_evidence_cache_is_repaired(self):
        original = {"bronze_social_posts": [post()]}
        first, _, _ = self.build(original)
        edited = {
            "bronze_social_posts": [
                post(text="Sân ABC Pickleball đã ngừng kinh doanh ở Huế")
            ]
        }
        expected, _, _ = self.build(edited)
        partial = copy.deepcopy(first)
        partial["silver_social_posts"] = expected["silver_social_posts"]
        repaired, _, _ = self.build(edited, previous=partial)
        self.assertEqual(
            repaired["silver_social_intent_evidence"],
            expected["silver_social_intent_evidence"],
        )


if __name__ == "__main__":
    unittest.main()
