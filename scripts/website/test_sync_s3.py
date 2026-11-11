import unittest
from sync_s3 import unique_latest, court_posts, social_records, collection_history, engagement_history

class SnapshotTests(unittest.TestCase):
    def test_latest_observation_wins_without_combining_sources(self):
        rows = [dict(source="threads", source_record_id="1", collected_at="2026-10-01",text="old"),
                dict(source="threads", source_record_id="1", collected_at="2026-10-02",text="new"),
                dict(source="youtube", source_record_id="1", collected_at="2026-10-01",text="video")]
        result = unique_latest(rows)
        self.assertEqual(len(result),2)
        self.assertIn("new", [r["text"] for r in result])
        self.assertNotIn("old", [r["text"] for r in result])
    def test_history_preserves_repeats_and_groups_collection_time_in_ict(self):
        rows = [dict(source="threads", source_record_id="1", collected_at="2026-10-01T18:00:00Z"),
                dict(source="threads", source_record_id="1", collected_at="2026-10-02T02:00:00Z"),
                dict(source="youtube", collected_at="invalid"),
                dict(source="threads", collected_at="2026-10-02T02:00:00")]
        result = collection_history({"social_posts": rows, "social_comments": [dict(source="youtube_comment", collected_at="2026-10-01T18:00:00Z")]})
        self.assertEqual(result["days"], [{"date": "2026-10-02", "counts": {"threads": 2, "youtube": 1, "pbplus": 0}}])
        self.assertEqual(result["skippedUndated"], 2)
    def test_engagement_keeps_latest_item_per_day_and_preserves_missing_metrics(self):
        rows=[dict(source="threads",source_record_id="1",collected_at="2026-10-01T18:00:00Z",like_count=3),dict(source="threads",source_record_id="1",collected_at="2026-10-02T02:00:00Z",like_count=7),dict(source="threads",source_record_id="2",collected_at="2026-10-02T03:00:00Z",like_count=2)]
        point=engagement_history(rows)["threads"][0]
        self.assertEqual(point["date"],"2026-10-02");self.assertEqual(point["items"],2);self.assertEqual(point["likes"],9)
        self.assertEqual(point["likesReported"],2);self.assertIsNone(point["views"])
        self.assertEqual(point["viewsReported"],0)
    def test_coordinates_require_matching_id_and_address(self):
        row = dict(source="pbplus",record_id="p:1",source_record_id="1",address="Hà Nội",collected_at="2026-10-01")
        posts, count = court_posts([row],[dict(id="1",address="Ha Noi",latitude=21,longitude=105)])
        self.assertEqual(count,1); self.assertEqual(posts[0]["coordinate_source"],"local_geocoding")
        posts, count = court_posts([row],[dict(id="1",address="Different address",latitude=21,longitude=105)])
        self.assertEqual(count,0); self.assertIsNone(posts[0]["latitude"])
    def test_social_export_omits_private_transport_fields_and_unsafe_links(self):
        rows = [dict(source="threads",record_id="t:1",source_record_id="1",source_url="javascript:alert(1)",text="example",author_id="private",source_artifact_uri="file:///private")]
        records = social_records(rows,{"threads"})
        self.assertEqual(len(records),1);self.assertIsNone(records[0]["url"])
        self.assertNotIn("author_id",records[0]);self.assertNotIn("source_artifact_uri",records[0])

if __name__ == "__main__": unittest.main()
