import unittest
from sync_s3 import snapshot_changes

def snapshot(posts,stamp='2026-11-01T00:00:00Z'):
    return {'posts':posts,'synced_at':stamp,'social':{'threads':[],'youtube':[],'youtube_comments':[]}}

class ChangesTests(unittest.TestCase):
    def test_added_updated_removed_are_distinct(self):
        old=snapshot([{'id':'same','address':'A'},{'id':'updated','address':'B'},{'id':'removed'}])
        new=snapshot([{'id':'same','address':'A'},{'id':'updated','address':'C'},{'id':'added'}])
        self.assertEqual(snapshot_changes(old,new)['sources'][0],{'id':'courts','added':1,'updated':1,'removed':1})
    def test_recollection_is_not_an_update(self):
        old=snapshot([{'id':'same','collectedAt':'old','likes':2}])
        new=snapshot([{'id':'same','collectedAt':'new','likes':2}])
        self.assertEqual(snapshot_changes(old,new)['sources'][0]['updated'],0)
    def test_no_baseline_is_unavailable(self):
        self.assertIsNone(snapshot_changes(None,snapshot([])))
