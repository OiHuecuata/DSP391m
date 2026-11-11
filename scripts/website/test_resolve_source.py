import unittest
from types import SimpleNamespace
from unittest.mock import patch
from resolve_source import resolve

class ResolveSourceTests(unittest.TestCase):
    def setUp(self):
        self.args = SimpleNamespace(base='auto', aws='aws', profile='tracker', region='ap-southeast-2')

    @patch('resolve_source.aws_json')
    def test_latest_bootstrap_excludes_other_prefixes(self, read):
        read.return_value={'CommonPrefixes':[{'Prefix':'bootstrap/20261028T064025Z/'},{'Prefix':'bootstrap/20261108T080000Z/'},{'Prefix':'bootstrap/test/'}]}
        self.assertEqual(resolve(self.args),('s3://dsp391m-data-lake/bootstrap/20261108T080000Z/lakehouse',None))

    @patch('resolve_source.aws_json')
    def test_generation_pins_validated_table_versions(self, read):
        self.args.base='s3://dsp391m-data-lake/staging/live'
        manifest={'generation_id':'abc','status':'VALIDATED','paths':{},'output_versions':{}}
        for name in ('courts','social_posts','social_comments'):
            manifest['paths']['silver_'+name]=self.args.base+'/generations/abc/silver/'+name
            manifest['output_versions']['silver_'+name]=2
        read.side_effect=[{'generation_id':'abc','manifest_key':'generations/abc.json'},manifest]
        _,(_,versions)=resolve(self.args)
        self.assertEqual(versions,{'courts':2,'social_posts':2,'social_comments':2})

    @patch('resolve_source.aws_json')
    def test_unvalidated_generation_is_rejected(self, read):
        self.args.base='s3://dsp391m-data-lake/staging/live'
        read.side_effect=[{'generation_id':'abc','manifest_key':'generations/abc.json'},{'generation_id':'abc','status':'BUILDING'}]
        with self.assertRaises(ValueError):resolve(self.args)

    @patch('resolve_source.aws_json')
    def test_auth_error_does_not_fall_back_to_old_export(self, read):
        read.side_effect=RuntimeError('expired')
        with self.assertRaises(RuntimeError):resolve(self.args)
