"""Resolve the newest immutable bootstrap export, or a pinned managed generation."""
import json
import re
import subprocess
from urllib.parse import urlparse


def aws_json(args, *command):
    result = subprocess.run([args.aws, *command, '--profile', args.profile, '--region', args.region, '--output', 'json'], check=True, capture_output=True, text=True, timeout=45)
    return json.loads(result.stdout)


def resolve(args):
    if args.base != 'auto':
        base = args.base.rstrip('/')
        parsed = urlparse(base)
        if parsed.scheme != 's3' or not parsed.netloc or '..' in parsed.path.split('/'):
            raise ValueError('Invalid S3 source')
        if '/bootstrap/' in base:
            return base, None
        prefix = parsed.path.strip('/')
        pointer = aws_json(args, 's3', 'cp', base + '/control/current.json', '-')
        key = pointer['manifest_key']
        if not re.fullmatch(r'generations/[A-Za-z0-9_-]+\.json', key):
            raise ValueError('Invalid generation manifest key')
        manifest = aws_json(args, 's3', 'cp', base + '/control/' + key, '-')
        if manifest.get('status') != 'VALIDATED' or manifest.get('generation_id') != pointer.get('generation_id'):
            raise ValueError('Generation is not validated')
        paths, versions = {}, {}
        for name in ('courts', 'social_posts', 'social_comments'):
            table = 'silver_' + name
            uri = manifest['paths'][table].replace('s3a://', 's3://', 1).rstrip('/')
            table_uri = urlparse(uri)
            if table_uri.netloc != parsed.netloc or not table_uri.path.startswith('/' + prefix + '/') or '..' in table_uri.path.split('/'):
                raise ValueError('Generation path outside configured source')
            version = manifest['output_versions'][table]
            if not isinstance(version, int) or version < 0:
                raise ValueError('Invalid pinned version')
            paths[name], versions[name] = uri, version
        return base, (paths, versions)
    result = aws_json(args, 's3api', 'list-objects-v2', '--bucket', 'dsp391m-data-lake', '--prefix', 'bootstrap/', '--delimiter', '/')
    candidates = [r['Prefix'] for r in result.get('CommonPrefixes', []) if re.fullmatch(r'bootstrap/\d{8}T\d{6}Z/', r['Prefix'])]
    if not candidates:
        raise ValueError('No bootstrap exports available')
    return 's3://dsp391m-data-lake/' + max(candidates) + 'lakehouse', None
