#!/usr/bin/env bash
set -euo pipefail
tracker_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
mkdir -p "$tracker_repo_root/.local/website-data"
exec 9>"$tracker_repo_root/.local/website-data/refresh.lock"
flock -n 9 || exit 0
tracker_aws_cli="${TRACKER_AWS_CLI:-aws}"
if ! command -v "$tracker_aws_cli" >/dev/null 2>&1 && [[ -x "$HOME/Documents/Codex/tools/bin/aws" ]]; then
  tracker_aws_cli="$HOME/Documents/Codex/tools/bin/aws"
fi
tracker_python="$tracker_repo_root/.local/website-s3-venv/bin/python"
if [[ ! -x "$tracker_python" ]]; then
  echo 'Install the reader first: python3 -m venv .local/website-s3-venv'
  echo '.local/website-s3-venv/bin/pip install -r scripts/website/requirements.txt'
  exit 1
fi
tracker_profile="${TRACKER_AWS_PROFILE:-}"
if [[ -z "$tracker_profile" ]]; then
  tracker_identity="$tracker_repo_root/.local/website-data/aws-identity.json"
  if [[ -f "$tracker_identity" ]]; then
    tracker_profile="$("$tracker_python" -c 'import json,re,sys; v=json.load(open(sys.argv[1]))["profile"]; assert isinstance(v,str) and re.fullmatch(r"[-A-Za-z0-9_.@]{1,128}",v), "Invalid website AWS profile"; print(v)' "$tracker_identity")"
  else
    tracker_profile=tracker
  fi
fi
if ! "$tracker_aws_cli" sts get-caller-identity --profile "${tracker_profile}" --region "${AWS_REGION:-ap-southeast-2}" --output json >/dev/null 2>&1; then
  printf 'AWS profile %q is unavailable. Check the website credential setup. CLI: %q; region: %q\n' "${tracker_profile}" "$tracker_aws_cli" "${AWS_REGION:-ap-southeast-2}"
  exit 1
fi
exec "$tracker_python" "$tracker_repo_root/scripts/website/sync_s3.py" --aws "$tracker_aws_cli" --profile "${tracker_profile}" --region "${AWS_REGION:-ap-southeast-2}" --base "${TRACKER_S3_BASE:-auto}"
