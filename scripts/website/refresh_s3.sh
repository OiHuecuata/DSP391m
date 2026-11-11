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
if ! "$tracker_aws_cli" sts get-caller-identity --profile "${TRACKER_AWS_PROFILE:-tracker}" --region "${AWS_REGION:-ap-southeast-2}" --output json >/dev/null 2>&1; then
  printf 'AWS sign-in is required. Run: %q login --profile %q --region %q\n' "$tracker_aws_cli" "${TRACKER_AWS_PROFILE:-tracker}" "${AWS_REGION:-ap-southeast-2}"
  exit 1
fi
exec "$tracker_python" "$tracker_repo_root/scripts/website/sync_s3.py" --aws "$tracker_aws_cli" --profile "${TRACKER_AWS_PROFILE:-tracker}" --region "${AWS_REGION:-ap-southeast-2}" --base "${TRACKER_S3_BASE:-auto}"
