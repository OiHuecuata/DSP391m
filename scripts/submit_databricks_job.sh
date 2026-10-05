#!/bin/sh
# Configuration/authentication stay in environment/profile; never echo secrets.
set -eu
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
PYTHONPATH="pipelines/spark/src:apps/collector/src${PYTHONPATH:+:$PYTHONPATH}" \
    poetry run python infra/databricks/submit_social_gold.py "$@"
