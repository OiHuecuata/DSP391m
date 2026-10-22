#!/bin/sh
set -eu
superset db upgrade
python /app/dsp-superset/ensure_admin.py
superset init
