"""Managed Spark adapter for the same admitted Threads processing engine."""

import argparse
import os

from dsp_collector.normalizers.observation import canonical_json, parse_json
from dsp_collector.runtime.durable import ArtifactStore

from orchestration.local.threads_vertical import process


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control-base-uri", required=True)
    parser.add_argument("--config-uri", required=True)
    parser.add_argument("--manifest-uri", required=True)
    args = parser.parse_args(argv)
    if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
        raise RuntimeError("Managed Databricks runtime required")
    store = ArtifactStore(args.control_base_uri)
    config = parse_json(store.read(args.config_uri))
    if not all(
        config[k].startswith("s3a://")
        for k in ("lakehouse_base_uri", "control_base_uri")
    ) or not config["acquisition"]["raw_base_uri"].startswith("s3a://"):
        raise ValueError("Managed operational paths must use S3A")
    print(canonical_json(process(config, manifest_uri=args.manifest_uri)))
