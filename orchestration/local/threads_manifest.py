"""Versioned ProcessingManifest/ack control for file development and S3 targets.

v1 local target canonicalization is unchanged. These requests keep its field and
stage model and add explicitly versioned portable target identity validation.
"""

from datetime import UTC, datetime
from functools import lru_cache
from importlib.resources import files
from pathlib import Path

from dsp_collector.normalizers.observation import (
    canonical_json,
    digest,
    parse_json,
    timestamp,
)
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

from .processing_manifest import _ack_lineage, _logical_request, _reference, _uri


@lru_cache(maxsize=1)
def validators():
    name = "orchestration/processing-manifest.v2.schema.json"
    root = next(
        (
            p / "contracts"
            for p in Path(__file__).resolve().parents
            if (p / "contracts" / name).is_file()
        ),
        None,
    )
    root = root or files("dsp_contracts")
    docs = {
        kind: parse_json(
            (root / f"orchestration/processing-{kind}.v2.schema.json").read_bytes()
        )
        for kind in ("manifest", "ack")
    }
    registry = Registry().with_resources(
        (d["$id"], Resource.from_contents(d)) for d in docs.values()
    )
    return {
        kind: Draft202012Validator(
            doc, registry=registry, format_checker=FormatChecker()
        )
        for kind, doc in docs.items()
    }


def validate(value, *, targets=None):
    validators()["manifest"].validate(value)
    for reference in value["inputs"] + value["ingestion_refs"]:
        _reference(reference)
    _uri(value["targets"]["base_uri"])
    if value["targets"]["resources"] != sorted(set(value["targets"]["resources"])):
        raise ValueError("Targets must be sorted and unique")
    if value["target_identity"] != "sha256:" + digest(
        ["processing-targets.v2", value["targets"]]
    ):
        raise ValueError("Processing target identity mismatch")
    if targets is not None and value["targets"] != targets:
        raise ValueError("Processing targets differ from the admitted handoff")
    if value["processing_spec_hash"] != "sha256:" + digest(
        value["processing_spec"]
    ) or value["manifest_id"] != "sha256:" + digest(_logical_request(value)):
        raise ValueError("Manifest specification or identity mismatch")
    if value["payload_sha256"] != digest(
        {k: v for k, v in value.items() if k != "payload_sha256"}
    ):
        raise ValueError("Manifest payload hash mismatch")
    runs = value["acquisition"]["runs"]
    if (
        sum(r["accepted_records"] for r in runs)
        != value["acquisition"]["accepted_records"]
    ):
        raise ValueError("Acquisition accepted count mismatch")
    keys = {(r["source_platform"], r["collection_run_id"]) for r in runs}
    if any(
        (r["source_platform"], r["collection_run_id"]) not in keys
        for r in value["inputs"] + value["ingestion_refs"]
    ):
        raise ValueError("Unadmitted acquisition lineage")
    ids = value["metadata"]["event_ids"]
    if ids != sorted(set(ids)) or len(ids) != value["acquisition"]["accepted_records"]:
        raise ValueError("Handoff native identities differ from acquisition count")
    return value


def build(*, inputs, ingestion_refs, acquisition, spec, targets, created_at, metadata):
    value = {
        "manifest_version": "2",
        "manifest_id": "",
        "processing_spec_hash": "sha256:" + digest(spec),
        "payload_sha256": "",
        "input_order": "UNORDERED",
        "inputs": sorted(inputs, key=canonical_json),
        "ingestion_refs": sorted(ingestion_refs, key=canonical_json),
        "acquisition": acquisition,
        "target_identity": "sha256:" + digest(["processing-targets.v2", targets]),
        "targets": targets,
        "processing_spec": spec,
        "readiness": "PARTIAL_READY",
        "created_at": created_at,
        "metadata": metadata,
    }
    value["manifest_id"] = "sha256:" + digest(_logical_request(value))
    value["payload_sha256"] = digest(
        {k: v for k, v in value.items() if k != "payload_sha256"}
    )
    return validate(value)


def ack(manifest, *, attempt, stage, outcome, error=None, generation=None):
    validate(manifest)
    value = {
        "ack_version": "2",
        "ack_id": "",
        "manifest_id": manifest["manifest_id"],
        "attempt_id": attempt,
        "stage": stage,
        "outcome": outcome,
        "collection_outcome": manifest["acquisition"]["outcome"],
        "started_at": None,
        "completed_at": timestamp(datetime.now(UTC)),
        "input_refs": manifest["inputs"] + manifest["ingestion_refs"],
        "output_refs": [],
        "error": error,
        "owner_reference": None,
        "publication_generation_reference": generation,
    }
    value["ack_id"] = "sha256:" + digest(
        {k: v for k, v in value.items() if k != "ack_id"}
    )
    validators()["ack"].validate(value)
    _ack_lineage(value, manifest)
    return value
