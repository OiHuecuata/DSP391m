"""Immutable full rebuild generations using the existing business transforms."""

import hashlib
import os
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from dsp_collector.runtime.bronze import DATASETS

from ...io.lakehouse_control import writable_root
from ...processing.clean.contracts import contract_documents
from ...processing.intelligence.catalog import GOLD, SILVER, VERSION
from ...processing.intelligence.common import (
    active_observations,
    content_id,
    digest,
    instant,
    vn_date,
)
from ...processing.intelligence.geography import load_reference
from ...processing.intelligence.quality import table_digest, validate_tables
from ...runtime.config import LakehousePaths
from .build_intelligence import build_intelligence, delta_version, rows_from_delta


def code_digest():
    root = Path(__file__).resolve().parents[2]
    return (
        "sha256:"
        + hashlib.sha256(
            b"".join(
                p.relative_to(root).as_posix().encode() + b"\0" + p.read_bytes()
                for p in sorted(root.rglob("*.py"))
            )
        ).hexdigest()
    )


def version_vector(spark, paths):
    vector = {n: delta_version(spark, getattr(paths, n)) for n in DATASETS}
    if any(v is None for v in vector.values()):
        raise ValueError("all six authoritative Bronze datasets must exist")
    return vector


def generation_settings(settings, base, generation_id):
    output = LakehousePaths(base.rstrip("/") + "/generations/" + generation_id)
    paths = replace(output, **{n: getattr(settings.paths, n) for n in DATASETS})
    return replace(settings, paths=paths)


def verify_generation(spark, settings, report):
    tables = {}
    for name in (*SILVER, *GOLD):
        path = report["paths"][name]
        if delta_version(spark, path) != report["output_versions"][name]:
            raise ValueError(f"generation version changed: {name}")
        rows = rows_from_delta(spark, path, version=report["output_versions"][name])
        if table_digest(rows) != report["after"][name]["digest"]:
            raise ValueError(f"generation read-back differs: {name}")
        tables[name] = rows
    validate_tables(tables)
    return tables


def reconciliation(inputs, eligible, outputs):
    """Explain each eligible ID with lineage or explicit control/orphan handling."""
    linked = {i for rows in outputs.values() for r in rows for i in r["lineage_ids"]}
    contents = {
        r["content_id"]
        for n in ("silver_social_posts", "silver_social_comments")
        for r in outputs[n]
    }
    report = {}
    for name in DATASETS:
        ids = {r["observation_id"] for r in eligible[name]}
        handled = ids & linked
        explanations = {"lineage": len(handled)}
        if name in ("bronze_collection_runs", "bronze_source_changes"):
            # Runs feed coverage, including unsuccessful/empty scopes. Changes
            # feed lifecycle filtering even when no surviving metric cites them.
            remaining = ids - handled
            explanations[
                "coverage_control" if name.endswith("runs") else "lifecycle_control"
            ] = len(remaining)
            handled |= remaining
        if name == "bronze_engagement_snapshots":
            orphans = {
                r["observation_id"]
                for r in eligible[name]
                if content_id(
                    r["source_platform"],
                    r["native_content_id"],
                    comment=r["content_type"] == "comment",
                )
                not in contents
            }
            explanations["orphan_snapshot_retained_in_bronze"] = len(orphans - handled)
            handled |= orphans
        report[name] = {
            "input_rows": len(inputs[name]),
            "eligible": len(eligible[name]),
            "excluded": len(inputs[name]) - len(eligible[name]),
            "consumed_or_handled": len(handled),
            "unexplained": len(ids - handled),
            "handling": explanations,
        }
        if report[name]["unexplained"]:
            raise ValueError(f"unexplained eligible Bronze input loss: {name}")
    return report


def build_generation(spark, settings, control, *, policies, as_of=None, force=False):
    """Caller owns the lakehouse writer lock. ClickHouse is outside this job."""
    base = writable_root(settings.paths.base)
    as_of = instant(as_of or datetime.now(UTC))
    vector = version_vector(spark, settings.paths)
    reference = load_reference(os.getenv("DSP_ADMIN_REFERENCE_PATH"))
    code_version = code_digest()
    signature = digest(
        [
            vector,
            policies,
            reference,
            code_version,
            contract_documents(),
            vn_date(as_of),
        ]
    )
    pointer, pointer_etag = control.get("current.json")
    if pointer and pointer.get("paused"):
        raise ValueError("processing is paused after rollback")
    previous = None
    if pointer:
        previous, _ = control.get(pointer["manifest_key"])
        if not previous or previous["status"] != "VALIDATED":
            raise ValueError("current pointer references an invalid generation")
        cutoff = previous.get("next_retention_cutoff")
        eligibility_cutoff = previous.get("next_eligibility_cutoff")
        forward = as_of >= instant(previous["as_of"])
        if (
            not force
            and forward
            and pointer["signature"] == signature
            and (not cutoff or instant(cutoff) > as_of)
            and (not eligibility_cutoff or instant(eligibility_cutoff) > as_of)
        ):
            verify_generation(spark, settings, previous)
            return {**previous, "execution": "UNCHANGED_PINNED_INPUT"}
    generation_id = as_of.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:12]
    target = generation_settings(settings, base, generation_id)
    previous_settings = (
        generation_settings(settings, base, previous["generation_id"])
        if previous
        else None
    )
    seed, _ = control.get("bootstrap.json")
    seed_versions = (
        seed.get("historical_output_versions") if seed and not previous else None
    )
    if seed_versions:
        if set(seed_versions) != set(SILVER + GOLD):
            raise ValueError(
                "historical output seed must pin all 23 validated snapshot tables"
            )
        # Reuse only historical caches/supply snapshots through the existing
        # eligibility and lifecycle logic, never copying Delta files or treating
        # old Silver as the current output generation.
        previous_settings = replace(
            settings, paths=LakehousePaths(seed["historical_root"])
        )
    if previous:
        verify_generation(spark, settings, previous)
    report = build_intelligence(
        spark,
        target,
        as_of=as_of,
        reference=reference,
        policies=policies,
        input_versions=vector,
        previous_settings=previous_settings,
        previous_versions=seed_versions,
        force=True,
    )
    report.update(
        generation_id=generation_id,
        mode="FULL_REBUILD",
        processing_timestamp=datetime.now(UTC).isoformat(),
        code_digest=code_version,
        reference_digest=digest(reference),
        paths={n: getattr(target.paths, n) for n in (*SILVER, *GOLD)},
        processing_version=VERSION,
        rollback_target=previous["generation_id"] if previous else None,
        historical_seed=(
            {"root": seed["historical_root"], "output_versions": seed_versions}
            if seed_versions
            else None
        ),
    )
    outputs = verify_generation(spark, target, report)
    inputs = {
        n: rows_from_delta(spark, getattr(settings.paths, n), version=vector[n])
        for n in DATASETS
    }
    future_instants = [
        instant(r[field])
        for name, field in (
            ("bronze_social_posts", "published_at"),
            ("bronze_social_comments", "published_at"),
            ("bronze_engagement_snapshots", "observed_at"),
        )
        for r in inputs[name]
        if r.get(field) and instant(r[field]) > as_of
    ]
    report["next_eligibility_cutoff"] = (
        min(future_instants).isoformat() if future_instants else None
    )
    eligible, _ = active_observations(inputs, as_of=as_of, policies=policies)
    report["reconciliation"] = reconciliation(inputs, eligible, outputs)
    report["schemas"] = {
        n: spark.read.format("delta")
        .option("versionAsOf", report["output_versions"][n])
        .load(report["paths"][n])
        .schema.jsonValue()
        for n in (*SILVER, *GOLD)
    }
    # Validate and write the immutable manifest first. A failed build never
    # replaces any prior generation or advances the authoritative watermark.
    key = "generations/" + generation_id + ".json"
    control.put(key, report, absent=True)
    control.put(
        "current.json",
        {
            "generation_id": generation_id,
            "manifest_key": key,
            "input_versions": vector,
            "signature": signature,
            "rollback_target": report["rollback_target"],
        },
        expected=pointer_etag,
        absent=pointer is None,
    )
    return report


def rollback(spark, settings, control, generation_id):
    report, _ = control.get("generations/" + generation_id + ".json")
    if not report or report["status"] != "VALIDATED":
        raise ValueError("rollback requires a validated retained manifest")
    verify_generation(spark, settings, report)
    current, etag = control.get("current.json")
    control.put(
        "current.json",
        {
            "generation_id": generation_id,
            "manifest_key": "generations/" + generation_id + ".json",
            "input_versions": report["input_versions"],
            "signature": "ROLLBACK_REQUIRES_EXPLICIT_RESUME",
            "paused": True,
            "rollback_target": current["generation_id"] if current else None,
        },
        expected=etag,
        absent=current is None,
    )
    return report
