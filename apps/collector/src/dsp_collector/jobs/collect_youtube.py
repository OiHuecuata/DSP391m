"""Immutable replayable YouTube Bronze and normalized SocialPost batches."""

import json
from pathlib import Path

from ..normalizers.youtube import normalize_youtube
from ..publishers.kafka import build_social_post_event
from ..runtime.artifacts import write_json_atomic
from ..sources.api.youtube import collect_youtube
from .collect_social import CollectionResult, _run_id


def process_youtube_envelope(
    envelope, *, run_id=None, artifact_root, observation_context=None
):
    from ..normalizers.observation import (
        native_batch_metadata,
        prepare_envelope,
        validate_native_input,
    )

    identity = _run_id(run_id or envelope.get("run_id"))
    if envelope.get("run_id") is not None and envelope["run_id"] != identity:
        raise ValueError("run_id conflicts with saved YouTube acquisition")
    bronze = {**prepare_envelope(envelope, observation_context), "run_id": identity}
    root = Path(artifact_root)
    raw = root / "bronze/youtube" / f"{identity}.json"
    normalized = root / "silver/social_posts" / f"{identity}.json"
    validate_native_input(bronze, raw)
    if raw.exists():
        if json.loads(raw.read_text()) != bronze:
            raise FileExistsError("Different YouTube Bronze exists")
    else:
        write_json_atomic(raw, bronze)
    if normalized.exists():
        batch = json.loads(normalized.read_text())
        for name, value in native_batch_metadata(bronze, raw).items():
            if batch.get(name) != value:
                raise ValueError("Saved native YouTube lineage metadata disagrees")
        return CollectionResult(
            identity,
            batch["status"],
            raw,
            normalized,
            batch["count"],
            batch["rejected_count"],
        )
    records, errors, seen = [], [], set()
    for payload, parent in [(v, None) for v in bronze["videos"]] + [
        (c["payload"], c["video_id"]) for c in bronze["comments"]
    ]:
        try:
            record = normalize_youtube(
                payload,
                run_id=identity,
                collected_at=bronze["collected_at"],
                queries=bronze["video_queries"].get(parent or payload["id"], []),
                video_id=parent,
            )
            build_social_post_event(record)
            if record["record_id"] not in seen:
                records.append(record)
                seen.add(record["record_id"])
        except (ValueError, TypeError, KeyError):
            errors.append({"reason": "invalid_source_record"})
    source_errors = [
        e for e in bronze["errors"] if e.get("reason") != "commentsDisabled"
    ]
    status = (
        "partial"
        if records and (errors or source_errors)
        else "failed" if errors or source_errors else "success"
    )
    write_json_atomic(
        normalized,
        {
            **native_batch_metadata(bronze, raw),
            "schema_version": "1",
            "source": "youtube",
            "run_id": identity,
            "collected_at": bronze["collected_at"],
            "count": len(records),
            "status": status,
            "rejected_count": len(errors),
            "normalization_errors": errors,
            "records": records,
        },
    )
    return CollectionResult(
        identity, status, raw, normalized, len(records), len(errors)
    )


def collect(api_key, configuration, *, run_id, artifact_root, progress=None):
    envelope = collect_youtube(api_key, configuration, progress=progress)
    return process_youtube_envelope(
        envelope, run_id=run_id, artifact_root=artifact_root
    )
