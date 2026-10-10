"""Collect/replay PBPlus into local Bronze and Silver, with optional geography.

Bronze is committed before enrichment. Replay accepts an already-enriched
envelope and performs no network I/O. Entity rules belong to normalize_court.
Matching Bronze can be reused after a failed downstream step; Silver is immutable.
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from ..normalizers.court import normalize_court
from ..runtime.artifacts import write_json_atomic as _write_json_atomic
from ..runtime.config import default_artifact_root
from ..runtime.status import CrawlerStatusTracker

LOGGER = logging.getLogger(__name__)
GEO_FIELDS = (
    "address",
    "address_source",
    "latitude",
    "longitude",
    "geocode_status",
    "geocode_matched_address",
    "geocode_country",
    "geocode_expected_region",
    "geocode_error",
)
GEO_FAILURES = frozenset(
    {"missing_address", "not_found", "outside_vietnam", "region_mismatch", "error"}
)


@dataclass(frozen=True, slots=True)
class CollectionResult:
    run_id: str
    status: Literal["success", "partial", "failed"]
    bronze_path: Path
    silver_path: Path
    count: int
    rejected_count: int


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _validate_envelope(envelope: Mapping[str, Any]) -> None:
    if not isinstance(envelope, Mapping):
        raise TypeError("envelope must be a mapping")
    if envelope.get("source") != "pbplus":
        raise ValueError("source must be pbplus")
    if (
        not isinstance(envelope.get("seed_query"), str)
        or not envelope["seed_query"].strip()
    ):
        raise ValueError("seed_query must be a nonempty string")
    if envelope.get("sort") != "directory":
        raise ValueError("sort must be directory")
    posts = envelope.get("posts")
    if not isinstance(posts, list) or any(not isinstance(post, dict) for post in posts):
        raise ValueError("posts must be a list of crawler record objects")
    if type(envelope.get("count")) is not int or envelope["count"] != len(posts):
        raise ValueError("count must equal the number of crawler posts")
    query_runs = envelope.get("query_runs")
    if not isinstance(query_runs, list) or any(
        not isinstance(item, dict) for item in query_runs
    ):
        raise ValueError("query_runs must be a list of objects")
    for field in ("collected_at", "geocoded_at"):
        value = envelope.get(field)
        if field == "geocoded_at" and value is None:
            continue
        if (
            not isinstance(value, str)
            or datetime.fromisoformat(value).utcoffset() is None
        ):
            raise ValueError(f"{field} must be a timezone-aware datetime string")
    if envelope.get("schema_version", "1") != "1":
        raise ValueError("unsupported Bronze schema_version")


def _persist_bronze(
    envelope: Mapping[str, Any],
    run_id: str | None,
    artifact_root: str | Path | None,
) -> tuple[dict[str, Any], Path, Path]:
    _validate_envelope(envelope)
    saved_run_id = envelope.get("run_id")
    if saved_run_id is not None and run_id is not None and saved_run_id != run_id:
        raise ValueError("run_id conflicts with the saved Bronze run")
    selected = run_id if run_id is not None else saved_run_id
    selected = uuid4().hex if selected is None else selected
    if (
        not isinstance(selected, str)
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", selected) is None
    ):
        raise ValueError(
            "run_id must be a safe filename identifier of 1..128 characters"
        )
    root = (
        Path(artifact_root).expanduser()
        if artifact_root is not None
        else default_artifact_root()
    )
    bronze_path = root / "bronze" / "pbplus" / f"{selected}.json"
    silver_path = root / "silver" / "courts" / f"{selected}.json"
    if silver_path.exists():
        raise FileExistsError(f"Silver artifact already exists: {silver_path}")
    bronze = copy.deepcopy(dict(envelope))
    bronze.update(schema_version="1", run_id=selected)
    from ..normalizers.observation import validate_native_input

    validate_native_input(bronze, bronze_path)
    if bronze_path.exists():
        if json.loads(bronze_path.read_text(encoding="utf-8")) != bronze:
            raise FileExistsError(
                f"different Bronze artifact already exists: {bronze_path}"
            )
    else:
        _write_json_atomic(bronze_path, bronze)
    return bronze, bronze_path, silver_path


def _normalization_input(
    bronze: dict[str, Any], enriched: Mapping[str, Any] | None
) -> dict[str, Any]:
    """Accept geo fields only, with unchanged batch and per-record lineage."""
    payload = copy.deepcopy(bronze)
    if enriched is None:
        return payload
    _validate_envelope(enriched)
    for field in ("source", "seed_query", "sort", "collected_at", "count"):
        if enriched[field] != bronze[field]:
            raise ValueError(f"enriched envelope conflicts with Bronze {field}")
    if enriched.get("run_id", bronze["run_id"]) != bronze["run_id"]:
        raise ValueError("enriched envelope conflicts with Bronze run_id")
    for raw, supplied in zip(payload["posts"], enriched["posts"]):
        if any(raw.get(key) != supplied.get(key) for key in ("id", "url")):
            raise ValueError("enriched court identity/order conflicts with Bronze")
        for field in GEO_FIELDS:
            if field in supplied:
                raw[field] = copy.deepcopy(supplied[field])
    if "geocoded_at" in enriched:
        payload["geocoded_at"] = enriched["geocoded_at"]
    return payload


def process_pbplus_envelope(
    envelope: Mapping[str, Any],
    *,
    run_id: str | None = None,
    artifact_root: str | Path | None = None,
    enriched_envelope: Mapping[str, Any] | None = None,
    observation_context: Mapping[str, Any] | None = None,
) -> CollectionResult:
    """Write/reuse Bronze and normalize offline, optionally using supplied geo.

    Supplied enrichment uses the existing geo helper's envelope shape. It must
    retain collection metadata, record order and URL identity. Only verified
    geo fields are overlaid; acquisition fields always come from raw Bronze.
    No crawler or geocoder is invoked or imported by this processing boundary.
    """
    from ..normalizers.observation import native_batch_metadata, prepare_envelope

    envelope = prepare_envelope(envelope, observation_context)
    bronze, bronze_path, silver_path = _persist_bronze(envelope, run_id, artifact_root)
    payload = _normalization_input(bronze, enriched_envelope)
    selected = bronze["run_id"]
    records = []
    errors = []
    enrichment_errors = []
    outcome_counts: dict[str, int] = {}
    for index, post in enumerate(payload["posts"]):
        source_id = post.get("id") or post.get("url")
        context = {
            "post_index": index,
            "source_record_id": (
                str(source_id) if type(source_id) in (str, int) else None
            ),
        }
        outcome = post.get("geocode_status")
        label = outcome if isinstance(outcome, str) else "not_requested"
        outcome_counts[label] = outcome_counts.get(label, 0) + 1
        if isinstance(outcome, str) and outcome in GEO_FAILURES:
            enrichment_errors.append(
                {**context, "status": outcome, "error": post.get("geocode_error")}
            )
        try:
            records.append(
                normalize_court(
                    post,
                    run_id=selected,
                    collected_at=bronze["collected_at"],
                    geocoded_at=payload.get("geocoded_at"),
                )
            )
        except (ValueError, TypeError) as exc:
            errors.append(
                {**context, "error_type": type(exc).__name__, "error": str(exc)}
            )
    # PBPlus reports recoverable acquisition errors inside query_runs. Keep
    # those visible rather than reporting success when all fetches failed.
    collection_errors = [
        copy.deepcopy(query)
        for query in bronze["query_runs"]
        if query.get("status") in ("partial", "error")
    ]
    has_errors = bool(errors or collection_errors)
    status: Literal["success", "partial", "failed"] = (
        "partial" if has_errors and records else "failed" if has_errors else "success"
    )
    silver = {
        **native_batch_metadata(bronze, bronze_path),
        "schema_version": "1",
        "source": "pbplus",
        "run_id": selected,
        "collected_at": bronze["collected_at"],
        "normalized_at": _utc_now(),
        "status": status,
        "input_count": bronze["count"],
        "count": len(records),
        "rejected_count": len(errors),
        "normalization_errors": errors,
        "collection_errors": collection_errors,
        "enrichment_status_counts": outcome_counts,
        "enrichment_errors": enrichment_errors,
        "records": records,
    }
    _write_json_atomic(silver_path, silver)
    return CollectionResult(
        selected, status, bronze_path, silver_path, len(records), len(errors)
    )


def _crawl_pbplus(**kwargs: Any) -> dict[str, Any]:
    from ..sources.crawler.pbplus.crawler import crawl_pbplus_courts

    return crawl_pbplus_courts(**kwargs)


def _geocode_pbplus(payload: dict[str, Any], *, delay: float) -> dict[str, Any]:
    from ..sources.crawler.geo.geo import geocode_payload

    return geocode_payload(payload, delay=delay)


def _report_failure(
    tracker: CrawlerStatusTracker | None, error: Exception | str
) -> None:
    if tracker is not None:
        try:
            tracker.fail(error)
        except Exception:
            LOGGER.exception("Failed to persist PBPlus job failure status")


def collect_pbplus(
    *,
    limit: int = 1000,
    timeout: int = 20,
    delay: float = 0.5,
    retries: int = 2,
    geocode: bool = False,
    geo_delay: float = 1.0,
    artifact_root: str | Path | None = None,
    status_tracker: CrawlerStatusTracker | None = None,
    crawler: Callable[..., dict[str, Any]] | None = None,
    geocoder: Callable[..., dict[str, Any]] | None = None,
) -> CollectionResult:
    """Track PBPlus at the job boundary without changing crawler mechanics.

    Geography is opt-in. The existing helper reports individual provider errors
    in records; transport errors escaping the helper become nullable enrichment
    for the batch. Unexpected programming/configuration errors fail explicitly.
    Geo failures are reported separately and do not invalidate court entities.
    """
    if status_tracker is not None and status_tracker.status.source != "pbplus":
        raise ValueError("status_tracker source must be pbplus")
    previous_run_id = status_tracker.status.run_id if status_tracker else None
    try:
        if geo_delay < 0:
            raise ValueError("geo_delay must be >= 0")
        if status_tracker is None:
            selected = uuid4().hex
        else:
            status_tracker.start(
                seed_query="pickleball courts", sort="directory", target_count=limit
            )
            selected = status_tracker.status.run_id
            if (
                status_tracker.status.status != "running"
                or not selected
                or selected == previous_run_id
            ):
                raise RuntimeError("job did not establish a new tracked PBPlus run")
        envelope = (crawler if crawler is not None else _crawl_pbplus)(
            limit=limit, timeout=timeout, delay=delay, retries=retries
        )
        bronze, _, _ = _persist_bronze(envelope, selected, artifact_root)
        enriched = None
        if geocode:
            try:
                enriched = (geocoder if geocoder is not None else _geocode_pbplus)(
                    copy.deepcopy(bronze), delay=geo_delay
                )
                if not isinstance(enriched, Mapping):
                    raise TypeError("geocoder must return an enriched envelope")
            except OSError as exc:
                LOGGER.warning("PBPlus geography unavailable: %s", exc)
                enriched = copy.deepcopy(bronze)
                enriched["geocoded_at"] = _utc_now()
                for post in enriched["posts"]:
                    post.update(
                        latitude=None,
                        longitude=None,
                        geocode_status="error",
                        geocode_error=str(exc),
                    )
        result = process_pbplus_envelope(
            bronze, artifact_root=artifact_root, enriched_envelope=enriched
        )
        if status_tracker is not None:
            if result.status == "success":
                status_tracker.succeed(
                    collected_count=result.count, output_path=result.silver_path
                )
            else:
                _report_failure(
                    status_tracker,
                    f"{result.status}: {result.rejected_count} records rejected; inspect collection_errors; Silver: {result.silver_path}",
                )
        return result
    except Exception as exc:
        _report_failure(status_tracker, exc)
        raise


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Collect/replay PBPlus into local Bronze and Silver JSON."
    )
    parser.add_argument(
        "--replay", type=Path, help="Process saved crawler/Bronze JSON offline."
    )
    parser.add_argument(
        "--geography", type=Path, help="Already-enriched envelope for offline replay."
    )
    parser.add_argument("--run-id", help="Run identity for replay input without one.")
    parser.add_argument("--artifact-root", type=Path, default=default_artifact_root())
    parser.add_argument("--no-status", action="store_true")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--delay", type=float, default=0.5)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument(
        "--geocode", action="store_true", help="Enable live geographic enrichment."
    )
    parser.add_argument("--geo-delay", type=float, default=1.0)
    args = parser.parse_args(argv)
    if (args.run_id is not None or args.geography is not None) and args.replay is None:
        parser.error("--run-id and --geography are only supported with --replay")
    if args.replay is not None and args.geocode:
        parser.error("--replay is offline; supply --geography instead of --geocode")
    try:
        if args.replay is not None:
            envelope = json.loads(args.replay.read_text(encoding="utf-8"))
            enriched = (
                json.loads(args.geography.read_text(encoding="utf-8"))
                if args.geography is not None
                else None
            )
            result = process_pbplus_envelope(
                envelope,
                run_id=args.run_id,
                artifact_root=args.artifact_root,
                enriched_envelope=enriched,
            )
        else:
            tracker = (
                None
                if args.no_status
                else CrawlerStatusTracker(
                    "pbplus", args.artifact_root / "pbplus" / "status.json"
                )
            )
            result = collect_pbplus(
                limit=args.limit,
                timeout=args.timeout,
                delay=args.delay,
                retries=args.retries,
                geocode=args.geocode,
                geo_delay=args.geo_delay,
                artifact_root=args.artifact_root,
                status_tracker=tracker,
            )
    except Exception:
        LOGGER.exception("PBPlus collection/replay failed")
        return 1
    print(json.dumps(asdict(result), default=str, ensure_ascii=False))
    return {"success": 0, "partial": 2, "failed": 1}[result.status]


if __name__ == "__main__":
    raise SystemExit(main())
