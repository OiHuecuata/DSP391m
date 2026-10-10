"""Collect/replay Threads into local Bronze and Silver run artifacts.

Bronze is committed before normalization. Invalid records (ValueError or
TypeError) are reported and skipped; other exceptions fail the job explicitly.
Partial/all-rejected batches remain available in Silver with non-success status.
Artifact files are immutable once committed. A matching existing Bronze file
can be reused to recover a run whose Silver write never completed.
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from uuid import uuid4

from ..normalizers.social_post import normalize_social_post
from ..runtime.artifacts import write_json_atomic as _write_json_atomic
from ..runtime.config import (
    KafkaSettings,
    default_artifact_root,
    kafka_enabled_from_env,
)
from ..runtime.status import CrawlerStatusTracker

LOGGER = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..publishers.kafka import KafkaPublisher, PublicationResult


@dataclass(frozen=True, slots=True)
class CollectionResult:
    run_id: str
    status: Literal["success", "partial", "failed"]
    bronze_path: Path
    silver_path: Path
    count: int
    rejected_count: int
    publication: PublicationResult | None = None


def _run_id(value: Any) -> str:
    if (
        not isinstance(value, str)
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", value) is None
    ):
        raise ValueError(
            "run_id must be a safe filename identifier of 1..128 characters"
        )
    return value


def _validate_envelope(envelope: Mapping[str, Any]) -> None:
    """Validate stable batch metadata before any writes; leave posts raw."""
    if envelope.get("source") != "threads":
        raise ValueError("source must be threads")
    if (
        not isinstance(envelope.get("seed_query"), str)
        or not envelope["seed_query"].strip()
    ):
        raise ValueError("seed_query must be a nonempty string")
    if envelope.get("sort") not in ("recent", "top"):
        raise ValueError("sort must be recent or top")
    posts = envelope.get("posts")
    if not isinstance(posts, list) or any(not isinstance(post, dict) for post in posts):
        raise ValueError("posts must be a list of crawler record objects")
    count = envelope.get("count")
    if type(count) is not int or count != len(posts):
        raise ValueError("count must equal the number of crawler posts")
    query_runs = envelope.get("query_runs")
    if not isinstance(query_runs, list) or any(
        not isinstance(item, dict) for item in query_runs
    ):
        raise ValueError("query_runs must be a list of objects")
    timestamp = envelope.get("collected_at")
    if (
        not isinstance(timestamp, str)
        or datetime.fromisoformat(timestamp).utcoffset() is None
    ):
        raise ValueError("collected_at must be a timezone-aware datetime string")
    if envelope.get("schema_version", "1") != "1":
        raise ValueError("unsupported Bronze schema_version")


def _capture_time(post, fallback):
    from ..normalizers.social_post import _eligible_detail

    detail = _eligible_detail(post, str(post.get("id")))
    return detail.get("_observed_at") or post.get("_observed_at") or fallback


def process_threads_envelope(
    envelope: Mapping[str, Any],
    *,
    run_id: str | None = None,
    artifact_root: str | Path | None = None,
) -> CollectionResult:
    """Process a crawler result or a saved Bronze envelope, entirely offline.

    A saved run_id is reused and cannot be relabeled. Crawler metadata and all
    raw fields are preserved by deep copy. Collection timestamps are preserved
    verbatim in both envelopes and passed to the normalizer, which emits its
    own canonical per-record timestamps. No per-record timestamp is invented.
    """
    if not isinstance(envelope, Mapping):
        raise TypeError("envelope must be a mapping")
    _validate_envelope(envelope)
    existing_run_id = envelope.get("run_id")
    if existing_run_id is not None and run_id is not None and existing_run_id != run_id:
        raise ValueError("run_id conflicts with the saved Bronze run")
    selected_run_id = _run_id(
        run_id
        if run_id is not None
        else existing_run_id if existing_run_id is not None else uuid4().hex
    )
    root = (
        Path(artifact_root).expanduser()
        if artifact_root is not None
        else default_artifact_root()
    )
    bronze_path = root / "bronze" / "threads" / f"{selected_run_id}.json"
    silver_path = root / "silver" / "social_posts" / f"{selected_run_id}.json"
    if silver_path.exists():
        raise FileExistsError(f"Silver artifact already exists: {silver_path}")

    bronze = copy.deepcopy(dict(envelope))
    bronze.update(schema_version="1", run_id=selected_run_id)
    if bronze_path.exists():
        existing = json.loads(bronze_path.read_text(encoding="utf-8"))
        if existing != bronze:
            raise FileExistsError(
                f"different Bronze artifact already exists: {bronze_path}"
            )
    else:
        _write_json_atomic(bronze_path, bronze)

    records = []
    errors = []
    for index, post in enumerate(bronze["posts"]):
        try:
            records.append(
                normalize_social_post(
                    post,
                    run_id=selected_run_id,
                    collected_at=_capture_time(post, bronze["collected_at"]),
                )
            )
        except (ValueError, TypeError) as exc:
            source_id = post.get("id")
            errors.append(
                {
                    "post_index": index,
                    "source_record_id": (
                        str(source_id) if type(source_id) in (str, int) else None
                    ),
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )

        # Accessible, identity-checked replies use the existing SocialPost
        # transport and its comment routing flag. They never inherit the
        # parent's inferred geography or analytical labels.
        from ..normalizers.social_post import _eligible_detail
        from ..sources.crawler.threads.vietnam_terms import (
            _detect_locations,
            _detect_topics,
        )

        source_id = str(post.get("id"))
        detail = _eligible_detail(post, source_id)
        for reply in detail.get("visible_replies", []):
            try:
                native = reply.get("raw_payload") or {}
                text = reply.get("text")
                projection = {
                    **reply,
                    **reply.get("engagement", {}),
                    "user": native.get("user"),
                    "is_reply": True,
                    "matched_queries": post.get("matched_queries"),
                    "query_contexts": post.get("query_contexts"),
                    "detected_locations": _detect_locations(text or ""),
                    "categories": _detect_topics(text or "")[0],
                    "detail": None,
                    "detail_status": None,
                }
                normalized = normalize_social_post(
                    projection,
                    run_id=selected_run_id,
                    collected_at=reply.get("_observed_at")
                    or detail.get("_observed_at")
                    or post.get("_observed_at", bronze["collected_at"]),
                )
                normalized["parent_record_id"] = "threads:" + source_id
                if not any(r["record_id"] == normalized["record_id"] for r in records):
                    records.append(normalized)
            except (ValueError, TypeError) as exc:
                errors.append(
                    {
                        "parent_source_id": source_id,
                        "source_record_id": reply.get("id"),
                        "error": str(exc),
                    }
                )

    collection_errors = [
        copy.deepcopy(q)
        for q in bronze["query_runs"]
        if q.get("status") == "error" or q.get("detail_failed_count", 0)
    ]
    collection_errors.extend(
        {"source_record_id": p.get("id"), "error": p.get("detail_error")}
        for p in bronze["posts"]
        if p.get("detail_status") == "error"
    )
    from ..runtime.acquisition import threads_outcome

    acquisition = threads_outcome(bronze)
    has_errors = bool(errors)
    status: Literal["success", "partial", "failed"] = (
        "partial" if has_errors and records else "failed" if has_errors else "success"
    )
    if not has_errors:
        status = acquisition["status"]
    silver = {
        "schema_version": "1",
        "source": "threads",
        "run_id": selected_run_id,
        "collected_at": bronze["collected_at"],
        # Batch completion time; each record's normalized_at belongs to the normalizer.
        "normalized_at": datetime.now(UTC)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z"),
        "status": status,
        "input_count": bronze["count"],
        "count": len(records),
        "rejected_count": len(errors),
        "normalization_errors": errors,
        "collection_errors": collection_errors,
        "acquisition": acquisition,
        "processing_status": (
            "partial" if errors and records else "failed" if errors else "success"
        ),
        "records": records,
    }
    _write_json_atomic(silver_path, silver)
    return CollectionResult(
        selected_run_id, status, bronze_path, silver_path, len(records), len(errors)
    )


def _crawl_threads(**kwargs: Any) -> dict[str, Any]:
    # Replay and injected-crawler tests never import Playwright/source mechanics.
    from ..sources.crawler.threads.crawler import crawl_threads_posts

    return crawl_threads_posts(**kwargs)


def _report_failure(
    tracker: CrawlerStatusTracker | None, error: Exception | str
) -> None:
    if tracker is not None:
        try:
            tracker.fail(error)
        except Exception:
            # Report persistence failure without masking the original job failure.
            LOGGER.exception("Failed to persist Threads job failure status")


def _publish_silver(
    result: CollectionResult,
    settings: KafkaSettings | None,
    publisher_factory: Callable[[KafkaSettings], KafkaPublisher] | None,
) -> CollectionResult:
    # Called only after Silver is committed; disabled collection never imports Kafka.
    from ..publishers.kafka import publish_social_batch

    publication = publish_social_batch(
        result.silver_path, settings=settings, publisher_factory=publisher_factory
    )
    statuses = (result.status, publication.status)
    status = (
        "failed"
        if "failed" in statuses
        else "partial" if "partial" in statuses else "success"
    )
    return replace(result, status=status, publication=publication)


def collect_threads(
    *,
    query: str = "pickleball",
    limit: int = 10,
    sort: str = "recent",
    headless: bool = True,
    timeout_ms: int = 60_000,
    per_query_cap: int = 3,
    artifact_root: str | Path | None = None,
    status_tracker: CrawlerStatusTracker | None = None,
    crawler: Callable[..., dict[str, Any]] | None = None,
    publish_kafka: bool = False,
    kafka_settings: KafkaSettings | None = None,
    publisher_factory: Callable[[KafkaSettings], KafkaPublisher] | None = None,
    crawl_options: dict[str, Any] | None = None,
) -> CollectionResult:
    """Invoke the existing crawler, preserve Bronze, then publish local Silver.

    The crawler starts the supplied tracker. Its new run ID is reused exactly;
    absent/failed tracker startup is an explicit job failure, not a second ID.
    Tracker states have no partial value, so partial batches are marked failed
    there and carry their more precise partial status in the result/Silver.
    """
    if status_tracker is not None and status_tracker.status.source != "threads":
        raise ValueError("status_tracker source must be threads")
    previous_run_id = status_tracker.status.run_id if status_tracker else None
    try:
        selected_run_id = uuid4().hex if status_tracker is None else None
        options = dict(crawl_options or {})
        if crawler is None and selected_run_id:
            checkpoint = options.get("checkpoint_path")
            if checkpoint and Path(checkpoint).exists() and options.get("resume", True):
                saved = json.loads(Path(checkpoint).read_text())
                if not saved.get("artifact_committed"):
                    selected_run_id = saved["run_id"]
            options.setdefault("run_id", selected_run_id)
        envelope = (crawler if crawler is not None else _crawl_threads)(
            query=query,
            limit=limit,
            sort=sort,
            headless=headless,
            timeout_ms=timeout_ms,
            per_query_cap=per_query_cap,
            status_tracker=status_tracker,
            **options,
        )
        selected_run_id = envelope.get("run_id") or selected_run_id
        if status_tracker is not None:
            selected_run_id = status_tracker.status.run_id
            if (
                status_tracker.status.status != "running"
                or not selected_run_id
                or selected_run_id == previous_run_id
            ):
                raise RuntimeError("crawler did not establish a new tracked run")
        result = process_threads_envelope(
            envelope, run_id=selected_run_id, artifact_root=artifact_root
        )
        if options.get("checkpoint_path"):
            from ..sources.crawler.threads.discovery import acknowledge_checkpoint

            acknowledge_checkpoint(options["checkpoint_path"], result.run_id)
        if publish_kafka or (kafka_settings is not None and kafka_settings.enabled):
            result = _publish_silver(result, kafka_settings, publisher_factory)
        if status_tracker is not None:
            if result.status == "success":
                status_tracker.succeed(
                    collected_count=result.count, output_path=result.silver_path
                )
            elif result.status == "partial":
                status_tracker.partial(
                    collected_count=result.count,
                    output_path=result.silver_path,
                    error=(
                        f"{result.rejected_count} records rejected"
                        if result.rejected_count
                        else None
                    ),
                )
            else:
                _report_failure(
                    status_tracker,
                    f"{result.status}: {result.rejected_count} records rejected; Kafka: {result.publication}; Silver: {result.silver_path}",
                )
        return result
    except Exception as exc:
        _report_failure(status_tracker, exc)
        raise


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Collect/replay Threads into local Bronze and Silver JSON."
    )
    replay = parser.add_mutually_exclusive_group()
    replay.add_argument(
        "--replay", type=Path, help="Process saved crawler/Bronze JSON offline."
    )
    replay.add_argument(
        "--replay-silver",
        type=Path,
        help="Publish existing Threads Silver to Kafka without crawling or writing artifacts.",
    )
    parser.add_argument(
        "--publish-kafka",
        action="store_true",
        help="Publish after Silver is saved; Kafka is disabled by default.",
    )
    parser.add_argument("--run-id", help="Run identity for a replay input without one.")
    parser.add_argument("--artifact-root", type=Path, default=default_artifact_root())
    parser.add_argument(
        "--no-status",
        action="store_true",
        help="Disable status tracking for live collection.",
    )
    parser.add_argument("--query", default="pickleball")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--sort", choices=("recent", "top"), default="recent")
    parser.add_argument("--per-query-cap", type=int, default=3)
    parser.add_argument("--timeout-ms", type=int, default=60_000)
    parser.add_argument("--discovery-config", type=Path)
    parser.add_argument("--max-queries", type=int)
    parser.add_argument("--max-requests", type=int)
    parser.add_argument("--max-pages-per-query", type=int)
    parser.add_argument("--published-after")
    parser.add_argument("--published-before")
    parser.add_argument("--lookback-days", type=int)
    parser.add_argument("--checkpoint", type=Path)
    parser.add_argument("--fresh", action="store_true")
    parser.add_argument("--headed", action="store_true")
    args = parser.parse_args(argv)
    if args.run_id is not None and args.replay is None:
        parser.error(
            "--run-id is only supported with --replay; live runs reuse tracker identity"
        )
    try:
        enabled = args.publish_kafka or kafka_enabled_from_env()
        if args.replay_silver is not None:
            from ..publishers.kafka import publish_social_batch

            result = publish_social_batch(args.replay_silver)
        elif args.replay is not None:
            envelope = json.loads(args.replay.read_text(encoding="utf-8"))
            result = process_threads_envelope(
                envelope, run_id=args.run_id, artifact_root=args.artifact_root
            )
            if enabled:
                result = _publish_silver(result, None, None)
        else:
            tracker = (
                None
                if args.no_status
                else CrawlerStatusTracker(
                    "threads", args.artifact_root / "threads" / "status.json"
                )
            )
            from ..sources.crawler.threads.crawler import discovery_options

            result = collect_threads(
                crawl_options=discovery_options(args),
                query=args.query,
                limit=args.limit,
                sort=args.sort,
                headless=not args.headed,
                per_query_cap=args.per_query_cap,
                timeout_ms=args.timeout_ms,
                artifact_root=args.artifact_root,
                status_tracker=tracker,
                publish_kafka=enabled,
            )
    except Exception:
        LOGGER.exception("Threads collection/replay failed")
        return 1
    print(json.dumps(asdict(result), default=str, ensure_ascii=False))
    return {"success": 0, "partial": 2, "failed": 1}[result.status]


if __name__ == "__main__":
    raise SystemExit(main())
