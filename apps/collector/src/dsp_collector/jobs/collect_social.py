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
import os
import re
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from ..normalizers.social_post import normalize_social_post
from ..runtime.config import default_artifact_root
from ..runtime.status import CrawlerStatusTracker

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CollectionResult:
    run_id: str
    status: Literal["success", "partial", "failed"]
    bronze_path: Path
    silver_path: Path
    count: int
    rejected_count: int


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


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    """Commit a UTF-8 JSON file with no partially written destination.

    Existing artifacts are rejected. Run IDs must be unique between concurrent
    processes; this local single-run boundary does not implement writer locking.
    """
    serialized = (
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(f"artifact already exists: {path}")
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            file.write(serialized)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


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
                    post, run_id=selected_run_id, collected_at=bronze["collected_at"]
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

    status: Literal["success", "partial", "failed"] = (
        "partial" if errors and records else "failed" if errors else "success"
    )
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
        envelope = (crawler if crawler is not None else _crawl_threads)(
            query=query,
            limit=limit,
            sort=sort,
            headless=headless,
            timeout_ms=timeout_ms,
            per_query_cap=per_query_cap,
            status_tracker=status_tracker,
        )
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
        if status_tracker is not None:
            if result.status == "success":
                status_tracker.succeed(
                    collected_count=result.count, output_path=result.silver_path
                )
            else:
                _report_failure(
                    status_tracker,
                    f"{result.status}: {result.rejected_count} records rejected; Silver: {result.silver_path}",
                )
        return result
    except Exception as exc:
        _report_failure(status_tracker, exc)
        raise


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Collect/replay Threads into local Bronze and Silver JSON."
    )
    parser.add_argument(
        "--replay", type=Path, help="Process saved crawler/Bronze JSON offline."
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
    parser.add_argument("--headed", action="store_true")
    args = parser.parse_args(argv)
    if args.run_id is not None and args.replay is None:
        parser.error(
            "--run-id is only supported with --replay; live runs reuse tracker identity"
        )
    try:
        if args.replay is not None:
            envelope = json.loads(args.replay.read_text(encoding="utf-8"))
            result = process_threads_envelope(
                envelope, run_id=args.run_id, artifact_root=args.artifact_root
            )
        else:
            tracker = (
                None
                if args.no_status
                else CrawlerStatusTracker(
                    "threads", args.artifact_root / "threads" / "status.json"
                )
            )
            result = collect_threads(
                query=args.query,
                limit=args.limit,
                sort=args.sort,
                headless=not args.headed,
                per_query_cap=args.per_query_cap,
                timeout_ms=args.timeout_ms,
                artifact_root=args.artifact_root,
                status_tracker=tracker,
            )
    except Exception:
        LOGGER.exception("Threads collection/replay failed")
        return 1
    print(json.dumps(asdict(result), default=str, ensure_ascii=False))
    return {"success": 0, "partial": 2, "failed": 1}[result.status]


if __name__ == "__main__":
    raise SystemExit(main())
