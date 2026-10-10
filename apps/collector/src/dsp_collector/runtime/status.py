"""Runtime status tracking for collector crawlers.

This module owns crawler run-state only:
- track one crawler run lifecycle
- expose lightweight progress/health metrics for the backend/frontend
- persist the latest status as JSON
- write status atomically so readers never see partial JSON

It intentionally does not contain:
- source-specific crawling logic
- Playwright/HTTP logic
- Vietnam/domain vocabulary or relevance scoring
- Kafka, Data Lake, or Data Warehouse logic
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

CrawlerRunState = Literal["idle", "running", "success", "partial", "failed"]
QueryRunState = Literal["running", "ok", "error"]


def _utc_now_iso() -> str:
    """Return a UTC timestamp in the same ISO-8601 style used by the crawler."""
    return datetime.now(UTC).isoformat()


def _collector_root() -> Path:
    """Return apps/collector for the current monorepo layout."""
    return Path(__file__).resolve().parents[3]


def default_status_path(source: str) -> Path:
    """Return the default status file for one collector source."""
    source = source.strip().lower()
    if not source:
        raise ValueError("source must not be empty")

    return _collector_root() / ".local" / source / "status.json"


@dataclass(slots=True)
class QueryStatus:
    """Small status view of the current/last query.

    Detailed query history already belongs to the crawler payload's
    ``query_runs`` field, so the runtime status file keeps only the query
    needed for live monitoring.
    """

    query: str
    kind: str | None = None
    geo_name: str | None = None
    status: QueryRunState = "running"
    raw_count: int | None = None
    accepted_count: int | None = None
    total_collected: int | None = None
    landed_url: str | None = None
    error: str | None = None


@dataclass(slots=True)
class CrawlerStatus:
    """Serializable latest-state snapshot for one crawler source."""

    schema_version: str = "1"

    source: str = ""
    run_id: str | None = None
    status: CrawlerRunState = "idle"
    pid: int | None = None

    seed_query: str | None = None
    sort: str | None = None

    target_count: int = 0
    collected_count: int = 0

    query_plan_count: int | None = None
    queries_processed: int = 0
    queries_succeeded: int = 0
    queries_failed: int = 0

    raw_count: int = 0

    current_query: QueryStatus | None = None
    last_query: QueryStatus | None = None

    started_at: str | None = None
    updated_at: str | None = None
    finished_at: str | None = None

    output_path: str | None = None
    error: str | None = None

    metadata: dict[str, Any] = field(default_factory=dict)


class CrawlerStatusTracker:
    """Persist the latest runtime status of one crawler.

    The tracker is source-agnostic. Threads can use it now, while Facebook,
    TikTok, PBPlus, etc. can reuse the same contract later.

    One status file represents the latest run for one source. Concurrent runs
    of the same source are intentionally out of scope for the current PoC.
    """

    def __init__(
        self,
        source: str,
        status_path: str | Path | None = None,
    ) -> None:
        source = source.strip().lower()
        if not source:
            raise ValueError("source must not be empty")

        self.status_path = (
            Path(status_path).expanduser().resolve()
            if status_path is not None
            else default_status_path(source)
        )

        self.status_path.parent.mkdir(parents=True, exist_ok=True)
        self._status = CrawlerStatus(source=source)

    @property
    def status(self) -> CrawlerStatus:
        """Return the in-memory status snapshot."""
        return self._status

    def snapshot(self) -> dict[str, Any]:
        """Return a JSON-serializable status snapshot."""
        return asdict(self._status)

    def start(
        self,
        *,
        seed_query: str,
        sort: str,
        target_count: int,
        query_plan_count: int | None = None,
        metadata: dict[str, Any] | None = None,
        run_id: str | None = None,
    ) -> None:
        """Start a new crawler run and replace status from any previous run."""
        seed_query = seed_query.strip()
        sort = sort.strip()

        if not seed_query:
            raise ValueError("seed_query must not be empty")
        if not sort:
            raise ValueError("sort must not be empty")
        if target_count < 1:
            raise ValueError("target_count must be >= 1")
        if query_plan_count is not None and query_plan_count < 0:
            raise ValueError("query_plan_count must be >= 0")

        if run_id is not None and (
            not isinstance(run_id, str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", run_id)
        ):
            raise ValueError("run_id must be a safe collection identifier")
        now = _utc_now_iso()

        self._status = CrawlerStatus(
            source=self._status.source,
            run_id=run_id or uuid4().hex,
            status="running",
            pid=os.getpid(),
            seed_query=seed_query,
            sort=sort,
            target_count=target_count,
            query_plan_count=query_plan_count,
            started_at=now,
            updated_at=now,
            metadata=dict(metadata or {}),
        )
        self._persist()

    def query_started(
        self,
        *,
        query: str,
        kind: str | None = None,
        geo_name: str | None = None,
    ) -> None:
        """Mark one query from the crawler's query plan as currently running."""
        self._require_running()

        query = query.strip()
        if not query:
            raise ValueError("query must not be empty")

        self._status.current_query = QueryStatus(
            query=query,
            kind=kind,
            geo_name=geo_name,
            status="running",
        )
        self._touch()

    def query_succeeded(
        self,
        *,
        query: str,
        raw_count: int,
        accepted_count: int,
        total_collected: int,
        kind: str | None = None,
        geo_name: str | None = None,
        landed_url: str | None = None,
    ) -> None:
        """Record one successfully processed query.

        ``accepted_count`` should match the crawler's accepted_this_query:
        newly accepted unique posts from this query.

        ``total_collected`` should match len(collected) after this query.
        """
        self._require_running()

        if raw_count < 0:
            raise ValueError("raw_count must be >= 0")
        if accepted_count < 0:
            raise ValueError("accepted_count must be >= 0")
        if total_collected < 0:
            raise ValueError("total_collected must be >= 0")

        last_query = QueryStatus(
            query=query,
            kind=kind,
            geo_name=geo_name,
            status="ok",
            raw_count=raw_count,
            accepted_count=accepted_count,
            total_collected=total_collected,
            landed_url=landed_url,
        )

        self._status.queries_processed += 1
        self._status.queries_succeeded += 1
        self._status.raw_count += raw_count
        self._status.collected_count = total_collected
        self._status.last_query = last_query
        self._status.current_query = None
        self._touch()

    def query_failed(
        self,
        *,
        query: str,
        error: Exception | str,
        kind: str | None = None,
        geo_name: str | None = None,
    ) -> None:
        """Record a recoverable query-level failure.

        This mirrors the current Threads crawler behavior: a failed query is
        logged and collection continues with the next QuerySpec.
        """
        self._require_running()

        last_query = QueryStatus(
            query=query,
            kind=kind,
            geo_name=geo_name,
            status="error",
            error=str(error),
        )

        self._status.queries_processed += 1
        self._status.queries_failed += 1
        self._status.last_query = last_query
        self._status.current_query = None
        self._touch()

    def heartbeat(self) -> None:
        """Refresh updated_at without changing progress."""
        self._require_running()
        self._touch()

    def succeed(
        self,
        *,
        collected_count: int,
        output_path: str | Path,
    ) -> None:
        """Mark the whole crawler run as successfully completed."""
        self._require_running()

        if collected_count < 0:
            raise ValueError("collected_count must be >= 0")

        now = _utc_now_iso()

        self._status.status = "success"
        self._status.collected_count = collected_count
        self._status.current_query = None
        self._status.output_path = str(Path(output_path).expanduser().resolve())
        self._status.error = None
        self._status.updated_at = now
        self._status.finished_at = now
        self._persist()

    def fail(self, error: Exception | str) -> None:
        """Mark the whole crawler run as failed."""
        now = _utc_now_iso()

        self._status.status = "failed"
        self._status.current_query = None
        self._status.error = str(error)
        self._status.updated_at = now
        self._status.finished_at = now
        self._persist()

    def partial(
        self, *, collected_count: int, output_path: str | Path, error: str | None = None
    ) -> None:
        self.succeed(collected_count=collected_count, output_path=output_path)
        self._status.status = "partial"
        self._status.metadata["resumable"] = True
        self._status.error = error
        self._persist()

    def reset(self) -> None:
        """Reset the source to an explicit idle state."""
        now = _utc_now_iso()

        self._status = CrawlerStatus(
            source=self._status.source,
            status="idle",
            updated_at=now,
        )
        self._persist()

    def _require_running(self) -> None:
        if self._status.status != "running":
            raise RuntimeError(
                "crawler status is not running; call start() before updating progress"
            )

    def _touch(self) -> None:
        self._status.updated_at = _utc_now_iso()
        self._persist()

    def _persist(self) -> None:
        """Atomically persist status JSON.

        The Go backend may read this file while the Python collector updates it.
        Writing to a temporary file in the same directory and then replacing
        the target prevents readers from observing partially written JSON.
        """
        payload = json.dumps(
            self.snapshot(),
            ensure_ascii=False,
            indent=2,
        )

        fd, temp_name = tempfile.mkstemp(
            prefix=f".{self.status_path.name}.",
            suffix=".tmp",
            dir=self.status_path.parent,
            text=True,
        )

        temp_path = Path(temp_name)

        try:
            with os.fdopen(fd, "w", encoding="utf-8") as file:
                file.write(payload)
                file.write("\n")
                file.flush()
                os.fsync(file.fileno())

            os.replace(temp_path, self.status_path)
        except Exception:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass
            raise
