"""Threads smoke-test crawler.

This file owns source mechanics only:
- build/open Threads search URLs
- parse SSR search results
- iterate a domain-provided query plan
- let ``vietnam_terms`` adapt/filter raw posts
- deduplicate and write JSON

Vietnam/domain vocabulary and scoring do not live here.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit

from playwright.sync_api import Page, sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

try:
    from .vietnam_terms import (
        QuerySpec,
        adapt_post,
        merge_adapted_post,
    )
except ImportError:
    # Direct execution support:
    # python .../threads/crawler.py
    from vietnam_terms import (
        QuerySpec,
        adapt_post,
        merge_adapted_post,
    )

try:
    from .post_parser import ThreadsPostParserError, parse_post_detail
except ImportError:
    # Direct execution support:
    # python .../threads/crawler.py
    from post_parser import ThreadsPostParserError, parse_post_detail

try:
    from ....runtime.status import CrawlerStatusTracker
except ImportError:
    # Direct execution support:
    # python .../threads/crawler.py
    _SRC_ROOT = Path(__file__).resolve().parents[4]
    if str(_SRC_ROOT) not in sys.path:
        sys.path.insert(0, str(_SRC_ROOT))
    from dsp_collector.runtime.status import CrawlerStatusTracker


THREADS_SEARCH_URL = "https://www.threads.com/search/"
DEFAULT_QUERY = "pickleball"
DEFAULT_LIMIT = 10
DEFAULT_SORT = "recent"
DEFAULT_TIMEOUT_MS = 60_000
DEFAULT_PER_QUERY_CAP = 3
VALID_SORTS = {"top", "recent"}
SEARCH_RESULTS_MARKER = '"searchResults":{"inform_module"'


class ThreadsCrawlerError(RuntimeError):
    """Raised when Threads search data cannot be fetched or parsed."""


class ThreadsAccessBlocked(ThreadsCrawlerError):
    """Public access requires authentication or has been rate limited."""


def ensure_public_access(page, response):
    if response is not None and response.status in (401, 403, 429):
        raise ThreadsAccessBlocked(f"Public access blocked: HTTP {response.status}")
    if any(
        part in urlsplit(page.url).path
        for part in ("/login", "/checkpoint", "/challenge")
    ):
        raise ThreadsAccessBlocked(
            "Public request redirected to an access-control page"
        )


def expanded_query_plan(seeds, **options):
    from dsp_collector.sources.crawler.threads.discovery import build_discovery_plan

    return build_discovery_plan(seeds, **options)


def publication_range(posts):
    dates = []
    for post in posts:
        value = post.get("taken_at")
        if type(value) in (int, float):
            try:
                dates.append(datetime.fromtimestamp(value, UTC).isoformat())
            except (ValueError, OverflowError, OSError):
                pass
    return {
        "publication_min": min(dates) if dates else None,
        "publication_max": max(dates) if dates else None,
    }


def _collector_root() -> Path:
    return Path(__file__).resolve().parents[5]


def default_output_path() -> Path:
    return _collector_root() / ".local" / "threads" / "pickleball_sample.json"


def _status_update(
    tracker: CrawlerStatusTracker | None,
    method: str,
    **kwargs: Any,
) -> None:
    """Best-effort status reporting; never change crawler behavior."""
    if tracker is None:
        return

    try:
        getattr(tracker, method)(**kwargs)
    except Exception as exc:  # noqa: BLE001
        print(
            f"[threads-crawler] status update failed ({method}): {exc}",
            flush=True,
        )


def build_search_url(query: str, sort: str = DEFAULT_SORT) -> str:
    query = query.strip()

    if not query:
        raise ValueError("query must not be empty")
    if sort not in VALID_SORTS:
        raise ValueError(f"sort must be one of {sorted(VALID_SORTS)}")

    params: dict[str, str] = {
        "q": query,
        "serp_type": "default",
    }

    if sort == "recent":
        params["filter"] = "recent"

    return f"{THREADS_SEARCH_URL}?{urlencode(params)}"


def _extract_search_results_candidates(html: str) -> list[dict[str, Any]]:
    key = '"searchResults":'
    decoder = json.JSONDecoder()
    results: list[dict[str, Any]] = []
    cursor = 0

    while True:
        marker_index = html.find(SEARCH_RESULTS_MARKER, cursor)
        if marker_index < 0:
            break

        object_start = html.find("{", marker_index + len(key))
        if object_start < 0:
            break

        try:
            result, consumed = decoder.raw_decode(html[object_start:])
        except json.JSONDecodeError:
            cursor = marker_index + len(SEARCH_RESULTS_MARKER)
            continue

        if isinstance(result, dict):
            results.append(result)

        cursor = object_start + consumed

    if not results:
        raise ThreadsCrawlerError("No searchResults objects found in Threads page")

    return results


def _post_from_edge(edge: dict[str, Any]) -> dict[str, Any] | None:
    try:
        post = edge["node"]["thread"]["thread_items"][0]["post"]
    except (KeyError, IndexError, TypeError):
        return None

    if not isinstance(post, dict):
        return None

    user = post.get("user") or {}
    info = post.get("text_post_app_info") or {}
    caption = post.get("caption") or {}

    username = user.get("username")
    code = post.get("code")

    url = (
        f"https://www.threads.com/@{username}/post/{code}"
        if username and code
        else None
    )

    has_media = bool(
        ((post.get("image_versions2") or {}).get("candidates") or [])
        or (post.get("video_versions") or [])
        or (post.get("carousel_media") or [])
    )

    return {
        "id": post.get("pk"),
        "code": code,
        "url": url,
        "text": caption.get("text") if isinstance(caption, dict) else None,
        "taken_at": post.get("taken_at"),
        "like_count": post.get("like_count"),
        "reply_count": info.get("direct_reply_count"),
        "repost_count": info.get("repost_count"),
        "quote_count": info.get("quote_count"),
        "is_reply": bool(info.get("is_reply")),
        "media_type": post.get("media_type"),
        "has_media": has_media,
        "user": {
            "pk": user.get("pk"),
            "username": username,
            "full_name": user.get("full_name"),
            "is_verified": bool(user.get("is_verified")),
        },
        "raw_payload": post,
    }


def _candidate_post_count(search_results: dict[str, Any]) -> int:
    return sum(
        1
        for edge in search_results.get("edges") or []
        if isinstance(edge, dict) and _post_from_edge(edge) is not None
    )


def parse_posts(
    html: str,
    limit: int,
    *,
    evidence: dict | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    if limit < 1:
        raise ValueError("limit must be >= 1")

    candidates = _extract_search_results_candidates(html)
    search_results = max(candidates, key=_candidate_post_count)

    print(
        f"[threads-crawler] SSR candidates={len(candidates)}, "
        f"selected_posts={_candidate_post_count(search_results)}",
        flush=True,
    )

    posts: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    source_posts = []
    for edge in search_results.get("edges") or []:
        if not isinstance(edge, dict):
            continue

        post = _post_from_edge(edge)
        if post is None:
            continue

        source_posts.append(post)
        post_id = post.get("id")
        if post_id is not None:
            post_id = str(post_id)
            if post_id in seen_ids:
                continue
            seen_ids.add(post_id)

        posts.append(post)

    if evidence is not None:
        evidence.update(
            search_results=search_results,
            source_exposure_count=len(source_posts),
            within_page_duplicates_count=len(source_posts) - len(posts),
            parser_deferred_count=max(0, len(posts) - limit),
            source_record_ids=[
                str(p["id"]) for p in source_posts if p.get("id") is not None
            ],
        )
    return posts[:limit], search_results.get("page_info")


def _fetch_search_html(
    page: Page,
    *,
    query: str,
    sort: str,
    timeout_ms: int,
) -> tuple[str, str]:
    search_url = build_search_url(query, sort)
    print(f"[threads-crawler] opening: {search_url}", flush=True)

    try:
        response = page.goto(
            search_url,
            wait_until="domcontentloaded",
            timeout=timeout_ms,
        )
        ensure_public_access(page, response)
        page.wait_for_timeout(2_000)
        html = page.evaluate("document.documentElement.innerHTML")

        if SEARCH_RESULTS_MARKER not in html:
            print(
                "[threads-crawler] searchResults not visible yet; retrying once...",
                flush=True,
            )
            response = page.reload(
                wait_until="domcontentloaded",
                timeout=timeout_ms,
            )
            ensure_public_access(page, response)
            page.wait_for_timeout(3_000)
            html = page.evaluate("document.documentElement.innerHTML")
    except PlaywrightTimeoutError as exc:
        raise ThreadsCrawlerError(
            f"Threads page timed out after {timeout_ms} ms for query {query!r}"
        ) from exc

    return html, page.url


def _collect_for_query(
    page: Page,
    *,
    query_spec: QuerySpec,
    sort: str,
    timeout_ms: int,
    raw_limit: int,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None, str]:
    """Fetch/parse one QuerySpec without applying domain rules."""
    html, landed_url = _fetch_search_html(
        page,
        query=query_spec.query,
        sort=sort,
        timeout_ms=timeout_ms,
    )
    evidence = {}
    posts, page_info = parse_posts(html=html, limit=raw_limit, evidence=evidence)
    page.last_search_evidence = evidence
    return posts, page_info, landed_url


def _collect_public_page(page, *, url, query_spec, sort, timeout_ms, raw_limit):
    response = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    ensure_public_access(page, response)
    page.wait_for_timeout(2000)
    html = page.evaluate("document.documentElement.innerHTML")
    evidence = {}
    posts, page_info = parse_posts(html, raw_limit, evidence=evidence)
    page.last_search_evidence = evidence
    return posts, page_info, page.url


def _fetch_post_detail(
    page: Page,
    *,
    post: dict[str, Any],
    timeout_ms: int,
) -> dict[str, Any]:
    """Fetch one accepted Threads post and parse its full page detail."""
    url = post.get("url")
    if not url:
        raise ThreadsCrawlerError(f"Post {post.get('id')!r} has no canonical URL")

    try:
        response = page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=timeout_ms,
        )
        ensure_public_access(page, response)
        page.wait_for_timeout(1_500)
        html = page.evaluate("document.documentElement.innerHTML")
    except PlaywrightTimeoutError as exc:
        raise ThreadsCrawlerError(
            f"Post detail timed out after {timeout_ms} ms: {url}"
        ) from exc

    try:
        return parse_post_detail(
            html,
            expected_post_id=(str(post["id"]) if post.get("id") is not None else None),
            expected_code=post.get("code"),
            source_url=url,
        )
    except ThreadsPostParserError:
        try:
            response = page.reload(
                wait_until="domcontentloaded",
                timeout=timeout_ms,
            )
            ensure_public_access(page, response)
            page.wait_for_timeout(2_500)
            html = page.evaluate("document.documentElement.innerHTML")

            return parse_post_detail(
                html,
                expected_post_id=(
                    str(post["id"]) if post.get("id") is not None else None
                ),
                expected_code=post.get("code"),
                source_url=url,
            )
        except PlaywrightTimeoutError as exc:
            raise ThreadsCrawlerError(f"Post detail retry timed out: {url}") from exc
        except ThreadsPostParserError as exc:
            raise ThreadsCrawlerError(
                f"Failed to parse post detail: {url}: {exc}"
            ) from exc


def _assert_post_detail_identity(
    post: dict[str, Any],
    detail: dict[str, Any],
) -> None:
    """Reject enriched detail that does not belong to the search candidate."""
    expected_id = str(post.get("id")) if post.get("id") is not None else None
    actual_id = str(detail.get("id")) if detail.get("id") is not None else None
    expected_code = post.get("code")
    actual_code = detail.get("code")

    expected_username = (post.get("user") or {}).get("username")
    actual_username = (detail.get("author") or {}).get("username")

    mismatches: list[str] = []

    if expected_id is not None and actual_id != expected_id:
        mismatches.append(f"id expected={expected_id!r} actual={actual_id!r}")

    if expected_code is not None and actual_code != expected_code:
        mismatches.append(f"code expected={expected_code!r} actual={actual_code!r}")

    if expected_username and actual_username and actual_username != expected_username:
        mismatches.append(
            "username " f"expected={expected_username!r} actual={actual_username!r}"
        )

    if mismatches:
        raise ThreadsCrawlerError(
            "Post detail identity mismatch: " + "; ".join(mismatches)
        )


def crawl_threads_posts(
    query: str = DEFAULT_QUERY,
    limit: int = DEFAULT_LIMIT,
    sort: str = DEFAULT_SORT,
    *,
    headless: bool = True,
    timeout_ms: int = DEFAULT_TIMEOUT_MS,
    per_query_cap: int = DEFAULT_PER_QUERY_CAP,
    status_tracker: CrawlerStatusTracker | None = None,
    query_seeds: list[str] | None = None,
    max_results_per_query: int = 30,
    detail_limit: int | None = 2,
    query_delay: float = 0.75,
    retries: int = 1,
    max_queries: int | None = None,
    max_requests: int = 32,
    max_pages_per_query: int = 1,
    max_duration_seconds: float | None = 300,
    query_categories: list[str] | None = None,
    geo_keys: list[str] | None = None,
    include_local: bool = False,
    max_geo_aliases: int | None = 2,
    expand_subject_variants: bool = True,
    published_after: str | None = None,
    published_before: str | None = None,
    lookback_days: int | None = None,
    checkpoint_path: str | Path | None = None,
    resume: bool = True,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Discover through public pages with bounded requests and page checkpoints.

    A checkpoint continues a finite survey across collection runs. Already
    exported records are not re-labelled as fresh observations. An interrupted
    unexported run resumes with its original identity and captured timestamps.
    Complete surveys reset on the next run, allowing legitimate later snapshots.
    """
    import fcntl
    from contextlib import nullcontext
    from dataclasses import asdict
    from uuid import uuid4

    from dsp_collector.sources.crawler.threads.discovery import (
        PLANNER_VERSION,
        BudgetExhausted,
        PublicPage,
        RequestBudget,
        fingerprint,
        public_next_url,
        query_id,
        rejection_reasons,
        save_checkpoint,
        window_bounds,
    )

    for name, value in (
        ("limit", limit),
        ("per_query_cap", per_query_cap),
        ("timeout_ms", timeout_ms),
        ("max_results_per_query", max_results_per_query),
        ("max_requests", max_requests),
        ("max_pages_per_query", max_pages_per_query),
    ):
        if type(value) is not int or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    if max_queries is not None and (type(max_queries) is not int or max_queries <= 0):
        raise ValueError("max_queries must be positive or null")
    if (
        type(retries) is not int
        or retries < 0
        or query_delay < 0
        or detail_limit is not None
        and (type(detail_limit) is not int or detail_limit < 0)
    ):
        raise ValueError("invalid retry, pacing or detail bounds")
    if max_duration_seconds is not None and max_duration_seconds <= 0:
        raise ValueError("max_duration_seconds must be positive or null")
    if sort not in VALID_SORTS:
        raise ValueError("unsupported Threads sort")
    if run_id is not None and (
        not isinstance(run_id, str)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", run_id)
    ):
        raise ValueError("run_id must be a safe collection identifier")
    seeds = query_seeds if query_seeds is not None else [query]
    planner_options = {
        "categories": query_categories,
        "geo_keys": geo_keys,
        "include_local": include_local,
        "max_geo_aliases": max_geo_aliases,
        "expand_subject_variants": expand_subject_variants,
    }
    plan = expanded_query_plan(seeds, **planner_options)
    config_key = fingerprint(
        [
            PLANNER_VERSION,
            [asdict(p) for p in plan],
            sort,
            per_query_cap,
            max_results_per_query,
            max_pages_per_query,
            published_after,
            published_before,
            lookback_days,
        ]
    )
    path = Path(checkpoint_path) if checkpoint_path else None
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
    lock = path.with_suffix(path.suffix + ".lock").open("a") if path else nullcontext()
    with lock as held:
        if path:
            fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = (
            json.loads(path.read_text()) if path and path.exists() and resume else None
        )
        if state and state.get("config_key") != config_key:
            raise ValueError(
                "discovery checkpoint configuration changed; use a new checkpoint"
            )
        if state and not state.get("exported") and run_id and run_id != state["run_id"]:
            raise ValueError(
                "pending discovery checkpoint belongs to a different collection run"
            )
        if (
            state
            and state.get("exported")
            and (run_id == state["run_id"] or not state.get("artifact_committed"))
        ):
            if (
                run_id
                and run_id != state["run_id"]
                and not state.get("artifact_committed")
            ):
                raise ValueError(
                    "source artifact for the previous run has not been acknowledged"
                )
            _status_update(
                status_tracker,
                "start",
                seed_query=query,
                sort=sort,
                target_count=limit,
                query_plan_count=len(plan),
                run_id=state["run_id"],
                metadata={"source_checkpoint_replay": True},
            )
            return state["envelope"]
        fresh_survey = (
            not state or state.get("exported") and state["query_index"] >= len(plan)
        )
        now = datetime.now(UTC)
        if fresh_survey:
            lower, upper = window_bounds(
                published_after, published_before, lookback_days, now
            )
            state = {
                "schema_version": "1",
                "planner_version": PLANNER_VERSION,
                "config_key": config_key,
                "survey_id": uuid4().hex,
                "query_index": 0,
                "query_progress": None,
                "seen_ids": [],
                "accepted_ids": [],
                "bounds": [
                    lower.isoformat() if lower else None,
                    upper.isoformat() if upper else None,
                ],
                "exported": True,
            }
        if state.get("exported"):
            state.update(
                run_id=run_id or uuid4().hex,
                started_at=now.isoformat(),
                posts={},
                query_runs=[],
                search_evidence=[],
                detail_attempts=0,
                exported=False,
                artifact_committed=False,
            )
            state.pop("envelope", None)
        identity = state["run_id"]
        lower, upper = (
            datetime.fromisoformat(v) if v else None for v in state["bounds"]
        )
        _status_update(
            status_tracker,
            "start",
            seed_query=query,
            sort=sort,
            target_count=limit,
            query_plan_count=len(plan),
            run_id=identity,
            metadata={
                "planner_version": PLANNER_VERSION,
                "survey_id": state["survey_id"],
                "max_queries": max_queries,
                "max_requests": max_requests,
                "per_query_cap": per_query_cap,
                "max_pages_per_query": max_pages_per_query,
            },
        )
        collected = state["posts"]
        seen = set(state["seen_ids"])
        accepted_ids = set(state["accepted_ids"])
        budget = RequestBudget(max_requests, max_duration_seconds)
        query_attempts = 0
        consecutive_errors = 0
        stopping_reason = "query_plan_exhausted"

        def persist():
            state.update(
                posts=collected,
                seen_ids=sorted(seen),
                accepted_ids=sorted(accepted_ids),
            )
            if path:
                save_checkpoint(path, state)

        persist()
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=headless)
            search_page = PublicPage(
                browser.new_page(
                    viewport={"width": 1440, "height": 1000}, locale="vi-VN"
                ),
                budget,
            )
            detail_page = PublicPage(
                browser.new_page(
                    viewport={"width": 1440, "height": 1000}, locale="vi-VN"
                ),
                budget,
            )
            try:
                while state["query_index"] < len(plan):
                    if len(collected) >= limit:
                        stopping_reason = "result_limit"
                        break
                    if max_queries is not None and query_attempts >= max_queries:
                        stopping_reason = "query_budget"
                        break
                    try:
                        budget.check()
                    except BudgetExhausted as exc:
                        stopping_reason = str(exc)
                        break
                    spec = plan[state["query_index"]]
                    qid = query_id(spec.query)
                    query_attempts += 1
                    started = time.monotonic()
                    requests_before = budget.used
                    progress = state.get("query_progress") or {
                        "page": 1,
                        "next_url": None,
                        "fingerprints": [],
                        "returned": 0,
                        "accepted": 0,
                    }
                    qrun = {
                        "query": spec.query,
                        "query_id": qid,
                        "query_category": spec.query_category,
                        "kind": spec.kind,
                        "geo_name": spec.geo_name,
                        "status": "ok",
                        "blocked": False,
                        "raw_count": 0,
                        "source_exposure_count": 0,
                        "accepted_count": 0,
                        "accepted_duplicates_count": 0,
                        "relevance_rejected_count": 0,
                        "rejected_count": 0,
                        "rejection_reasons": {},
                        "rejection_reason_matches": {},
                        "unique_discovered_count": 0,
                        "duplicate_discovered_count": 0,
                        "within_page_duplicates_count": 0,
                        "parser_deferred_count": 0,
                        "not_evaluated_count": 0,
                        "detail_success_count": 0,
                        "detail_failed_count": 0,
                        "detail_skipped_count": 0,
                        "pages": [],
                        "requests": 0,
                    }
                    state["query_runs"].append(qrun)
                    _status_update(
                        status_tracker,
                        "query_started",
                        query=spec.query,
                        kind=spec.kind,
                        geo_name=spec.geo_name,
                    )
                    done = False
                    while not done:
                        try:
                            for attempt in range(retries + 1):
                                try:
                                    budget.check()
                                    search_page.last_search_evidence = None
                                    if progress.get("next_url"):
                                        raw_posts, page_info, landed_url = (
                                            _collect_public_page(
                                                search_page,
                                                url=progress["next_url"],
                                                query_spec=spec,
                                                sort=sort,
                                                timeout_ms=timeout_ms,
                                                raw_limit=max_results_per_query
                                                - progress["returned"],
                                            )
                                        )
                                    else:
                                        raw_posts, page_info, landed_url = (
                                            _collect_for_query(
                                                search_page,
                                                query_spec=spec,
                                                sort=sort,
                                                timeout_ms=timeout_ms,
                                                raw_limit=max_results_per_query
                                                - progress["returned"],
                                            )
                                        )
                                    break
                                except ThreadsAccessBlocked:
                                    raise
                                except ThreadsCrawlerError:
                                    if attempt == retries:
                                        raise
                                    search_page.wait_for_timeout(int(2**attempt * 1000))
                            captured = datetime.now(UTC).isoformat()
                            native = search_page.last_search_evidence
                            if not isinstance(native, dict):
                                native = {
                                    "source_exposure_count": len(raw_posts),
                                    "within_page_duplicates_count": 0,
                                    "parser_deferred_count": 0,
                                    "source_record_ids": [
                                        str(p["id"])
                                        for p in raw_posts
                                        if p.get("id") is not None
                                    ],
                                    "records": raw_posts,
                                }
                            page_fingerprint = fingerprint(
                                native.get(
                                    "search_results", native.get("records", raw_posts)
                                )
                            )
                            if page_fingerprint in progress["fingerprints"]:
                                qrun["pagination_stop"] = "repeated_public_page"
                                done = True
                                break
                            progress["fingerprints"].append(page_fingerprint)
                            evidence = {
                                "query_id": qid,
                                "query": spec.query,
                                "source_run_id": identity,
                                "observed_at": captured,
                                "page": progress["page"],
                                "landed_url": landed_url,
                                "page_info": page_info,
                                "native": native,
                                "decisions": [],
                            }
                            state["search_evidence"].append(evidence)
                            page_ids = set(native["source_record_ids"])
                            qrun["unique_discovered_count"] += len(page_ids - seen)
                            qrun["duplicate_discovered_count"] += len(page_ids & seen)
                            seen.update(page_ids)
                            qrun["raw_count"] += len(raw_posts)
                            qrun["source_exposure_count"] += native[
                                "source_exposure_count"
                            ]
                            qrun["within_page_duplicates_count"] += native[
                                "within_page_duplicates_count"
                            ]
                            qrun["parser_deferred_count"] += native[
                                "parser_deferred_count"
                            ]
                            progress["returned"] += len(raw_posts)
                            qrun["landed_url"] = landed_url
                            qrun["page_info"] = page_info
                            qrun["sort_preserved"] = sort != "recent" or parse_qs(
                                urlsplit(landed_url).query
                            ).get("filter") == ["recent"]
                            qrun["pages"].append(
                                {
                                    "page": progress["page"],
                                    "raw_count": len(raw_posts),
                                    "source_exposure_count": native[
                                        "source_exposure_count"
                                    ],
                                    "page_info": page_info,
                                    **publication_range(raw_posts),
                                }
                            )
                            for index, raw in enumerate(raw_posts):
                                sid = (
                                    str(raw["id"])
                                    if raw.get("id") is not None
                                    else None
                                )
                                if (
                                    len(collected) >= limit
                                    or progress["accepted"] >= per_query_cap
                                ):
                                    reason = (
                                        "result_limit"
                                        if len(collected) >= limit
                                        else "per_query_cap"
                                    )
                                    evidence["decisions"].append(
                                        {
                                            "source_record_id": sid,
                                            "outcome": "not_evaluated",
                                            "reason": reason,
                                        }
                                    )
                                    qrun["not_evaluated_count"] += 1
                                    continue
                                reasons = rejection_reasons(
                                    raw,
                                    spec,
                                    published_after=lower,
                                    published_before=upper,
                                )
                                if reasons:
                                    qrun["rejected_count"] += 1
                                    qrun["relevance_rejected_count"] += int(
                                        any(
                                            r
                                            in (
                                                "missing_pickleball_subject",
                                                "missing_market_topic",
                                                "missing_vietnam_evidence",
                                            )
                                            for r in reasons
                                        )
                                    )
                                    qrun["rejection_reasons"][reasons[0]] = (
                                        qrun["rejection_reasons"].get(reasons[0], 0) + 1
                                    )
                                    for r in reasons:
                                        qrun["rejection_reason_matches"][r] = (
                                            qrun["rejection_reason_matches"].get(r, 0)
                                            + 1
                                        )
                                    evidence["decisions"].append(
                                        {
                                            "source_record_id": sid,
                                            "outcome": "rejected",
                                            "reasons": reasons,
                                        }
                                    )
                                    continue
                                adapted = adapt_post(raw, query_spec=spec)
                                if sid in accepted_ids:
                                    qrun["accepted_duplicates_count"] += 1
                                    if sid in collected:
                                        merge_adapted_post(collected[sid], adapted)
                                    evidence["decisions"].append(
                                        {
                                            "source_record_id": sid,
                                            "outcome": "accepted_duplicate",
                                        }
                                    )
                                    continue
                                adapted["_observed_at"] = captured
                                if (
                                    detail_limit is None
                                    or state["detail_attempts"] < detail_limit
                                ):
                                    try:
                                        budget.check()
                                        state["detail_attempts"] += 1
                                        detail = _fetch_post_detail(
                                            detail_page,
                                            post=adapted,
                                            timeout_ms=timeout_ms,
                                        )
                                        _assert_post_detail_identity(adapted, detail)
                                        adapted.update(
                                            detail=detail, detail_status="ok"
                                        )
                                        qrun["detail_success_count"] += 1
                                    except ThreadsAccessBlocked as exc:
                                        adapted.update(
                                            detail=None,
                                            detail_status="error",
                                            detail_error=str(exc),
                                        )
                                        qrun.update(
                                            status="error", blocked=True, error=str(exc)
                                        )
                                        qrun["detail_failed_count"] += 1
                                        stopping_reason = "access_blocked"
                                    except BudgetExhausted as exc:
                                        adapted.update(detail=None, detail_status=None)
                                        qrun["detail_skipped_count"] += 1
                                        stopping_reason = str(exc)
                                    except ThreadsCrawlerError as exc:
                                        adapted.update(
                                            detail=None,
                                            detail_status="error",
                                            detail_error=str(exc),
                                        )
                                        qrun["detail_failed_count"] += 1
                                else:
                                    adapted.update(detail=None, detail_status=None)
                                    qrun["detail_skipped_count"] += 1
                                collected[sid] = adapted
                                accepted_ids.add(sid)
                                qrun["accepted_count"] += 1
                                progress["accepted"] += 1
                                evidence["decisions"].append(
                                    {"source_record_id": sid, "outcome": "accepted"}
                                )
                                if qrun["blocked"]:
                                    remaining = len(raw_posts) - index - 1
                                    qrun["not_evaluated_count"] += remaining
                                    evidence["decisions"].extend(
                                        {
                                            "source_record_id": str(p.get("id")),
                                            "outcome": "not_evaluated",
                                            "reason": "access_blocked",
                                        }
                                        for p in raw_posts[index + 1 :]
                                    )
                                    break
                            if qrun["blocked"]:
                                done = True
                            elif (
                                len(collected) >= limit
                                or progress["accepted"] >= per_query_cap
                            ):
                                qrun["pagination_stop"] = (
                                    "result_limit"
                                    if len(collected) >= limit
                                    else "per_query_cap"
                                )
                                done = True
                            elif progress["returned"] >= max_results_per_query:
                                qrun["pagination_stop"] = "per_query_result_limit"
                                done = True
                            elif progress["page"] >= max_pages_per_query:
                                qrun["pagination_stop"] = "page_budget"
                                done = True
                            elif not page_info or not page_info.get("has_next_page"):
                                qrun["pagination_stop"] = "no_public_continuation"
                                done = True
                            else:
                                target = public_next_url(search_page, spec.query)
                                if not target:
                                    qrun["pagination_stop"] = (
                                        "cursor_without_supported_public_navigation"
                                    )
                                    done = True
                                elif target == landed_url:
                                    qrun["pagination_stop"] = "repeated_public_link"
                                    done = True
                                else:
                                    progress.update(
                                        page=progress["page"] + 1, next_url=target
                                    )
                            state["query_progress"] = progress
                            persist()
                        except BudgetExhausted as exc:
                            stopping_reason = str(exc)
                            qrun["status"] = "partial"
                            qrun["pagination_stop"] = stopping_reason
                            break
                        except ThreadsAccessBlocked as exc:
                            stopping_reason = "access_blocked"
                            qrun.update(status="error", blocked=True, error=str(exc))
                            break
                        except ThreadsCrawlerError as exc:
                            qrun.update(status="error", error=str(exc))
                            done = True
                            consecutive_errors += 1
                            break
                    qrun.update(
                        duration_seconds=time.monotonic() - started,
                        requests=budget.used - requests_before,
                    )
                    if done and not qrun["blocked"]:
                        state["query_index"] += 1
                        state["query_progress"] = None
                    if qrun["status"] == "error":
                        _status_update(
                            status_tracker,
                            "query_failed",
                            query=spec.query,
                            kind=spec.kind,
                            geo_name=spec.geo_name,
                            error=qrun.get("error", "query failed"),
                        )
                    else:
                        consecutive_errors = 0
                        _status_update(
                            status_tracker,
                            "query_succeeded",
                            query=spec.query,
                            kind=spec.kind,
                            geo_name=spec.geo_name,
                            raw_count=qrun["source_exposure_count"],
                            accepted_count=qrun["accepted_count"],
                            total_collected=len(collected),
                            landed_url=qrun.get("landed_url"),
                        )
                    persist()
                    if stopping_reason in (
                        "access_blocked",
                        "request_budget",
                        "duration_budget",
                    ):
                        break
                    if consecutive_errors >= 3:
                        stopping_reason = "consecutive_source_errors"
                        break
                    search_page.wait_for_timeout(int(query_delay * 1000))
            finally:
                browser.close()
        if len(collected) >= limit and state["query_index"] < len(plan):
            stopping_reason = "result_limit"
        runs = state["query_runs"]
        totals = {
            name: sum(r.get(name, 0) for r in runs)
            for name in (
                "source_exposure_count",
                "raw_count",
                "accepted_count",
                "accepted_duplicates_count",
                "rejected_count",
                "duplicate_discovered_count",
                "unique_discovered_count",
                "within_page_duplicates_count",
                "parser_deferred_count",
                "not_evaluated_count",
                "detail_failed_count",
            )
        }
        has_more = state["query_index"] < len(plan)
        coverage = dict(
            planner_version=PLANNER_VERSION,
            survey_id=state["survey_id"],
            planned_queries=len(plan),
            attempted_queries=len(runs),
            successful_queries=sum(r["status"] == "ok" for r in runs),
            pagination_limited_queries=sum(
                r.get("pagination_stop")
                in {
                    "page_budget",
                    "per_query_result_limit",
                    "per_query_cap",
                    "cursor_without_supported_public_navigation",
                    "repeated_public_page",
                    "repeated_public_link",
                }
                for r in runs
            ),
            failed_queries=sum(r["status"] == "error" for r in runs),
            partial_queries=sum(r["status"] == "partial" for r in runs),
            queries_attempted_this_invocation=query_attempts,
            completed_query_cursor=state["query_index"],
            remaining_queries=len(plan) - state["query_index"],
            requests_this_invocation=budget.used,
            request_unit="public_page_navigation_or_reload",
            max_requests=max_requests,
            max_queries=max_queries,
            max_pages_per_query=max_pages_per_query,
            max_results_per_query=max_results_per_query,
            limit=limit,
            per_query_cap=per_query_cap,
            published_after=state["bounds"][0],
            published_before=state["bounds"][1],
            window_enforcement="local_source_timestamp_filter",
            has_more=has_more,
            **totals,
        )
        envelope = {
            "source": "threads",
            "seed_query": query,
            "sort": sort,
            "run_id": identity,
            "started_at": state["started_at"],
            "collected_at": datetime.now(UTC).isoformat(),
            "count": len(collected),
            "query_runs": runs,
            "posts": list(collected.values()),
            "collection_stopping_reason": stopping_reason,
            "discovered_unique_count": len(seen),
            "query_seeds": seeds,
            "query_plan": [{**asdict(p), "query_id": query_id(p.query)} for p in plan],
            "discovery": coverage,
            "search_evidence": state["search_evidence"],
        }
        state.update(exported=True, envelope=envelope)
        persist()
        return envelope


def save_json(payload: dict[str, Any], output_path: Path) -> Path:
    output_path = output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl Vietnam-oriented Threads posts to JSON."
    )
    parser.add_argument("--query", default=DEFAULT_QUERY)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument(
        "--sort",
        choices=sorted(VALID_SORTS),
        default=DEFAULT_SORT,
    )
    parser.add_argument(
        "--per-query-cap",
        type=int,
        default=DEFAULT_PER_QUERY_CAP,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=default_output_path(),
    )
    parser.add_argument("--discovery-config", type=Path)
    parser.add_argument("--max-queries", type=int)
    parser.add_argument("--max-requests", type=int)
    parser.add_argument("--max-pages-per-query", type=int)
    parser.add_argument("--published-after")
    parser.add_argument("--published-before")
    parser.add_argument("--lookback-days", type=int)
    parser.add_argument("--checkpoint", type=Path)
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Start a fresh discovery survey instead of resuming.",
    )
    parser.add_argument("--headed", action="store_true")
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=DEFAULT_TIMEOUT_MS,
    )
    return parser.parse_args()


def discovery_options(args):
    options = {}
    if args.discovery_config:
        config = json.loads(args.discovery_config.read_text())
        options = config.get("threads", config)
        if not isinstance(options, dict):
            raise ValueError("discovery config must be an object")
        options = {
            k: v
            for k, v in options.items()
            if k
            not in {"query", "limit", "sort", "per_query_cap", "timeout_ms", "headless"}
        }
    for name in (
        "max_queries",
        "max_requests",
        "max_pages_per_query",
        "published_after",
        "published_before",
        "lookback_days",
    ):
        value = getattr(args, name)
        if value is not None:
            options[name] = value
    if args.checkpoint:
        options["checkpoint_path"] = args.checkpoint
    if args.fresh:
        options["resume"] = False
    return options


def main() -> int:
    args = _parse_args()

    print(
        f"[threads-crawler] start seed={args.query!r} "
        f"sort={args.sort!r} limit={args.limit}",
        flush=True,
    )

    # Status tracking is optional and must not prevent collection.
    try:
        status_tracker: CrawlerStatusTracker | None = CrawlerStatusTracker(
            source="threads"
        )
    except Exception as exc:  # noqa: BLE001
        print(
            f"[threads-crawler] status initialization failed: {exc}",
            flush=True,
        )
        status_tracker = None

    try:
        payload = crawl_threads_posts(
            query=args.query,
            limit=args.limit,
            sort=args.sort,
            headless=not args.headed,
            timeout_ms=args.timeout_ms,
            per_query_cap=args.per_query_cap,
            status_tracker=status_tracker,
            **discovery_options(args),
        )
        output_path = save_json(payload, args.output)
        options = discovery_options(args)
        if options.get("checkpoint_path"):
            from dsp_collector.sources.crawler.threads.discovery import (
                acknowledge_checkpoint,
            )

            acknowledge_checkpoint(options["checkpoint_path"], payload["run_id"])
        _status_update(
            status_tracker,
            "succeed",
            collected_count=payload["count"],
            output_path=output_path,
        )
    except (ThreadsCrawlerError, ValueError) as exc:
        _status_update(
            status_tracker,
            "fail",
            error=exc,
        )
        print(f"[threads-crawler] ERROR: {exc}", flush=True)
        return 1

    print(
        f"[threads-crawler] saved {payload['count']} posts to {output_path}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
