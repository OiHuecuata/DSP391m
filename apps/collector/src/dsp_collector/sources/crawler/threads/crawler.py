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
import random
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit

from playwright.sync_api import Page, sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

try:
    from .vietnam_terms import (
        QuerySpec,
        adapt_post,
        build_query_plan,
        merge_adapted_post,
    )
except ImportError:
    # Direct execution support:
    # python .../threads/crawler.py
    from vietnam_terms import (
        QuerySpec,
        adapt_post,
        build_query_plan,
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


def expanded_query_plan(seeds):
    plan, seen = [], set()
    for seed in seeds:
        for spec in build_query_plan(seed):
            key = " ".join(spec.query.casefold().split())
            if key not in seen:
                seen.add(key)
                plan.append(spec)
    return plan


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
        "like_count": post.get("like_count") or 0,
        "reply_count": info.get("direct_reply_count") or 0,
        "repost_count": info.get("repost_count") or 0,
        "quote_count": info.get("quote_count") or 0,
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

    for edge in search_results.get("edges") or []:
        if not isinstance(edge, dict):
            continue

        post = _post_from_edge(edge)
        if post is None:
            continue

        post_id = post.get("id")
        if post_id is not None:
            post_id = str(post_id)
            if post_id in seen_ids:
                continue
            seen_ids.add(post_id)

        posts.append(post)

        if len(posts) >= limit:
            break

    return posts, search_results.get("page_info")


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
    posts, page_info = parse_posts(
        html=html,
        limit=raw_limit,
    )
    return posts, page_info, landed_url


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
    detail_limit: int | None = None,
    query_delay: float = 0.75,
    retries: int = 1,
) -> dict[str, Any]:
    """Execute the domain query plan and collect adapted posts."""
    if limit < 1:
        raise ValueError("limit must be >= 1")
    if per_query_cap < 1:
        raise ValueError("per_query_cap must be >= 1")

    if (
        max_results_per_query < 1
        or query_delay < 0
        or retries < 0
        or (detail_limit is not None and detail_limit < 0)
    ):
        raise ValueError("Invalid collection bounds")
    query_plan = expanded_query_plan(query_seeds or [query])

    _status_update(
        status_tracker,
        "start",
        seed_query=query,
        sort=sort,
        target_count=limit,
        query_plan_count=len(query_plan),
        metadata={
            "per_query_cap": per_query_cap,
            "headless": headless,
        },
    )

    collected: dict[str, dict[str, Any]] = {}
    query_runs: list[dict[str, Any]] = []
    discovered_ids = set()
    detail_attempts = 0
    consecutive_errors = 0
    stopping_reason = "query_plan_exhausted"

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        search_page = browser.new_page(
            viewport={"width": 1440, "height": 1000},
            locale="vi-VN",
        )
        detail_page = browser.new_page(
            viewport={"width": 1440, "height": 1000},
            locale="vi-VN",
        )

        try:
            for query_spec in query_plan:
                if len(collected) >= limit:
                    stopping_reason = "result_limit"
                    break
                query_started = time.monotonic()

                _status_update(
                    status_tracker,
                    "query_started",
                    query=query_spec.query,
                    kind=query_spec.kind,
                    geo_name=query_spec.geo_name,
                )

                try:
                    for attempt in range(retries + 1):
                        try:
                            raw_posts, page_info, landed_url = _collect_for_query(
                                search_page,
                                query_spec=query_spec,
                                sort=sort,
                                timeout_ms=timeout_ms,
                                raw_limit=max_results_per_query,
                            )
                            break
                        except ThreadsAccessBlocked:
                            raise
                        except ThreadsCrawlerError:
                            if attempt == retries:
                                raise
                            search_page.wait_for_timeout(
                                int((2**attempt + random.uniform(0, 0.5)) * 1000)
                            )
                    consecutive_errors = 0
                except ThreadsAccessBlocked as exc:
                    query_runs.append(
                        {
                            "query": query_spec.query,
                            "kind": query_spec.kind,
                            "geo_name": query_spec.geo_name,
                            "status": "error",
                            "error": str(exc),
                            "blocked": True,
                        }
                    )
                    stopping_reason = "access_blocked"
                    break
                except ThreadsCrawlerError as exc:
                    print(
                        f"[threads-crawler] query failed "
                        f"{query_spec.query!r}: {exc}",
                        flush=True,
                    )
                    query_runs.append(
                        {
                            "query": query_spec.query,
                            "kind": query_spec.kind,
                            "geo_name": query_spec.geo_name,
                            "status": "error",
                            "error": str(exc),
                        }
                    )
                    _status_update(
                        status_tracker,
                        "query_failed",
                        query=query_spec.query,
                        kind=query_spec.kind,
                        geo_name=query_spec.geo_name,
                        error=exc,
                    )
                    consecutive_errors += 1
                    if consecutive_errors >= 3:
                        stopping_reason = "consecutive_source_errors"
                        break
                    search_page.wait_for_timeout(int(query_delay * 1000))
                    continue

                accepted_this_query = 0
                detail_success_count = 0
                detail_failed_count = 0
                detail_skipped_count = 0
                eligible_duplicates = 0
                irrelevant_count = 0
                query_ids = {str(p["id"]) for p in raw_posts if p.get("id") is not None}
                newly_discovered = len(query_ids - discovered_ids)
                discovered_ids.update(query_ids)
                detail_seconds = 0
                detail_blocked = False

                for raw_post in raw_posts:
                    adapted = adapt_post(
                        raw_post,
                        query_spec=query_spec,
                    )
                    if adapted is None:
                        irrelevant_count += 1
                        continue

                    post_id = adapted.get("id")
                    if post_id is None:
                        continue

                    post_id = str(post_id)

                    if post_id in collected:
                        eligible_duplicates += 1
                        merge_adapted_post(
                            collected[post_id],
                            adapted,
                        )
                        continue

                    if detail_limit is None or detail_attempts < detail_limit:
                        detail_started = time.monotonic()
                        detail_attempts += 1
                        try:
                            detail = _fetch_post_detail(
                                detail_page, post=adapted, timeout_ms=timeout_ms
                            )
                            _assert_post_detail_identity(adapted, detail)
                            adapted["detail"] = detail
                            adapted["detail_status"] = "ok"
                            detail_success_count += 1
                        except ThreadsAccessBlocked as exc:
                            adapted.update(
                                detail=None,
                                detail_status="error",
                                detail_error=str(exc),
                            )
                            detail_failed_count += 1
                            detail_blocked = True
                            stopping_reason = "access_blocked"
                        except ThreadsCrawlerError as exc:
                            adapted.update(
                                detail=None,
                                detail_status="error",
                                detail_error=str(exc),
                            )
                            detail_failed_count += 1
                        detail_seconds += time.monotonic() - detail_started
                    else:
                        adapted.update(detail=None, detail_status=None)
                        detail_skipped_count += 1

                    collected[post_id] = adapted
                    accepted_this_query += 1

                    if detail_blocked:
                        break

                    if accepted_this_query >= per_query_cap:
                        break
                    if len(collected) >= limit:
                        break

                query_runs.append(
                    {
                        "query": query_spec.query,
                        "kind": query_spec.kind,
                        "geo_name": query_spec.geo_name,
                        "status": "error" if detail_blocked else "ok",
                        "blocked": detail_blocked,
                        "landed_url": landed_url,
                        "raw_count": len(raw_posts),
                        "accepted_count": accepted_this_query,
                        "detail_success_count": detail_success_count,
                        "detail_failed_count": detail_failed_count,
                        "detail_skipped_count": detail_skipped_count,
                        "unique_discovered_count": newly_discovered,
                        "duplicate_discovered_count": len(query_ids) - newly_discovered,
                        "accepted_duplicates_count": eligible_duplicates,
                        "relevance_rejected_count": irrelevant_count,
                        "duration_seconds": time.monotonic() - query_started,
                        "detail_seconds": detail_seconds,
                        **publication_range(raw_posts),
                        "pages": [
                            {
                                "page": 1,
                                "raw_count": len(raw_posts),
                                "page_info": page_info,
                            }
                        ],
                        "pagination_stop": (
                            "no_public_continuation"
                            if not page_info or not page_info.get("has_next_page")
                            else "cursor_without_supported_public_navigation"
                        ),
                        "page_info": page_info,
                    }
                )

                _status_update(
                    status_tracker,
                    "query_succeeded",
                    query=query_spec.query,
                    kind=query_spec.kind,
                    geo_name=query_spec.geo_name,
                    raw_count=len(raw_posts),
                    accepted_count=accepted_this_query,
                    total_collected=len(collected),
                    landed_url=landed_url,
                )

                print(
                    f"[threads-crawler] query={query_spec.query!r} "
                    f"raw={len(raw_posts)} accepted={accepted_this_query} "
                    f"detail_ok={detail_success_count} "
                    f"detail_failed={detail_failed_count} "
                    f"total={len(collected)}/{limit}",
                    flush=True,
                )

                search_page.wait_for_timeout(
                    int((query_delay + random.uniform(0, 0.25)) * 1000)
                )
                if detail_blocked:
                    break
        finally:
            browser.close()

    posts = list(collected.values())[:limit]

    if not posts:
        if stopping_reason == "access_blocked":
            raise ThreadsAccessBlocked(
                query_runs[-1].get("error", "Public collection was blocked")
            )
        raise ThreadsCrawlerError(
            "No posts matched the configured Vietnam relevance policy"
        )

    return {
        "source": "threads",
        "seed_query": query,
        "sort": sort,
        "collected_at": datetime.now(UTC).isoformat(),
        "count": len(posts),
        "query_runs": query_runs,
        "posts": posts,
        "collection_stopping_reason": stopping_reason,
        "discovered_unique_count": len(discovered_ids),
        "query_seeds": query_seeds or [query],
    }


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
    parser.add_argument("--headed", action="store_true")
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=DEFAULT_TIMEOUT_MS,
    )
    return parser.parse_args()


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
        )
        output_path = save_json(payload, args.output)
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
