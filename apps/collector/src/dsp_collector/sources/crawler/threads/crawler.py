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
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import Page, sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

try:
    from .vietnam_terms import (
        QuerySpec,
    )
except ImportError:
    # Direct execution support:
    # python .../threads/crawler.py
    from vietnam_terms import (
        QuerySpec,
    )

try:
    from .post_parser import (
        ThreadsPostParserError,
        extract_post_candidates,
        parse_post_detail,
    )
except ImportError:
    # Direct execution support:
    # python .../threads/crawler.py
    from post_parser import (
        ThreadsPostParserError,
        extract_post_candidates,
        parse_post_detail,
    )

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


def browser_factory():
    """Keep the public browser factory injectable for isolated collector tests."""
    return sync_playwright()


class ThreadsCrawlerError(RuntimeError):
    """Raised when Threads search data cannot be fetched or parsed."""

    def __init__(self, message, *, classification=None):
        super().__init__(message)
        self.classification = classification or "source_error"


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

    user = post.get("user") if isinstance(post.get("user"), dict) else {}
    info = (
        post.get("text_post_app_info")
        if isinstance(post.get("text_post_app_info"), dict)
        else {}
    )
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
            "is_verified": (
                user.get("is_verified")
                if type(user.get("is_verified")) is bool
                else None
            ),
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
            all_posts=posts,
            ssr_candidate_count=len(candidates),
            ssr_candidates=candidates,
            extractable_edge_counts=[
                _candidate_post_count(candidate) for candidate in candidates
            ],
            malformed_edge_count=len(search_results.get("edges") or [])
            - len(source_posts),
            parser_decision="most_extractable_posts",
            empty_classification=(
                "empty_selected_ssr_unverified" if not source_posts else "nonempty"
            ),
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
        page.last_navigation_diagnostics = {
            "requested_url": search_url,
            "http_status": response.status if response else None,
            "requested_sort": sort,
            "readiness": "domcontentloaded_then_public_marker",
        }
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
    except PlaywrightError as exc:
        raise ThreadsCrawlerError(
            f"Threads navigation failed for query {query!r}: {type(exc).__name__}",
            classification=(
                "navigation_timeout"
                if isinstance(exc, PlaywrightTimeoutError)
                else "navigation_failure"
            ),
        ) from exc

    page.last_navigation_diagnostics.update(
        landed_url=page.url,
        redirected=page.url != search_url,
        effective_sort=(
            "recent"
            if parse_qs(urlsplit(page.url).query).get("filter") == ["recent"]
            else "source_default"
        ),
        marker_present=SEARCH_RESULTS_MARKER in html,
    )
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
    evidence["navigation"] = page.last_navigation_diagnostics
    evidence["navigation"]["readiness_attempts"] = 1
    if not posts:
        page.wait_for_timeout(1000)
        following = page.evaluate("document.documentElement.innerHTML")
        next_evidence = {}
        following_posts, following_info = parse_posts(
            following, raw_limit, evidence=next_evidence
        )
        stable = next_evidence["search_results"] == evidence["search_results"]
        navigation = evidence["navigation"]
        navigation["readiness_attempts"] = 2
        evidence = next_evidence
        evidence["navigation"] = navigation
        evidence["empty_classification"] = (
            "stable_empty_public_ssr"
            if stable and not following_posts and not evidence["malformed_edge_count"]
            else "nonempty" if following_posts else "ambiguous_empty"
        )
        posts, page_info = following_posts, following_info
    page.last_search_evidence = evidence
    return posts, page_info, landed_url


def _collect_public_page(page, *, url, query_spec, sort, timeout_ms, raw_limit):
    try:
        response = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        ensure_public_access(page, response)
        page.wait_for_timeout(2000)
        html = page.evaluate("document.documentElement.innerHTML")
    except PlaywrightError as exc:
        raise ThreadsCrawlerError(
            "Public pagination navigation failed",
            classification=(
                "pagination_timeout"
                if isinstance(exc, PlaywrightTimeoutError)
                else "pagination_navigation_failure"
            ),
        ) from exc
    evidence = {}
    posts, page_info = parse_posts(html, raw_limit, evidence=evidence)
    evidence["navigation"] = {
        "requested_url": url,
        "landed_url": page.url,
        "http_status": response.status if response else None,
        "requested_sort": sort,
        "effective_sort": (
            "recent"
            if parse_qs(urlsplit(page.url).query).get("filter") == ["recent"]
            else "source_default"
        ),
    }
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
    except PlaywrightError as exc:
        raise ThreadsCrawlerError(
            f"Post detail timed out after {timeout_ms} ms: {url}"
        ) from exc

    page.last_detail_evidence = {
        "source_url": url,
        "observed_at": datetime.now(UTC).isoformat(),
        "candidates": extract_post_candidates(html),
    }
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
        except PlaywrightError as exc:
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
    max_geo_aliases: int | None = None,
    expand_subject_variants: bool = True,
    published_after: str | None = None,
    published_before: str | None = None,
    lookback_days: int | None = None,
    checkpoint_path: str | Path | None = None,
    resume: bool = True,
    max_query_failures: int = 3,
    retry_backoff_seconds: float = 30,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Execute a durable public discovery survey with independent run budgets."""
    options = locals().copy()
    for name in (
        "limit",
        "per_query_cap",
        "timeout_ms",
        "max_results_per_query",
        "max_requests",
        "max_pages_per_query",
        "max_query_failures",
    ):
        if type(options[name]) is not int or options[name] <= 0:
            raise ValueError(f"{name} must be a positive integer")
    if max_queries is not None and (type(max_queries) is not int or max_queries <= 0):
        raise ValueError("max_queries must be positive or null")
    if (
        type(retries) is not int
        or retries < 0
        or query_delay < 0
        or retry_backoff_seconds < 0
    ):
        raise ValueError("Invalid retry or pacing limits")
    if detail_limit is not None and (type(detail_limit) is not int or detail_limit < 0):
        raise ValueError("Invalid detail limit")
    if max_duration_seconds is not None and max_duration_seconds <= 0:
        raise ValueError("Invalid duration limit")
    if sort not in VALID_SORTS:
        raise ValueError("Unsupported Threads sort")
    if run_id is not None and (
        not isinstance(run_id, str)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", run_id)
    ):
        raise ValueError("Invalid run identity")
    from dsp_collector.sources.crawler.threads.execution import execute

    return execute(options)


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
