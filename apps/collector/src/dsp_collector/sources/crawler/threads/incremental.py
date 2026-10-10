"""Bounded public collection with durable per-fetch checkpoints and exact retry.

No cursor API is inferred. Only a source-provided same-query public next link
can be followed. Unsupported historical search remains a reported limitation.
"""

import copy
import fcntl
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlsplit

from ....normalizers.observation import canonical_json, digest
from ....runtime.durable import ArtifactStore
from ....runtime.source_policy import admit, policy_hash, validate_payload
from .crawler import (
    ThreadsAccessBlocked,
    _assert_post_detail_identity,
    build_search_url,
    ensure_public_access,
    expanded_query_plan,
    parse_posts,
)
from .rich_fields import additional_fields
from .vietnam_terms import adapt_post


def _checkpoint(path, value):
    temporary = path.with_suffix(".pending")
    with temporary.open("w") as out:
        out.write(canonical_json(value))
        out.flush()
        os.fsync(out.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


class PublicFetcher:
    def __init__(self, *, pacing=1.0, timeout_ms=60000):
        self.pacing, self.timeout_ms = pacing, timeout_ms

    def __call__(self, spec, seen, budget, pages):
        from playwright.sync_api import sync_playwright

        from .post_parser import parse_post_detail

        posts, used, limitation = [], 0, "public_results_exhausted"
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                search = browser.new_page(locale="vi-VN")
                detail = browser.new_page(locale="vi-VN")
                url = build_search_url(spec.query, sort="recent")
                response = search.goto(
                    url, wait_until="domcontentloaded", timeout=self.timeout_ms
                )
                ensure_public_access(search, response)
                search.wait_for_timeout(1500)
                raw, page_info = parse_posts(html=search.content(), limit=1000)
                landed = search.url
                used += 1
                visited = {landed}
                for page_index in range(pages):
                    for candidate in raw:
                        if str(candidate.get("id")) in seen:
                            continue
                        adapted = adapt_post(candidate, query_spec=spec)
                        if (
                            adapted is None
                            and "pickleball" in candidate.get("text", "").casefold()
                        ):
                            adapted = copy.deepcopy(
                                candidate
                            )  # General discussion; features remain exploratory.
                        if adapted is None:
                            continue
                        adapted.update(
                            additional_fields(candidate.get("raw_payload") or {})
                        )
                        adapted["detail_status"] = "BUDGET_SKIPPED"
                        if used < budget:
                            time.sleep(self.pacing)
                            used += 1
                            try:
                                response = detail.goto(
                                    candidate["url"],
                                    wait_until="domcontentloaded",
                                    timeout=self.timeout_ms,
                                )
                                ensure_public_access(detail, response)
                                detail.wait_for_timeout(1500)
                                value = parse_post_detail(
                                    detail.content(),
                                    expected_post_id=str(candidate["id"]),
                                    expected_code=candidate.get("code"),
                                    source_url=candidate["url"],
                                )
                                _assert_post_detail_identity(candidate, value)
                                adapted.update(
                                    detail=value,
                                    detail_status="OK",
                                    detail_error=None,
                                    detail_observed_at=datetime.now(UTC).isoformat(),
                                )
                            except ThreadsAccessBlocked:
                                raise
                            except Exception as error:
                                adapted.update(
                                    detail=None,
                                    detail_status="ERROR",
                                    detail_error=type(error).__name__,
                                )
                        posts.append(adapted)
                        seen.add(str(candidate["id"]))
                    if not page_info or not page_info.get("has_next_page"):
                        break
                    limitation = "cursor_without_supported_public_navigation"
                    links = search.locator('a[rel="next"]').all()
                    if not links or used >= budget or page_index + 1 >= pages:
                        break
                    next_url = urljoin(search.url, links[0].get_attribute("href") or "")
                    parsed = urlsplit(next_url)
                    if (
                        parsed.scheme != "https"
                        or parsed.netloc not in ("www.threads.com", "www.threads.net")
                        or parsed.path != "/search"
                        or parse_qs(parsed.query).get("q") != [spec.query]
                        or next_url in visited
                    ):
                        break
                    time.sleep(self.pacing)
                    response = search.goto(
                        next_url, wait_until="domcontentloaded", timeout=self.timeout_ms
                    )
                    ensure_public_access(search, response)
                    used += 1
                    visited.add(next_url)
                    raw, page_info = parse_posts(html=search.content(), limit=1000)
                    limitation = "public_page_budget"
            finally:
                browser.close()
        return {"posts": posts, "requests": used, "limitation": limitation}


def collect_incremental(
    config, *, run_id, directory, policy, fetcher=None, fixture=False, progress=None
):
    origin = admit(policy, fixture=fixture)  # Before browser or any source request.
    if origin == "SYNTHETIC" and fetcher is None:
        raise PermissionError("Synthetic mode requires an injected fixture fetcher")
    if origin == "SYNTHETIC" and isinstance(fetcher, PublicFetcher):
        raise PermissionError("The live public interface cannot be labeled synthetic")
    if not run_id or "/" in run_id or ".." in run_id:
        raise ValueError("Invalid acquisition run ID")
    target = config.get("unique_target", 100)
    budget = config.get("request_budget", 250)
    concurrency = config.get("concurrency", 1)
    pages = config.get("max_public_pages", 3)
    if (
        any(type(v) is not int or v < 1 for v in (target, budget, concurrency, pages))
        or concurrency > 4
    ):
        raise ValueError("Positive bounds and concurrency 1..4 required")
    pacing = config.get("pacing_seconds", 1.0)
    if type(pacing) not in (int, float) or pacing < 0.5:
        raise ValueError("Public collection pacing must be >= 0.5 seconds")
    seeds = config.get(
        "queries",
        [
            "pickleball",
            "pickleball tìm sân",
            "pickleball đặt sân",
            "pickleball giá thuê",
            "pickleball ghép kèo",
            "pickleball tiện ích",
        ],
    )
    if (
        not seeds
        or len(seeds) > 64
        or any(
            not isinstance(seed, str) or not seed.strip() or len(seed) > 256
            for seed in seeds
        )
    ):
        raise ValueError("Supply 1..64 nonempty query seeds")
    windows = {
        key: datetime.fromisoformat(config[key]) if config.get(key) else None
        for key in ("published_from", "published_to")
    }
    if any(
        value is not None and value.utcoffset() is None for value in windows.values()
    ):
        raise ValueError("Publication filters require explicit timezone timestamps")
    if (
        windows["published_from"]
        and windows["published_to"]
        and windows["published_from"] > windows["published_to"]
    ):
        raise ValueError("Publication window is reversed")
    plan = expanded_query_plan(seeds)
    identity = digest([run_id, config, policy_hash(policy)])
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    store = ArtifactStore(config["raw_base_uri"])
    fetcher = fetcher or PublicFetcher(pacing=pacing)
    with (directory / "acquisition.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        path = directory / "fetch-checkpoint.json"
        state = (
            json.loads(path.read_text())
            if path.exists()
            else {
                "identity": identity,
                "seen": [],
                "queries": {},
                "artifacts": [],
                "requests": 0,
            }
        )
        if state["identity"] != identity:
            raise ValueError("Resume configuration or reviewed policy changed")
        seen = set(state["seen"])
        remaining = [
            s
            for s in plan
            if state["queries"].get(digest(s.query), {}).get("status") != "OK"
        ]

        def fetch_or_resume(spec, reservation):
            slot = digest([run_id, "sha256:" + digest(spec.query)])
            saved = store.optional(store.uri(f"threads/{run_id}/{slot}.json"))
            return (
                {"saved": saved}
                if saved is not None
                else fetcher(spec, set(seen), reservation, pages)
            )

        while remaining and len(seen) < target and state["requests"] < budget:
            chosen = remaining[: min(concurrency, budget - state["requests"])]
            remaining = remaining[len(chosen) :]
            reservation = max(1, (budget - state["requests"]) // len(chosen))
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                futures = [
                    pool.submit(fetch_or_resume, spec, reservation) for spec in chosen
                ]
                for spec, future in zip(chosen, futures):
                    query_id = "sha256:" + digest(spec.query)
                    slot = digest([run_id, query_id])
                    key = f"threads/{run_id}/{slot}.json"
                    try:
                        result = future.result()
                    except Exception as error:
                        state["requests"] += reservation
                        state["queries"][digest(spec.query)] = {
                            "query": spec.query,
                            "status": "FAILED",
                            "error": type(error).__name__,
                        }
                        _checkpoint(path, state)
                        if isinstance(error, ThreadsAccessBlocked):
                            state["stop_reason"] = "ACCESS_BLOCKED"
                            _checkpoint(path, state)
                            return state
                        continue
                    # A raw write may precede its checkpoint. Retry reads that slot first.
                    try:
                        result = future.result()
                        raw = result.get("saved")
                        if raw is None:
                            raise FileNotFoundError()
                        envelope = json.loads(raw)
                    except FileNotFoundError:
                        try:
                            result = future.result()
                        except Exception as error:
                            state["requests"] += (
                                reservation  # Conservative bound after an uncertain failure.
                            )
                            state["queries"][digest(spec.query)] = {
                                "query": spec.query,
                                "status": "FAILED",
                                "error": type(error).__name__,
                            }
                            _checkpoint(path, state)
                            if isinstance(error, ThreadsAccessBlocked):
                                return state
                            continue
                        if result["requests"] > reservation:
                            raise ValueError("Fetcher exceeded reserved request budget")
                        posts = []
                        for post in result["posts"]:
                            if str(post["id"]) in seen or len(seen) >= target:
                                continue
                            published = (post.get("detail") or {}).get(
                                "taken_at", post.get("taken_at")
                            )
                            published = (
                                datetime.fromtimestamp(published, UTC)
                                if type(published) in (int, float)
                                else (
                                    datetime.fromisoformat(published)
                                    if published
                                    else None
                                )
                            )
                            if (
                                windows["published_from"]
                                and (
                                    not published
                                    or published < windows["published_from"]
                                )
                            ) or (
                                windows["published_to"]
                                and (
                                    not published or published > windows["published_to"]
                                )
                            ):
                                continue
                            posts.append(post)
                            seen.add(str(post["id"]))
                        now = datetime.now(UTC)
                        envelope = {
                            "source": "threads",
                            "run_id": run_id,
                            "collected_at": now.isoformat(),
                            "count": len(posts),
                            "posts": posts,
                            "query": spec.query,
                            "query_geo_name": spec.geo_name,
                            "query_geo_key": spec.geo_key,
                            "source_policy_sha256": policy_hash(policy),
                            "requests": result["requests"],
                            "pagination_limitation": result["limitation"],
                            "historical_window_status": "PUBLIC_RESULTS_FILTER_ONLY_NO_BACKFILL_GUARANTEE",
                            "native_observation_context": {
                                "origin": origin,
                                "permission_reference": policy["reference"],
                                "retention_policy_id": policy.get(
                                    "retention_policy_id", "isolated-fixture"
                                ),
                                "retention_expires_at": (
                                    now
                                    + timedelta(days=policy.get("retention_days", 7))
                                ).isoformat(),
                                "provenance_scope": "FETCH_BATCH",
                                "fetch_batch_id": "sha256:" + slot,
                                "query_id": query_id,
                                "observed_at": now.isoformat(),
                            },
                        }
                        validate_payload(envelope, policy)
                        raw = canonical_json(envelope).encode()
                        store.put(key, raw)
                    except Exception as error:
                        if getattr(error, "response", {}).get("Error", {}).get(
                            "Code"
                        ) in ("NoSuchKey", "404"):
                            raise RuntimeError(
                                "S3 raw slot absent; retry through acquisition storage adapter"
                            ) from error
                        raise
                    seen.update(str(p["id"]) for p in envelope["posts"])
                    reference = store.put(key, raw)
                    if reference not in state["artifacts"]:
                        state["artifacts"].append(reference)
                        state["requests"] += envelope["requests"]
                    state["seen"] = sorted(seen)
                    state["queries"][digest(spec.query)] = {
                        "query": spec.query,
                        "query_id": query_id,
                        "status": "OK",
                        "accepted": envelope["count"],
                        "limitation": envelope["pagination_limitation"],
                    }
                    _checkpoint(path, state)
                    if progress:
                        progress(
                            {
                                "queries_processed": len(state["queries"]),
                                "query_plan_count": len(plan),
                                "collected": len(seen),
                            }
                        )
        state["stop_reason"] = (
            "UNIQUE_TARGET"
            if len(seen) >= target
            else (
                "REQUEST_BUDGET"
                if state["requests"] >= budget
                else "QUERY_PLAN_EXHAUSTED"
            )
        )
        _checkpoint(path, state)
        return state
