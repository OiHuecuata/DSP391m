"""Bounded public-page execution of a persistent, versioned query survey."""

import fcntl
import json
import time
from contextlib import nullcontext
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from .discovery import (
    PLANNER_VERSION,
    BudgetExhausted,
    PublicPage,
    RequestBudget,
    fingerprint,
    query_id,
    rejection_reasons,
    save_checkpoint,
    window_bounds,
)
from .survey import coverage, legacy_plan, migrate, select_query
from .vietnam_terms import adapt_post, merge_adapted_post


def execute(options):
    from . import crawler as c
    from . import discovery as d

    o = options
    seeds = o["query_seeds"] if o["query_seeds"] is not None else [o["query"]]
    plan = c.expanded_query_plan(
        seeds,
        categories=o["query_categories"],
        geo_keys=o["geo_keys"],
        include_local=o["include_local"],
        max_geo_aliases=o["max_geo_aliases"],
        expand_subject_variants=o["expand_subject_variants"],
    )
    settings = [
        o[k] for k in ("sort", "published_after", "published_before", "lookback_days")
    ]
    key = fingerprint(settings)
    path = Path(o["checkpoint_path"]) if o["checkpoint_path"] else None
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
    lock = path.with_suffix(path.suffix + ".lock").open("a") if path else nullcontext()
    with lock as held:
        if path:
            fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = (
            json.loads(path.read_text())
            if path and path.exists() and o["resume"]
            else None
        )
        if path and path.exists() and not o["resume"]:
            raise ValueError(
                "Cannot reset an existing survey; select a separate checkpoint"
            )
        rid = o["run_id"]
        if state and not state.get("exported") and rid and rid != state["run_id"]:
            raise ValueError(
                "pending discovery checkpoint belongs to a different collection run"
            )
        if (
            state
            and state.get("exported")
            and (rid == state["run_id"] or not state.get("artifact_committed"))
        ):
            if rid and rid != state["run_id"]:
                raise ValueError("source artifact has not been acknowledged")
            c._status_update(
                o["status_tracker"],
                "start",
                seed_query=o["query"],
                sort=o["sort"],
                target_count=o["limit"],
                query_plan_count=len(plan),
                run_id=state["run_id"],
                metadata={"operation": "processing_replay"},
            )
            return state["envelope"]
        if state and state.get("settings_key") and state["settings_key"] != key:
            raise ValueError(
                "Publication window or sort changed; use a separate survey"
            )
        if state and state.get("coverage_version") != "2":
            old = state.get("envelope", {})
            if not old and state.get("planner_version") == "2":
                # Interrupted v2 checkpoints did not save their plan. Rebuild
                # only when its SHA-256 configuration proves an exact match.
                reconstructed = None
                for categories in [
                    None,
                    [v for v in (o["query_categories"] or []) if v in d.MARKET_QUERIES],
                ]:
                    candidate = legacy_plan(
                        seeds,
                        include_local=o["include_local"],
                        categories=categories,
                        expand_subject_variants=o["expand_subject_variants"],
                    )
                    proof = fingerprint(
                        [
                            "2",
                            candidate,
                            o["sort"],
                            o["per_query_cap"],
                            o["max_results_per_query"],
                            o["max_pages_per_query"],
                            o["published_after"],
                            o["published_before"],
                            o["lookback_days"],
                        ]
                    )
                    if proof == state.get("config_key"):
                        reconstructed = candidate
                        break
                if reconstructed is None:
                    raise ValueError(
                        "Cannot prove the original interrupted legacy plan; checkpoint preserved"
                    )
                old = {
                    "source": "threads",
                    "sort": o["sort"],
                    "query_plan": reconstructed,
                    "discovery": {
                        k: o[k]
                        for k in (
                            "per_query_cap",
                            "max_results_per_query",
                            "max_pages_per_query",
                        )
                    },
                }
                state["envelope"] = old
            old_discovery = old.get("discovery", {})
            if old and state.get("config_key"):
                original_plan = [
                    {k: v for k, v in q.items() if k != "query_id"}
                    for q in old.get("query_plan", [])
                ]
                legacy_key = fingerprint(
                    [
                        state.get("planner_version", "2"),
                        original_plan,
                        old.get("sort"),
                        old_discovery.get("per_query_cap", o["per_query_cap"]),
                        old_discovery.get(
                            "max_results_per_query", o["max_results_per_query"]
                        ),
                        old_discovery.get(
                            "max_pages_per_query", o["max_pages_per_query"]
                        ),
                        o["published_after"],
                        o["published_before"],
                        o["lookback_days"],
                    ]
                )
                if legacy_key != state["config_key"]:
                    raise ValueError(
                        "Legacy survey configuration cannot be safely matched"
                    )
            if old and (
                old.get("sort") != o["sort"]
                or state.get("bounds", [None, None]) != [None, None]
                and not any(settings[1:])
            ):
                raise ValueError(
                    "Legacy survey filters conflict with requested settings"
                )
        now = datetime.now(UTC)
        if not state:
            lower, upper = window_bounds(
                o["published_after"], o["published_before"], o["lookback_days"], now
            )
            state = {
                "schema_version": "2",
                "survey_id": uuid4().hex,
                "query_index": 0,
                "bounds": [v.isoformat() if v else None for v in (lower, upper)],
                "seen_ids": [],
                "exported": True,
            }
        state = migrate(state, plan, key)
        state["planner_version"] = PLANNER_VERSION
        # Finished surveys remain persistent. A new survey is an explicit new
        # checkpoint; plan expansion adds work without erasing completed IDs.
        previous = state.get("run_id")
        operation = (
            "interrupted_recovery"
            if not state.get("exported")
            else "discovery_continuation"
        )
        if state.get("exported"):
            state.update(
                run_id=rid or uuid4().hex,
                started_at=now.isoformat(),
                posts={},
                query_runs=[],
                search_evidence=[],
                detail_attempts=0,
                exported=False,
                artifact_committed=False,
                continuation_of=previous,
            )
            state.pop("envelope", None)
        identity = state["run_id"]
        posts = state["posts"]
        seen = set(state.get("seen_ids", []))
        accepted = set(
            posts
        )  # cross-query duplicate encounters within this acquisition
        lower, upper = (
            datetime.fromisoformat(v) if v else None for v in state["bounds"]
        )
        budget = RequestBudget(o["max_requests"], o["max_duration_seconds"])
        eligible_at_start = coverage(state, plan)["eligible_queries"]
        attempted = set()
        consecutive = 0
        stop = "query_plan_exhausted"

        def publish_progress(survey=None):
            c._status_update(
                o["status_tracker"], "discovery_progress",
                batch={
                    "requests_used": budget.used,
                    "max_requests": o["max_requests"],
                    "max_queries": o["max_queries"],
                    "queries_executed": sum(
                        bool(q.get("search_navigations")) for q in state["query_runs"]
                    ),
                    "technical_errors": sum(
                        q.get("status") == "error" for q in state["query_runs"]
                    ),
                },
                survey=survey,
            )

        # Navigation telemetry must not imply that a query is completed.
        budget.on_consume = publish_progress

        def persist(confirm_coverage=False):
            state["seen_ids"] = sorted(seen)
            if path:
                save_checkpoint(path, state)
            if not path or not confirm_coverage:
                publish_progress()
                return
            cov = coverage(state, plan)
            publish_progress({
                **{k: cov[k] for k in (
                    "completed_queries", "remaining_queries", "eligible_queries",
                    "deferred_queries", "retry_pending_queries", "has_more",
                )},
                "survey_id": state["survey_id"],
                "plan_id": state["plan_id"],
                "planner_version": PLANNER_VERSION,
                "candidate_queries": len(plan),
                "updated_at": datetime.now(UTC).isoformat(),
            })

        c._status_update(
            o["status_tracker"],
            "start",
            seed_query=o["query"],
            sort=o["sort"],
            target_count=o["limit"],
            query_plan_count=len(plan),
            run_id=identity,
            metadata={
                "planner_version": PLANNER_VERSION,
                "survey_id": state["survey_id"],
                "operation": operation,
            },
        )
        persist(confirm_coverage=True)
        with c.browser_factory() as playwright:
            browser = playwright.chromium.launch(headless=o["headless"])
            search = PublicPage(
                browser.new_page(
                    viewport={"width": 1440, "height": 1000}, locale="vi-VN"
                ),
                budget,
            )
            detail = PublicPage(
                browser.new_page(
                    viewport={"width": 1440, "height": 1000}, locale="vi-VN"
                ),
                budget,
            )
            try:
                while True:
                    if len(posts) >= o["limit"]:
                        stop = "result_limit"
                        break
                    if (
                        o["max_queries"] is not None
                        and len(attempted) >= o["max_queries"]
                    ):
                        stop = "query_budget"
                        break
                    try:
                        budget.check()
                    except BudgetExhausted as exc:
                        stop = str(exc)
                        break
                    selected = select_query(state, plan, attempted)
                    if selected is None:
                        stop = (
                            "deferred_work"
                            if coverage(state, plan)["has_more"]
                            else "query_plan_exhausted"
                        )
                        break
                    spec, qid = selected
                    attempted.add(qid)
                    work = state["ledger"].setdefault(
                        qid, {"query": spec.query, "status": "pending", "attempts": 0}
                    )
                    work["attempts"] += 1
                    progress = work.setdefault(
                        "progress", {"page": 1, "next_url": None, "fingerprints": []}
                    )
                    run = {
                        "query": spec.query,
                        "query_id": qid,
                        "query_category": spec.query_category,
                        "geo_provenance": list(spec.geo_provenance),
                        "kind": spec.kind,
                        "geo_name": spec.geo_name,
                        "status": "ok",
                        "blocked": False,
                        "pages": [],
                        "search_navigations": 0,
                        "raw_count": 0,
                        "source_exposure_count": 0,
                        "accepted_count": 0,
                        "accepted_duplicates_count": 0,
                        "rejected_count": 0,
                        "relevance_rejected_count": 0,
                        "rejection_reasons": {},
                        "rejection_reason_matches": {},
                        "unique_discovered_count": 0,
                        "duplicate_discovered_count": 0,
                        "within_page_duplicates_count": 0,
                        "parser_deferred_count": 0,
                        "not_evaluated_count": 0,
                        "detail_failed_count": 0,
                        "detail_success_count": 0,
                        "detail_skipped_count": 0,
                    }
                    state["query_runs"].append(run)
                    requests_before = budget.used
                    began = time.monotonic()
                    pages_this_run = 0
                    raw_this_run = 0
                    accepted_this_query = 0
                    c._status_update(
                        o["status_tracker"],
                        "query_started",
                        query=spec.query,
                        kind=spec.kind,
                        geo_name=spec.geo_name,
                    )
                    persist()
                    try:
                        while True:
                            staged = work.get("staged_page")
                            if staged is None:
                                for attempt in range(o["retries"] + 1):
                                    search_before = budget.used
                                    try:
                                        search.last_search_evidence = None
                                        if progress.get("next_url"):
                                            records, info, url = c._collect_public_page(
                                                search,
                                                url=progress["next_url"],
                                                query_spec=spec,
                                                sort=o["sort"],
                                                timeout_ms=o["timeout_ms"],
                                                raw_limit=o["max_results_per_query"],
                                            )
                                        else:
                                            records, info, url = c._collect_for_query(
                                                search,
                                                query_spec=spec,
                                                sort=o["sort"],
                                                timeout_ms=o["timeout_ms"],
                                                raw_limit=o["max_results_per_query"],
                                            )
                                        break
                                    except c.ThreadsAccessBlocked:
                                        raise
                                    except c.ThreadsCrawlerError as exc:
                                        run.setdefault("retry_errors", []).append(
                                            {
                                                "attempt": attempt + 1,
                                                "classification": exc.classification,
                                                "error": str(exc),
                                            }
                                        )
                                        if attempt == o["retries"]:
                                            raise
                                        search.wait_for_timeout(int(1000 * 2**attempt))
                                    finally:
                                        run["search_navigations"] += (
                                            budget.used - search_before
                                        )
                                captured = datetime.now(UTC).isoformat()
                                native = search.last_search_evidence or {
                                    "records": records,
                                    "source_record_ids": [
                                        str(p["id"])
                                        for p in records
                                        if p.get("id") is not None
                                    ],
                                    "source_exposure_count": len(records),
                                    "within_page_duplicates_count": 0,
                                    "parser_deferred_count": 0,
                                }
                                full_records = native.get("all_posts", records)
                                fp = fingerprint(
                                    native.get(
                                        "search_results", native.get("records", records)
                                    )
                                )
                                target = (
                                    d.public_next_url(search, spec.query)
                                    if info and info.get("has_next_page")
                                    else None
                                )
                                staged = {
                                    "records": full_records,
                                    "index": 0,
                                    "observed_at": captured,
                                    "capture_run_id": identity,
                                    "landed_url": url,
                                    "page_info": info,
                                    "native": native,
                                    "fingerprint": fp,
                                    "next_url": target,
                                }
                                work["staged_page"] = staged
                                # Commit raw source evidence before ANY optional detail request.
                                persist()
                            evidence = {
                                "query_id": qid,
                                "query": spec.query,
                                "source_run_id": identity,
                                "capture_run_id": staged["capture_run_id"],
                                "observed_at": staged["observed_at"],
                                "page": progress["page"],
                                "landed_url": staged["landed_url"],
                                "page_info": staged["page_info"],
                                "native": staged["native"],
                                "decisions": [],
                            }
                            state["search_evidence"].append(evidence)
                            native = staged["native"]
                            records = staged["records"]
                            info = staged["page_info"]
                            page_ids = set(native["source_record_ids"])
                            if (
                                not staged.get("exposure_accounted")
                                and staged["capture_run_id"] == identity
                            ):
                                run["unique_discovered_count"] += len(page_ids - seen)
                                run["duplicate_discovered_count"] += len(
                                    page_ids & seen
                                )
                                seen.update(page_ids)
                                run["source_exposure_count"] += native[
                                    "source_exposure_count"
                                ]
                                run["within_page_duplicates_count"] += native.get(
                                    "within_page_duplicates_count", 0
                                )
                                run["parser_deferred_count"] += native.get(
                                    "parser_deferred_count", 0
                                )
                                staged["exposure_accounted"] = True
                            else:
                                run["retained_page_records_processed"] = (
                                    len(records) - staged["index"]
                                )
                            run["landed_url"] = staged["landed_url"]
                            run["page_info"] = info
                            from urllib.parse import parse_qs, urlsplit

                            run["sort_preserved"] = o["sort"] != "recent" or parse_qs(
                                urlsplit(staged["landed_url"]).query
                            ).get("filter") == ["recent"]
                            run["pages"].append(
                                {
                                    "page": progress["page"],
                                    "page_info": info,
                                    "source_exposure_count": native[
                                        "source_exposure_count"
                                    ],
                                    **c.publication_range(records),
                                }
                            )
                            repeated = staged["fingerprint"] in progress["fingerprints"]
                            while staged["index"] < len(records):
                                if (
                                    len(posts) >= o["limit"]
                                    or raw_this_run >= o["max_results_per_query"]
                                    or accepted_this_query >= o["per_query_cap"]
                                ):
                                    run["not_evaluated_count"] += (
                                        len(records) - staged["index"]
                                    )
                                    work["status"] = "deferred"
                                    run["status"] = "partial"
                                    run["pagination_stop"] = (
                                        "result_limit"
                                        if len(posts) >= o["limit"]
                                        else "per_query_result_limit"
                                    )
                                    break
                                raw = records[staged["index"]]
                                sid = (
                                    str(raw.get("id"))
                                    if raw.get("id") is not None
                                    else None
                                )
                                staged["index"] += 1
                                raw_this_run += 1
                                run["raw_count"] += 1
                                reasons = rejection_reasons(
                                    raw,
                                    spec,
                                    published_after=lower,
                                    published_before=upper,
                                )
                                if reasons:
                                    run["rejected_count"] += 1
                                    run["relevance_rejected_count"] += int(
                                        any(
                                            r.startswith("missing_")
                                            and r != "missing_publication_time"
                                            for r in reasons
                                        )
                                    )
                                    run["rejection_reasons"][reasons[0]] = (
                                        run["rejection_reasons"].get(reasons[0], 0) + 1
                                    )
                                    for reason in reasons:
                                        run["rejection_reason_matches"][reason] = (
                                            run["rejection_reason_matches"].get(
                                                reason, 0
                                            )
                                            + 1
                                        )
                                    evidence["decisions"].append(
                                        {
                                            "source_record_id": sid,
                                            "outcome": "rejected",
                                            "reasons": reasons,
                                        }
                                    )
                                    persist()
                                    continue
                                adapted = adapt_post(
                                    raw, query_spec=spec, expanded_topics=True
                                )
                                if sid in accepted:
                                    run["accepted_duplicates_count"] += 1
                                    merge_adapted_post(posts[sid], adapted)
                                    evidence["decisions"].append(
                                        {
                                            "source_record_id": sid,
                                            "outcome": "accepted_duplicate",
                                        }
                                    )
                                    persist()
                                    continue
                                adapted.update(
                                    _observed_at=staged["observed_at"],
                                    capture_run_id=staged["capture_run_id"],
                                    detail=None,
                                    detail_status=None,
                                )
                                for context in adapted["query_contexts"]:
                                    context["capture_run_id"] = staged["capture_run_id"]
                                posts[sid] = adapted
                                accepted.add(sid)
                                accepted_this_query += 1
                                run["accepted_count"] += 1
                                evidence["decisions"].append(
                                    {"source_record_id": sid, "outcome": "accepted"}
                                )
                                persist()
                                if (
                                    o["detail_limit"] is None
                                    or state["detail_attempts"] < o["detail_limit"]
                                ):
                                    try:
                                        budget.check()
                                        state["detail_attempts"] += 1
                                        adapted["detail_coverage"] = {
                                            "status": "interrupted_or_pending",
                                            "complete": False,
                                        }
                                        persist()
                                        detail.last_detail_evidence = None
                                        resolved = c._fetch_post_detail(
                                            detail,
                                            post=adapted,
                                            timeout_ms=o["timeout_ms"],
                                        )
                                        c._assert_post_detail_identity(
                                            adapted, resolved
                                        )
                                        observed = datetime.now(UTC).isoformat()
                                        resolved["_observed_at"] = observed
                                        for reply in resolved.get(
                                            "visible_replies", []
                                        ):
                                            reply["_observed_at"] = observed
                                        adapted.update(
                                            detail=resolved,
                                            detail_status="ok",
                                            _metrics_observed_at=observed,
                                        )
                                        adapted["detail_coverage"] = {
                                            "status": "visible_public_ssr",
                                            "complete": False,
                                            "visible_replies": len(
                                                resolved.get("visible_replies", [])
                                            ),
                                            "reply_pagination": "not_supported",
                                        }
                                        run["detail_success_count"] += 1
                                    except BudgetExhausted:
                                        run["detail_skipped_count"] += 1
                                    except c.ThreadsAccessBlocked as exc:
                                        adapted.update(
                                            detail_status="error", detail_error=str(exc)
                                        )
                                        run["detail_failed_count"] += 1
                                        run["failure_stage"] = "detail"
                                        raise
                                    except (
                                        c.ThreadsCrawlerError,
                                        ValueError,
                                        TypeError,
                                    ) as exc:
                                        adapted.update(
                                            detail_status="error", detail_error=str(exc)
                                        )
                                        run["detail_failed_count"] += 1
                                    if isinstance(detail.last_detail_evidence, dict):
                                        run.setdefault("detail_evidence", []).append(
                                            detail.last_detail_evidence
                                        )
                                    persist()
                                else:
                                    run["detail_skipped_count"] += 1
                            if staged["index"] < len(records):
                                break
                            work.pop("staged_page", None)
                            progress["fingerprints"].append(staged["fingerprint"])
                            pages_this_run += 1
                            if not records and native.get("empty_classification") in {
                                "ambiguous_empty",
                                "empty_selected_ssr_unverified",
                            }:
                                work["empty_attempts"] = (
                                    work.get("empty_attempts", 0) + 1
                                )
                                work["status"] = (
                                    "unsupported_continuation"
                                    if work["empty_attempts"] >= o["max_query_failures"]
                                    else "retry_pending"
                                )
                                work["retry_after"] = (
                                    datetime.now(UTC).timestamp()
                                    + o["retry_backoff_seconds"]
                                )
                                run.update(
                                    status="partial",
                                    pagination_stop="ambiguous_empty",
                                    diagnostic_failure="unverified_search_readiness",
                                )
                                progress["fingerprints"] = []
                                break
                            if repeated:
                                work["status"] = "unsupported_continuation"
                                run["pagination_stop"] = "repeated_public_page"
                                break
                            if not info or not info.get("has_next_page"):
                                work["status"] = "exhausted"
                                run["pagination_stop"] = "no_public_continuation"
                                break
                            if not staged["next_url"]:
                                work["status"] = "unsupported_continuation"
                                run["pagination_stop"] = (
                                    "cursor_without_supported_public_navigation"
                                )
                                break
                            progress.update(
                                page=progress["page"] + 1, next_url=staged["next_url"]
                            )
                            if (
                                pages_this_run >= o["max_pages_per_query"]
                                or raw_this_run >= o["max_results_per_query"]
                                or accepted_this_query >= o["per_query_cap"]
                            ):
                                work["status"] = "deferred"
                                run["status"] = "partial"
                                run["pagination_stop"] = "page_budget"
                                break
                            persist()
                            budget.check()
                        consecutive = 0
                    except BudgetExhausted as exc:
                        work["status"] = "deferred"
                        run["status"] = "partial"
                        stop = str(exc)
                        run["pagination_stop"] = stop
                    except c.ThreadsAccessBlocked as exc:
                        work["status"] = "retry_pending"
                        run.update(status="error", blocked=True, error=str(exc))
                        stop = "access_blocked"
                    except c.ThreadsCrawlerError as exc:
                        work["failures"] = work.get("failures", 0) + 1
                        work["status"] = (
                            "terminal_failed"
                            if work["failures"] >= o["max_query_failures"]
                            else "retry_pending"
                        )
                        work["retry_after"] = datetime.now(UTC).timestamp() + o[
                            "retry_backoff_seconds"
                        ] * 2 ** min(work["failures"] - 1, 8)
                        run.update(
                            status="error",
                            error=str(exc),
                            failure_classification=exc.classification,
                        )
                        if isinstance(
                            getattr(search, "last_navigation_diagnostics", None), dict
                        ):
                            run["navigation_diagnostics"] = (
                                search.last_navigation_diagnostics
                            )
                        consecutive += 1
                    finally:
                        run.update(
                            requests=budget.used - requests_before,
                            duration_seconds=time.monotonic() - began,
                            coverage_status=work["status"],
                        )
                        persist(confirm_coverage=True)
                    method = (
                        "query_failed"
                        if run["status"] == "error"
                        else "query_succeeded"
                    )
                    common = {
                        "query": spec.query,
                        "kind": spec.kind,
                        "geo_name": spec.geo_name,
                    }
                    if method == "query_failed":
                        common["error"] = run.get("error", "query failed")
                    else:
                        common.update(
                            raw_count=run["source_exposure_count"],
                            accepted_count=run["accepted_count"],
                            total_collected=len(posts),
                            landed_url=run.get("landed_url"),
                        )
                    c._status_update(o["status_tracker"], method, **common)
                    if stop in {"access_blocked", "request_budget", "duration_budget"}:
                        break
                    if consecutive >= 3:
                        stop = "consecutive_source_errors"
                        break
                    try:
                        search.wait_for_timeout(int(o["query_delay"] * 1000))
                    except BudgetExhausted as exc:
                        stop = str(exc)
                        break
            finally:
                browser.close()
        cov = coverage(state, plan)
        totals = {
            k: sum(q.get(k, 0) for q in state["query_runs"])
            for k in (
                "source_exposure_count",
                "raw_count",
                "accepted_count",
                "accepted_duplicates_count",
                "rejected_count",
                "relevance_rejected_count",
                "duplicate_discovered_count",
                "unique_discovered_count",
                "within_page_duplicates_count",
                "parser_deferred_count",
                "not_evaluated_count",
                "detail_failed_count",
            )
        }
        discovery = {
            **cov,
            **totals,
            "planner_version": PLANNER_VERSION,
            "plan_id": state["plan_id"],
            "survey_id": state["survey_id"],
            "candidate_queries": len(plan),
            "candidates_before_phrase_dedup": getattr(
                plan, "candidates_before_phrase_dedup", len(plan)
            ),
            "deduplicated_unique_queries": len(plan),
            "scheduled_queries": len(attempted),
            "eligible_queries_at_start": eligible_at_start,
            "transient_failures": sum(
                len(q.get("retry_errors", [])) for q in state["query_runs"]
            ),
            "executed_queries": sum(
                bool(q.get("search_navigations")) for q in state["query_runs"]
            ),
            "search_navigations": sum(
                q.get("search_navigations", 0) for q in state["query_runs"]
            ),
            "detail_navigations": sum(
                max(0, q.get("requests", 0) - q.get("search_navigations", 0))
                for q in state["query_runs"]
            ),
            "planned_queries": len(plan),
            "attempted_queries": len(state["query_runs"]),
            "queries_attempted_this_invocation": len(attempted),
            "successful_queries": sum(
                q["status"] != "error" and bool(q["pages"]) for q in state["query_runs"]
            ),
            "failed_queries": sum(q["status"] == "error" for q in state["query_runs"]),
            "partial_queries": sum(
                q["status"] == "partial" for q in state["query_runs"]
            ),
            "pagination_limited_queries": sum(
                q["coverage_status"] in {"deferred", "unsupported_continuation"}
                for q in state["query_runs"]
            ),
            "completed_query_cursor": cov["completed_queries"],
            "requests_this_invocation": budget.used,
            "request_unit": "public_page_navigation_or_reload",
            **{
                k: o[k]
                for k in (
                    "max_requests",
                    "max_queries",
                    "max_pages_per_query",
                    "max_results_per_query",
                    "limit",
                    "per_query_cap",
                )
            },
            "published_after": state["bounds"][0],
            "published_before": state["bounds"][1],
            "window_enforcement": "local_source_timestamp_filter",
            "continuation_of": state.get("continuation_of"),
            "checkpoint_migration": state.get("migration"),
        }
        envelope = {
            "schema_version": "1",
            "source": "threads",
            "seed_query": o["query"],
            "sort": o["sort"],
            "run_id": identity,
            "started_at": state["started_at"],
            "collected_at": datetime.now(UTC).isoformat(),
            "count": len(posts),
            "posts": list(posts.values()),
            "query_runs": state["query_runs"],
            "search_evidence": state["search_evidence"],
            "query_plan": [
                {
                    **asdict(q),
                    "geo_provenance": list(q.geo_provenance),
                    "semantic_provenance": list(q.semantic_provenance),
                    "query_id": query_id(q.query),
                }
                for q in plan
                if query_id(q.query) in attempted
            ],
            "query_universe": {
                "scope": "query_plan contains attempted queries; universe reconstructable from versioned references",
                "plan_id": state["plan_id"],
                "planner_version": PLANNER_VERSION,
                "geography": d.reference_resource("threads_geography.v1.json"),
                "vocabulary": d.reference_resource("threads_vocabulary.v1.json"),
            },
            "query_seeds": seeds,
            "discovered_unique_count": len(seen),
            "collection_stopping_reason": stop,
            "discovery": discovery,
            "operation": operation,
            "collection_configuration": {
                k: v
                for k, v in o.items()
                if k not in {"status_tracker", "checkpoint_path"}
            },
            "checkpoint": {
                "artifact_committed": False,
                "survey_complete": not cov["has_more"],
            },
        }
        from ....runtime.acquisition import threads_outcome

        envelope["acquisition"] = threads_outcome(envelope)
        envelope["checkpoint"]["survey_complete"] = envelope["acquisition"][
            "survey_complete"
        ]
        state.update(exported=True, envelope=envelope)
        persist(confirm_coverage=True)
        return envelope
