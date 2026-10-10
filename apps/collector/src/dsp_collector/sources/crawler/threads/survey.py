"""Stable query coverage and non-destructive migration of saved surveys."""

import unicodedata
from dataclasses import asdict
from datetime import UTC, datetime

from .discovery import (
    MARKET_QUERIES,
    fingerprint,
    geography_registry,
    normalized,
    query_id,
)


def legacy_plan(
    seeds,
    *,
    include_local,
    max_geo_aliases=2,
    categories=None,
    expand_subject_variants=True,
):
    """Exact v2 composition used only to prove legacy checkpoint identities."""
    from .vietnam_terms import COUNTRY_SCOPE, LOCAL_AREA_ALIASES, PICKLEBALL_TERMS

    scopes = [
        (None, None, "fallback", [""]),
        (
            COUNTRY_SCOPE.key,
            COUNTRY_SCOPE.name,
            "country",
            list(COUNTRY_SCOPE.search_terms),
        ),
    ]
    for row in geography_registry():
        terms = []
        seen = set()
        for term in [row["canonical_name"], *row["aliases"]]:
            if normalized(term) not in seen:
                terms.append(term)
                seen.add(normalized(term))
        scopes.append(
            (
                row["location_id"],
                row["canonical_name"],
                "city",
                terms[:max_geo_aliases] if max_geo_aliases else terms,
            )
        )
    if include_local:
        for name, aliases in LOCAL_AREA_ALIASES.items():
            terms = list(dict.fromkeys([name, *aliases]))
            scopes.append(
                (
                    "local:" + normalized(name),
                    name,
                    "city",
                    terms[:max_geo_aliases] if max_geo_aliases else terms,
                )
            )
    if expand_subject_variants:
        seeds = list(seeds) + [
            v
            for seed in seeds
            if normalized(seed) == "pickleball"
            for v in PICKLEBALL_TERMS
        ]
    families = []
    seed_seen = set()
    for seed in seeds:
        seed = " ".join(unicodedata.normalize("NFKC", seed).split())
        if normalized(seed) in seed_seen:
            continue
        seed_seen.add(normalized(seed))
        families.append((seed, seed, "subject"))
        for category in categories if categories is not None else MARKET_QUERIES:
            for template in MARKET_QUERIES[category]:
                families.append((template.format(seed=seed), seed, category))
    plan = []
    seen = set()
    for alias in range(max(len(s[3]) for s in scopes)):
        for text, seed, category in families:
            for geo_key, name, kind, terms in scopes:
                if alias >= len(terms):
                    continue
                phrase = (text + " " + terms[alias]).strip()
                if normalized(phrase) in seen:
                    continue
                seen.add(normalized(phrase))
                plan.append(
                    {
                        "query": phrase,
                        "seed": seed,
                        "geo_key": geo_key,
                        "geo_name": name,
                        "priority": len(plan),
                        "kind": kind,
                        "query_category": category,
                    }
                )
    return plan


TERMINAL = {
    "exhausted",
    "terminal_failed",
    "unsupported_continuation",
    "legacy_completed",
}


def migrate(state, plan, settings_key):
    """Migrate in memory; callers persist only as part of an authorized run.

    Old cursor positions are resolved against the saved old plan, never against
    the new ordering. Removed queries remain in the ledger for provenance.
    """
    if state.get("coverage_version") != "2":
        old_plan = state.get("envelope", {}).get("query_plan", [])
        if state.get("query_index") and not old_plan:
            raise ValueError(
                "Legacy checkpoint lacks its original plan; cannot safely migrate"
            )
        ledger = {}
        known = {r["query"]: r for r in state.get("query_runs", [])}
        for spec in old_plan[: state.get("query_index", 0)]:
            run = known.get(spec["query"], {})
            status = (
                "retry_pending" if run.get("status") == "error" else "legacy_completed"
            )
            if run.get("pagination_stop") in {
                "page_budget",
                "per_query_result_limit",
                "per_query_cap",
                "result_limit",
            }:
                status = "deferred"
            ledger[query_id(spec["query"])] = {
                "status": status,
                "query": spec["query"],
                "migration_evidence": "saved_legacy_plan",
                "attempts": 1,
                "attempts_are_lower_bound": True,
            }
        progress = state.get("query_progress")
        if progress and state.get("query_index", 0) < len(old_plan):
            spec = old_plan[state["query_index"]]
            ledger[query_id(spec["query"])] = {
                "status": "deferred",
                "query": spec["query"],
                "progress": progress,
                "attempts": 0,
            }
        state.update(
            coverage_version="2",
            ledger=ledger,
            schedule_cursor=0,
            migration={
                "from": "1",
                "old_cursor": state.get("query_index", 0),
                "old_plan_count": len(old_plan),
            },
        )
    state["settings_key"] = settings_key
    state["plan_id"] = fingerprint([asdict(q) for q in plan])
    for run in state.get("query_runs", []):
        run.setdefault(
            "coverage_status",
            state["ledger"].get(query_id(run["query"]), {}).get("status", "pending"),
        )
        run.setdefault("pages", [])
    return state


def select_query(state, plan, attempted):
    now = datetime.now(UTC).timestamp()
    start = state.get("schedule_cursor", 0) % len(plan)
    for step in range(len(plan)):
        index = (start + step) % len(plan)
        spec = plan[index]
        qid = query_id(spec.query)
        row = state["ledger"].get(qid, {})
        if (
            qid in attempted
            or row.get("status") in TERMINAL
            or row.get("retry_after", 0) > now
        ):
            continue
        state["schedule_cursor"] = (index + 1) % len(plan)
        return spec, qid
    return None


def coverage(state, plan):
    now = datetime.now(UTC).timestamp()
    ids = {query_id(q.query) for q in plan}
    ledger = state["ledger"]
    counts = {
        key: sum(ledger.get(qid, {}).get("status") == key for qid in ids)
        for key in [
            "exhausted",
            "legacy_completed",
            "deferred",
            "retry_pending",
            "terminal_failed",
            "unsupported_continuation",
        ]
    }
    completed = sum(counts[k] for k in TERMINAL)
    by_geo = {}
    by_category = {}
    for spec in plan:
        status = ledger.get(query_id(spec.query), {}).get("status", "pending")
        for p in spec.geo_provenance:
            key = p["admin_version"]
            group = by_geo.setdefault(
                key, {"planned": 0, "attempted": 0, "exhausted": 0, "geo_ids": set()}
            )
            group["planned"] += 1
            group["attempted"] += int(
                bool(ledger.get(query_id(spec.query), {}).get("attempts"))
            )
            group["exhausted"] += int(status == "exhausted")
            group["geo_ids"].add(p["geo_id"])
        group = by_category.setdefault(
            spec.query_category or "subject",
            {"planned": 0, "attempted": 0, "completed": 0},
        )
        group["planned"] += 1
        group["attempted"] += int(
            bool(ledger.get(query_id(spec.query), {}).get("attempts"))
        )
        group["completed"] += int(status in TERMINAL)
    for group in by_geo.values():
        group["geographic_references"] = len(group.pop("geo_ids"))
    return {
        "eligible_queries": sum(
            ledger.get(qid, {}).get("status") not in TERMINAL
            and ledger.get(qid, {}).get("retry_after", 0) <= now
            for qid in ids
        ),
        "terminal_queries": completed,
        "completed_queries": completed,
        "exhausted_queries": counts["exhausted"],
        "legacy_completed_queries": counts["legacy_completed"],
        "deferred_queries": counts["deferred"],
        "retry_pending_queries": counts["retry_pending"],
        "terminal_failed_queries": counts["terminal_failed"],
        "unsupported_continuation_queries": counts["unsupported_continuation"],
        "remaining_queries": len(ids) - completed,
        "has_more": completed < len(ids),
        "geographic_coverage": by_geo,
        "semantic_coverage": by_category,
    }
