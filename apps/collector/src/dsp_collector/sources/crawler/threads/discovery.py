"""Bounded public discovery planning, query evidence and durable checkpoints."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlsplit

from ....runtime.bronze import check_secrets
from .vietnam_terms import (
    COUNTRY_SCOPE,
    LOCAL_AREA_ALIASES,
    PICKLEBALL_TERMS,
    QuerySpec,
    assess_post,
)

PLANNER_VERSION = "2"
MARKET_QUERIES = {
    "venues": ("sân {seed}", "khai trương sân {seed}"),
    "pricing": ("giá thuê sân {seed}", "đặt sân {seed}"),
    "players": ("ghép kèo {seed}", "tìm người chơi {seed}"),
    "training": ("lớp học {seed}", "huấn luyện {seed}"),
}


def normalized(value):
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def fingerprint(value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)
    return "sha256:" + hashlib.sha256(raw.encode()).hexdigest()


def query_id(query):
    return fingerprint(["threads", normalized(query)])


@lru_cache(maxsize=1)
def geography_registry():
    root = next(
        (
            p
            for p in Path(__file__).resolve().parents
            if (p / "configs/reference/vietnam_locations.v1.json").is_file()
        ),
        None,
    )
    path = (
        root / "configs/reference/vietnam_locations.v1.json"
        if root
        else files("dsp_contracts") / "reference/vietnam_locations.v1.json"
    )
    return json.loads(path.read_text())["locations"]


def build_discovery_plan(
    seeds,
    *,
    categories=None,
    geo_keys=None,
    include_local=False,
    max_geo_aliases=2,
    expand_subject_variants=True,
):
    """Interleave subject, intent and geographic scopes; budgets govern execution.

    Geographic terms come from the existing registry, not a invented national
    list. Case/spacing/Unicode variants deduplicate; distinct accented search
    spellings remain separate. Query intent is discovery context, not an NLP label.
    """
    if (
        not isinstance(seeds, list)
        or not seeds
        or any(not isinstance(s, str) or not s.strip() for s in seeds)
    ):
        raise ValueError("query_seeds must be nonempty strings")
    categories = list(MARKET_QUERIES) if categories is None else categories
    if not isinstance(categories, list) or any(
        c not in MARKET_QUERIES for c in categories
    ):
        raise ValueError("unknown discovery category")
    if max_geo_aliases is not None and (
        type(max_geo_aliases) is not int or max_geo_aliases < 1
    ):
        raise ValueError("max_geo_aliases must be positive or null")
    registry = geography_registry()
    known = {r["location_id"] for r in registry}
    if geo_keys is not None and (
        not isinstance(geo_keys, list) or set(geo_keys) - known
    ):
        raise ValueError("geo_keys must refer to the existing geographic registry")
    scopes = [
        (None, None, "fallback", [""]),
        (
            COUNTRY_SCOPE.key,
            COUNTRY_SCOPE.name,
            "country",
            list(COUNTRY_SCOPE.search_terms),
        ),
    ]
    for row in registry:
        if geo_keys is not None and row["location_id"] not in geo_keys:
            continue
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
            variant
            for seed in seeds
            if normalized(seed) == "pickleball"
            for variant in PICKLEBALL_TERMS
        ]
    families = []
    seed_seen = set()
    for seed in seeds:
        seed = " ".join(unicodedata.normalize("NFKC", seed).split())
        if normalized(seed) in seed_seen:
            continue
        seed_seen.add(normalized(seed))
        families.append((seed, seed, "subject"))
        for category in categories:
            for template in MARKET_QUERIES[category]:
                # An already-specific custom seed stays intact.
                families.append((template.format(seed=seed), seed, category))
    plan = []
    seen = set()
    max_aliases = max(len(s[3]) for s in scopes)
    for alias_index in range(max_aliases):
        for text, seed, category in families:
            for geo_key, geo_name, kind, terms in scopes:
                if alias_index >= len(terms):
                    continue
                phrase = (text + " " + terms[alias_index]).strip()
                key = normalized(phrase)
                if key in seen:
                    continue
                seen.add(key)
                plan.append(
                    QuerySpec(
                        phrase, seed, geo_key, geo_name, len(plan), kind, category
                    )
                )
    return plan


def rejection_reasons(post, spec, *, published_after=None, published_before=None):
    value = post.get("id")
    if (
        type(value) not in (str, int)
        or not str(value).isascii()
        or not str(value).isdecimal()
        or int(value) <= 0
    ):
        return ["invalid_source_id"]
    if published_after is not None or published_before is not None:
        value = post.get("taken_at")
        if type(value) not in (int, float):
            return ["missing_publication_time"]
        try:
            when = datetime.fromtimestamp(value, UTC)
        except (ValueError, OverflowError, OSError):
            return ["invalid_publication_time"]
        if (
            published_after is not None
            and when < published_after
            or published_before is not None
            and when >= published_before
        ):
            return ["outside_time_window"]
    assessment = assess_post(post, query_spec=spec)
    reasons = []
    if not any(s.startswith("subject:") for s in assessment.signals):
        reasons.append("missing_pickleball_subject")
    if not assessment.categories:
        reasons.append("missing_market_topic")
    if not assessment.locations and assessment.language_hint != "vi":
        reasons.append("missing_vietnam_evidence")
    return reasons


def utc_bound(value):
    if value is None:
        return None
    when = datetime.fromisoformat(value)
    if when.utcoffset() is None:
        raise ValueError("publication bounds must include timezone")
    return when.astimezone(UTC)


def window_bounds(after, before, days, now):
    lower, upper = utc_bound(after), utc_bound(before)
    if days is not None:
        if type(days) is not int or days <= 0 or lower is not None:
            raise ValueError(
                "lookback_days must be positive and cannot combine with published_after"
            )
        upper = upper or now
        lower = upper - timedelta(days=days)
    if lower is not None and upper is not None and lower >= upper:
        raise ValueError("publication window must be increasing")
    return lower, upper


class BudgetExhausted(RuntimeError):
    pass


@dataclass
class RequestBudget:
    maximum: int
    duration: float | None
    used: int = 0
    started: float = 0

    def __post_init__(self):
        self.started = time.monotonic()

    def check(self):
        if (
            self.duration is not None
            and time.monotonic() - self.started >= self.duration
        ):
            raise BudgetExhausted("duration_budget")
        if self.used >= self.maximum:
            raise BudgetExhausted("request_budget")

    def consume(self):
        self.check()
        self.used += 1


class PublicPage:
    """Count all goto/reload calls, including retries; never manufacture cursors."""

    def __init__(self, page, budget):
        self.page, self.budget = page, budget

    def __getattr__(self, name):
        return getattr(self.page, name)

    def goto(self, *args, **kwargs):
        self.budget.consume()
        return self.page.goto(*args, **kwargs)

    def reload(self, *args, **kwargs):
        self.budget.consume()
        return self.page.reload(*args, **kwargs)


def public_next_url(page, query):
    """Only follow a real public next-page anchor on the same query/origin."""
    href = page.evaluate("""() => {
        const a=document.querySelector('a[rel="next"]');
        return a ? a.getAttribute('href') : null;
    }""")
    if not isinstance(href, str) or not href:
        return None
    target = urljoin(page.url, href)
    parts = urlsplit(target)
    if (
        parts.scheme != "https"
        or parts.hostname not in ("threads.com", "www.threads.com")
        or parts.username
        or parts.password
        or parts.path.rstrip("/") != "/search"
    ):
        return None
    if normalized(parse_qs(parts.query).get("q", [""])[0]) != normalized(query):
        return None
    return target


def save_checkpoint(path, state):
    check_secrets(state)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix="." + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, allow_nan=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)


def acknowledge_checkpoint(path, run_id):
    """Advance a survey only after the caller has committed its source artifact."""
    import fcntl

    path = Path(path)
    if not path.exists():
        return
    with path.with_suffix(path.suffix + ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = json.loads(path.read_text())
        if state["run_id"] != run_id or not state.get("exported"):
            raise ValueError("checkpoint acknowledgement conflicts with collection run")
        state["artifact_committed"] = True
        save_checkpoint(path, state)
