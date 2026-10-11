"""Bounded public discovery planning, query evidence and durable checkpoints."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
import unicodedata
from dataclasses import dataclass
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlsplit

from ....runtime.bronze import check_secrets
from .vietnam_terms import (
    COUNTRY_SCOPE,
    PICKLEBALL_TERMS,
    QuerySpec,
    assess_post,
)

PLANNER_VERSION = "3"
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


def reference_resource(name):
    root = next(
        (
            p
            for p in Path(__file__).resolve().parents
            if (p / "configs/reference" / name).is_file()
        ),
        None,
    )
    path = (
        root / "configs/reference" / name
        if root
        else files("dsp_contracts") / "reference" / name
    )
    return json.loads(path.read_text())


@lru_cache(maxsize=1)
def semantic_vocabulary():
    return reference_resource("threads_vocabulary.v1.json")["categories"]


@lru_cache(maxsize=1)
def national_geography():
    rows = reference_resource("threads_geography.v1.json")["locations"]
    for row in rows:
        if not row.get("source_url") or not row.get("geo_id"):
            raise ValueError("Geographic reference requires identity and provenance")
    return rows


def ascii_variant(value):
    value = value.replace("đ", "d").replace("Đ", "D")
    return "".join(
        c for c in unicodedata.normalize("NFD", value) if not unicodedata.combining(c)
    )


class DiscoveryPlan(list):
    """A normal list with composition counts for discovery diagnostics."""

    def __init__(self, items, composed):
        super().__init__(items)
        self.candidates_before_phrase_dedup = composed


def build_discovery_plan(
    seeds,
    *,
    categories=None,
    geo_keys=None,
    include_local=False,
    max_geo_aliases=None,
    expand_subject_variants=True,
):
    """Compose traceable phrases, deduplicate strings while merging provenance.

    max_geo_aliases is accepted for old callers but never discards validated
    aliases. Query budgets apply to execution, never candidate generation.
    Diagonal scheduling rotates geography for each semantic family.
    """
    from collections import deque
    from dataclasses import replace

    if (
        not isinstance(seeds, list)
        or not seeds
        or any(not isinstance(s, str) or not s.strip() for s in seeds)
    ):
        raise ValueError("query_seeds must be nonempty strings")
    vocab = semantic_vocabulary()
    categories = list(vocab) if categories is None else categories
    if not isinstance(categories, list) or set(categories) - set(vocab):
        raise ValueError("unknown discovery category")
    rows = national_geography()
    if geo_keys is not None:
        known = {r["geo_id"] for r in rows}
        legacy = {
            r["location_id"]: r["canonical_name"].replace("TP. ", "")
            for r in geography_registry()
        }
        if not isinstance(geo_keys, list) or set(geo_keys) - known - set(legacy):
            raise ValueError("geo_keys must refer to the existing geographic registry")
        names = {legacy[k] for k in geo_keys if k in legacy}
        rows = [
            r for r in rows if r["geo_id"] in geo_keys or r["canonical_name"] in names
        ]
    rows = [
        r
        for r in rows
        if include_local or r["level"] == "province" or geo_keys is not None
    ]
    scopes = [
        (None, None, "fallback", [""], ()),
        ("vietnam", "Vietnam", "country", list(COUNTRY_SCOPE.search_terms), ()),
    ]
    # Merge equivalent historical/current names at the search boundary only.
    by_name = {}
    for row in rows:
        key = normalized(row["canonical_name"])
        group = by_name.setdefault(key, {"row": row, "aliases": [], "provenance": []})
        group["provenance"].append(
            {
                k: row[k]
                for k in (
                    "geo_id",
                    "canonical_name",
                    "admin_version",
                    "level",
                    "source_url",
                )
            }
        )
        preferred = row.get("preferred_search_aliases", [])
        for term in [*preferred, row["canonical_name"], *row["aliases"]]:
            for spelling in [term, ascii_variant(term)]:
                if normalized(spelling) not in {
                    normalized(t) for t in group["aliases"]
                }:
                    group["aliases"].append(spelling)
    for key, group in sorted(by_name.items()):
        row = group["row"]
        scopes.append(
            (
                row["geo_id"],
                row["canonical_name"],
                "city",
                group["aliases"],
                tuple(group["provenance"]),
            )
        )
    subjects = list(seeds)
    if expand_subject_variants and any(normalized(s) == "pickleball" for s in seeds):
        subjects += list(PICKLEBALL_TERMS)
    subjects = list(dict.fromkeys(normalized(s) for s in subjects))
    families = {"subject": [("{seed}", "subject")]}
    for category in categories:
        families[category] = []
        for concept in vocab[category]:
            for template in concept["templates"]:
                for phrase in (
                    [template, ascii_variant(template)]
                    if concept.get("ascii_variant")
                    else [template]
                ):
                    if phrase not in [p for p, _ in families[category]]:
                        families[category].append((phrase, concept["concept"]))
    family_keys = list(families)
    candidates = {}
    composed = 0
    n = len(scopes)
    for variant in range(max(len(v) for v in families.values())):
        for geo_step in range(n):
            for category_index, category in enumerate(family_keys):
                if variant >= len(families[category]):
                    continue
                template, concept = families[category][variant]
                scope = scopes[
                    (geo_step + category_index * max(1, n // len(family_keys))) % n
                ]
                geo_key, geo_name, kind, terms, provenance = scope
                for seed in subjects:
                    for term in terms:
                        composed += 1
                        phrase = (template.format(seed=seed) + " " + term).strip()
                        key = normalized(phrase)
                        if key in candidates:
                            old = candidates[key]
                            merged = list(old.geo_provenance)
                            merged.extend(p for p in provenance if p not in merged)
                            semantics = tuple(
                                dict.fromkeys((*old.semantic_provenance, category))
                            )
                            candidates[key] = replace(
                                old,
                                geo_provenance=tuple(merged),
                                semantic_provenance=semantics,
                            )
                        else:
                            candidates[key] = QuerySpec(
                                phrase,
                                seed,
                                geo_key,
                                geo_name,
                                len(candidates),
                                kind,
                                category,
                                provenance,
                                (category,),
                            )
    # Alias variants are fairly mixed by round across geography/category queues.
    buckets = {}
    for spec in candidates.values():
        key = (spec.geo_name, spec.query_category)
        buckets.setdefault(key, deque()).append(spec)
    plan = []
    while buckets:
        for key in list(buckets):
            plan.append(replace(buckets[key].popleft(), priority=len(plan)))
            if not buckets[key]:
                del buckets[key]
    return DiscoveryPlan(plan, composed)


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
    assessment = assess_post(post, query_spec=spec, expanded_topics=True)
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
    on_consume: Callable[[], None] | None = None

    def __post_init__(self):
        self.started = time.monotonic()

    def check(self):
        self.check_duration()
        if self.used >= self.maximum:
            raise BudgetExhausted("request_budget")

    def check_duration(self):
        if (
            self.duration is not None
            and time.monotonic() - self.started >= self.duration
        ):
            raise BudgetExhausted("duration_budget")

    def remaining_ms(self):
        self.check_duration()
        return (
            None
            if self.duration is None
            else max(1, int((self.duration - (time.monotonic() - self.started)) * 1000))
        )

    def consume(self):
        self.check()
        self.used += 1
        if self.on_consume is not None:
            self.on_consume()


class PublicPage:
    """Count all goto/reload calls, including retries; never manufacture cursors."""

    def __init__(self, page, budget):
        self.page, self.budget = page, budget

    def __getattr__(self, name):
        return getattr(self.page, name)

    def goto(self, *args, **kwargs):
        self.budget.consume()
        remaining = self.budget.remaining_ms()
        if remaining is not None:
            kwargs["timeout"] = min(kwargs.get("timeout", remaining), remaining)
        try:
            return self.page.goto(*args, **kwargs)
        except Exception:
            self.budget.check_duration()
            raise

    def reload(self, *args, **kwargs):
        self.budget.consume()
        remaining = self.budget.remaining_ms()
        if remaining is not None:
            kwargs["timeout"] = min(kwargs.get("timeout", remaining), remaining)
        try:
            return self.page.reload(*args, **kwargs)
        except Exception:
            self.budget.check_duration()
            raise

    def wait_for_timeout(self, milliseconds):
        remaining = self.budget.remaining_ms()
        return self.page.wait_for_timeout(
            milliseconds if remaining is None else min(milliseconds, remaining)
        )


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
    try:
        port = parts.port
    except ValueError:
        return None
    if (
        parts.scheme != "https"
        or port not in (None, 443)
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
