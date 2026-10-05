"""Vietnam-oriented collection policy for pickleball social posts.

This module owns domain knowledge only:
- geographic vocabulary
- topic vocabulary
- query-plan generation
- relevance scoring
- post annotation/merge rules

It has no Playwright, HTML, Threads SSR, or filesystem logic.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class GeoScope:
    """One canonical Vietnam geography with searchable aliases."""

    key: str
    name: str
    aliases: tuple[str, ...]
    search_terms: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class QuerySpec:
    """A domain query that the crawler can execute without knowing its meaning."""

    query: str
    seed: str
    geo_key: str | None
    geo_name: str | None
    priority: int
    kind: str  # country | city | fallback


@dataclass(frozen=True, slots=True)
class RelevanceAssessment:
    """Structured evaluation result for one raw social post."""

    is_relevant: bool
    categories: tuple[str, ...]
    locations: tuple[str, ...]
    language_hint: str
    vietnam_score: int
    topic_score: int
    signals: tuple[str, ...]


COUNTRY_SCOPE = GeoScope(
    key="vietnam",
    name="Vietnam",
    aliases=("việt nam", "viet nam", "vietnam"),
    search_terms=("Việt Nam", "Vietnam"),
)

CITY_SCOPES: tuple[GeoScope, ...] = (
    GeoScope(
        key="hanoi",
        name="Hà Nội",
        aliases=("hà nội", "ha noi", "hanoi"),
        search_terms=("Hà Nội", "Hanoi"),
    ),
    GeoScope(
        key="ho_chi_minh_city",
        name="TP. Hồ Chí Minh",
        aliases=(
            "hồ chí minh",
            "ho chi minh",
            "tp hcm",
            "tphcm",
            "hcm",
            "hcmc",
            "sài gòn",
            "sai gon",
            "saigon",
        ),
        search_terms=("TP HCM", "Sài Gòn", "Ho Chi Minh"),
    ),
    GeoScope(
        key="da_nang",
        name="Đà Nẵng",
        aliases=("đà nẵng", "da nang"),
        search_terms=("Đà Nẵng", "Da Nang"),
    ),
    GeoScope(
        key="hai_phong",
        name="Hải Phòng",
        aliases=("hải phòng", "hai phong"),
        search_terms=("Hải Phòng",),
    ),
    GeoScope(
        key="can_tho",
        name="Cần Thơ",
        aliases=("cần thơ", "can tho"),
        search_terms=("Cần Thơ",),
    ),
    GeoScope(
        key="hue",
        name="Huế",
        aliases=("huế", "hue"),
        search_terms=("Huế",),
    ),
    GeoScope(
        key="nha_trang",
        name="Nha Trang",
        aliases=("nha trang",),
        search_terms=("Nha Trang",),
    ),
    GeoScope(
        key="vung_tau",
        name="Vũng Tàu",
        aliases=("vũng tàu", "vung tau"),
        search_terms=("Vũng Tàu",),
    ),
    GeoScope(
        key="binh_duong",
        name="Bình Dương",
        aliases=("bình dương", "binh duong"),
        search_terms=("Bình Dương",),
    ),
    GeoScope(
        key="dong_nai",
        name="Đồng Nai",
        aliases=("đồng nai", "dong nai"),
        search_terms=("Đồng Nai",),
    ),
)

# Sub-city names are useful as text evidence but not necessarily worth one
# search request each.
LOCAL_AREA_ALIASES: dict[str, tuple[str, ...]] = {
    "Cầu Giấy": ("cầu giấy", "cau giay"),
    "Bắc Từ Liêm": ("bắc từ liêm", "bac tu liem"),
    "Nam Từ Liêm": ("nam từ liêm", "nam tu liem"),
    "Thủ Đức": ("thủ đức", "thu duc"),
}

TOPIC_TERMS: dict[str, tuple[str, ...]] = {
    # Strong signals that are meaningful on their own once the post is known
    # to be about pickleball. Ambiguous words such as plain "học", "thầy",
    # "beginner", "looking for", and "available" are handled below with
    # contextual co-occurrence rules instead of raw keyword matching.
    "training": (
        "lớp",
        "dạy",
        "huấn luyện",
        "hlv",
        "giáo viên",
        "thầy dạy",
        "cô dạy",
        "coach",
        "coaching",
        "lesson",
        "lessons",
        "training",
        "practice",
        "tập luyện",
        "tập chơi",
        "học thầy",
        "học đánh",
        "bắt đầu tập",
        "newbie",
        "người mới",
    ),
    "court": (
        "sân",
        "sân bãi",
        "địa điểm",
        "thuê sân",
        "đặt sân",
        "court",
        "courts",
        "venue",
        "facility",
        "book court",
        "booking",
    ),
    "demand": (
        "cần tìm",
        "tìm sân",
        "tìm lớp",
        "tìm người",
        "ở đâu",
        "ai biết",
        "có ai",
        "ghép",
        "slot",
        "chơi cùng",
        "đánh cùng",
        "anyone know",
        "recommend",
        "recommendation",
    ),
}


# Each rule is a set of phrases that must all occur in the same post. This
# prevents generic commercial wording such as "looking for your next paddle"
# or "available now" from being mistaken for court/training demand.
CONTEXTUAL_TOPIC_RULES: dict[str, tuple[tuple[str, ...], ...]] = {
    "training": (
        ("beginner", "class"),
        ("beginner", "lesson"),
        ("beginner", "coach"),
        ("beginner", "training"),
    ),
    "demand": (
        ("looking for", "court"),
        ("looking for", "courts"),
        ("looking for", "coach"),
        ("looking for", "class"),
        ("looking for", "lesson"),
        ("looking for", "players"),
        ("looking for", "team"),
        ("looking for", "partner"),
        ("need a", "court"),
        ("need a", "coach"),
        ("need a", "class"),
        ("need a", "team"),
        ("available", "court"),
        ("available", "slot"),
        ("availability", "court"),
        ("availability", "booking"),
    ),
}


PICKLEBALL_TERMS = (
    "pickleball",
    "pickle ball",
    "picketball",
)


VIETNAMESE_PHRASES = (
    "mọi người",
    "mn biết",
    "cho mình",
    "cho em",
    "mình cần",
    "em cần",
    "cần tìm",
    "tìm lớp",
    "tìm sân",
    "khu vực",
    "ở đâu",
    "có ai",
    "ai biết",
    "người mới",
    "chưa biết",
    "liên hệ",
    "giáo viên",
    "thầy dạy",
    "cô dạy",
    "thuê sân",
    "đặt sân",
    "ghép lớp",
    "ghép sân",
    "chơi cùng",
    "đánh cùng",
)

VIETNAMESE_SPECIFIC_CHARS = set(
    "ăâđêôơư"
    "áàảãạắằẳẵặấầẩẫậ"
    "éèẻẽẹếềểễệ"
    "íìỉĩị"
    "óòỏõọốồổỗộớờởỡợ"
    "úùủũụứừửữự"
    "ýỳỷỹỵ"
)


def _normalize(text: str) -> str:
    return " ".join(text.casefold().split())


def _ascii_fold(text: str) -> str:
    normalized = unicodedata.normalize("NFD", _normalize(text))
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def _tokens(text: str, *, fold_ascii: bool = False) -> tuple[str, ...]:
    value = _ascii_fold(text) if fold_ascii else _normalize(text)
    # ``[^\W_]`` means Unicode alphanumeric without underscore. Token-based
    # phrase matching prevents aliases such as "can tho" from matching the
    # beginning of "can thong tin", or "hue" from matching "thue".
    return tuple(re.findall(r"[^\W_]+", value, flags=re.UNICODE))


def _contains_token_phrase(
    text_tokens: tuple[str, ...],
    term_tokens: tuple[str, ...],
) -> bool:
    if not term_tokens or len(term_tokens) > len(text_tokens):
        return False

    width = len(term_tokens)
    return any(
        text_tokens[index : index + width] == term_tokens
        for index in range(len(text_tokens) - width + 1)
    )


def _term_matches(text: str, term: str) -> bool:
    """Match complete token phrases without collapsing Vietnamese meanings.

    Accent folding is used only for terms that are already ASCII. Folding an
    accented Vietnamese term such as ``sân`` into ``san`` would incorrectly
    match a different word such as ``săn``. Geographic aliases that need
    accentless support explicitly include ASCII variants (for example
    ``hà nội`` and ``ha noi``).
    """
    if not text or not term:
        return False

    if _contains_token_phrase(_tokens(text), _tokens(term)):
        return True

    normalized_term = _normalize(term)
    if normalized_term != _ascii_fold(normalized_term):
        return False

    return _contains_token_phrase(
        _tokens(text, fold_ascii=True),
        _tokens(term, fold_ascii=True),
    )


def _hits(text: str, terms: Iterable[str]) -> list[str]:
    return [term for term in terms if _term_matches(text, term)]


def build_query_plan(
    seed: str,
    *,
    include_country: bool = True,
    include_cities: bool = True,
    include_fallback: bool = True,
) -> list[QuerySpec]:
    """Generate a broad geo-oriented plan instead of hardcoded intent queries.

    Search is optimized for recall:
        <seed> + <Vietnam geography>

    Training/court/demand classification is done after collection.
    """
    seed = seed.strip()
    if not seed:
        raise ValueError("seed query must not be empty")

    plan: list[QuerySpec] = []
    seen: set[str] = set()

    def add(
        query: str,
        *,
        geo: GeoScope | None,
        priority: int,
        kind: str,
    ) -> None:
        key = _normalize(query)
        if key in seen:
            return
        seen.add(key)
        plan.append(
            QuerySpec(
                query=query,
                seed=seed,
                geo_key=geo.key if geo else None,
                geo_name=geo.name if geo else None,
                priority=priority,
                kind=kind,
            )
        )

    if include_country:
        for term in COUNTRY_SCOPE.search_terms:
            add(
                f"{seed} {term}",
                geo=COUNTRY_SCOPE,
                priority=10,
                kind="country",
            )

    if include_cities:
        priority = 20
        for geo in CITY_SCOPES:
            for term in geo.search_terms:
                add(
                    f"{seed} {term}",
                    geo=geo,
                    priority=priority,
                    kind="city",
                )
            priority += 1

    if include_fallback:
        add(
            seed,
            geo=None,
            priority=1000,
            kind="fallback",
        )

    return sorted(plan, key=lambda item: item.priority)


def _detect_locations(text: str) -> list[str]:
    locations: list[str] = []

    for geo in (COUNTRY_SCOPE, *CITY_SCOPES):
        if _hits(text, geo.aliases):
            locations.append(geo.name)

    for name, aliases in LOCAL_AREA_ALIASES.items():
        if _hits(text, aliases):
            locations.append(name)

    return list(dict.fromkeys(locations))


def _detect_topics(text: str) -> tuple[list[str], list[str]]:
    categories: list[str] = []
    signals: list[str] = []

    for category, terms in TOPIC_TERMS.items():
        matched = _hits(text, terms)
        if matched:
            categories.append(category)
            signals.extend(f"topic:{category}:{term}" for term in matched[:3])

    for category, rules in CONTEXTUAL_TOPIC_RULES.items():
        for required_terms in rules:
            if not all(_term_matches(text, term) for term in required_terms):
                continue

            if category not in categories:
                categories.append(category)

            label = "+".join(required_terms)
            signals.append(f"topic:{category}:context:{label}")
            break

    return categories, signals


def _detect_vietnamese(text: str) -> tuple[bool, list[str]]:
    if not text:
        return False, []

    signals: list[str] = []

    if any(ch in VIETNAMESE_SPECIFIC_CHARS for ch in text.casefold()):
        signals.append("language:vietnamese_orthography")

    phrase_hits = _hits(text, VIETNAMESE_PHRASES)
    signals.extend(f"language:vi_phrase:{term}" for term in phrase_hits[:3])

    return bool(signals), signals


def assess_post(
    post: dict[str, Any],
    *,
    query_spec: QuerySpec,
) -> RelevanceAssessment:
    """Score one raw post against the Vietnam pickleball research scope.

    Search-query geography is treated as weak context.
    Geography/language found in the post body is stronger evidence.

    The post must still contain a training/court/demand signal; generic
    pickleball equipment/news posts are therefore rejected.
    """
    text = str(post.get("text") or "").strip()

    categories, topic_signals = _detect_topics(text)
    locations = _detect_locations(text)
    vietnamese_like, language_signals = _detect_vietnamese(text)
    subject_hits = _hits(text, PICKLEBALL_TERMS)
    subject_present = bool(subject_hits)

    topic_score = 2 * len(categories)

    vietnam_score = 0
    signals: list[str] = [*topic_signals, *language_signals]

    if locations:
        vietnam_score += 4
        signals.extend(f"geo:text:{location}" for location in locations)

    if vietnamese_like:
        vietnam_score += 3

    if query_spec.geo_key is not None:
        # Weak evidence only: Threads may occasionally return noisy results.
        vietnam_score += 1
        signals.append(f"geo:query:{query_spec.geo_name}")

    if subject_present:
        topic_score += 1
        signals.extend(f"subject:{term}" for term in subject_hits[:2])

    # Acceptance policy:
    # - the body itself must be about pickleball (query context cannot rescue a
    #   tennis/other-sport result)
    # - at least one research topic must be present
    # - Vietnam evidence must come from the body/language. Query geography is
    #   retained only as collection metadata because Threads search can be noisy.
    strong_vietnam_evidence = bool(locations) or vietnamese_like

    is_relevant = subject_present and bool(categories) and strong_vietnam_evidence

    language_hint = "vi" if vietnamese_like else "unknown_or_non_vi"

    return RelevanceAssessment(
        is_relevant=is_relevant,
        categories=tuple(categories),
        locations=tuple(locations),
        language_hint=language_hint,
        vietnam_score=vietnam_score,
        topic_score=topic_score,
        signals=tuple(dict.fromkeys(signals)),
    )


def adapt_post(
    post: dict[str, Any],
    *,
    query_spec: QuerySpec,
) -> dict[str, Any] | None:
    """Return an annotated post when relevant, otherwise ``None``."""
    assessment = assess_post(post, query_spec=query_spec)

    if not assessment.is_relevant:
        return None

    return {
        **post,
        "matched_queries": [query_spec.query],
        "query_contexts": [
            {
                "query": query_spec.query,
                "kind": query_spec.kind,
                "geo_key": query_spec.geo_key,
                "geo_name": query_spec.geo_name,
            }
        ],
        "categories": list(assessment.categories),
        "detected_locations": list(assessment.locations),
        "relevance": {
            "vietnam_score": assessment.vietnam_score,
            "topic_score": assessment.topic_score,
            "language_hint": assessment.language_hint,
            "signals": list(assessment.signals),
        },
    }


def merge_adapted_post(
    existing: dict[str, Any],
    incoming: dict[str, Any],
) -> dict[str, Any]:
    """Merge duplicate observations of the same post across search queries."""
    for query in incoming.get("matched_queries") or []:
        if query not in existing["matched_queries"]:
            existing["matched_queries"].append(query)

    existing_contexts = {
        (
            item.get("query"),
            item.get("geo_key"),
        )
        for item in existing.get("query_contexts") or []
    }

    for context in incoming.get("query_contexts") or []:
        key = (context.get("query"), context.get("geo_key"))
        if key not in existing_contexts:
            existing["query_contexts"].append(context)
            existing_contexts.add(key)

    for field in ("categories", "detected_locations"):
        for value in incoming.get(field) or []:
            if value not in existing[field]:
                existing[field].append(value)

    existing_rel = existing["relevance"]
    incoming_rel = incoming["relevance"]

    existing_rel["vietnam_score"] = max(
        existing_rel["vietnam_score"],
        incoming_rel["vietnam_score"],
    )
    existing_rel["topic_score"] = max(
        existing_rel["topic_score"],
        incoming_rel["topic_score"],
    )

    if incoming_rel["language_hint"] == "vi":
        existing_rel["language_hint"] = "vi"

    for signal in incoming_rel.get("signals") or []:
        if signal not in existing_rel["signals"]:
            existing_rel["signals"].append(signal)

    return existing
