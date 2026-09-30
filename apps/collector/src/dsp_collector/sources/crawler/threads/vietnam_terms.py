"""Vietnam scope terms and relevance helpers for Threads collection.

This module contains no browser or Threads-specific crawling logic.
It only defines:
- Vietnam-oriented search query generation
- training/court/demand vocabulary
- Vietnam/language relevance heuristics
"""

from __future__ import annotations

from typing import Any

VIETNAM_LOCATION_TERMS = (
    "việt nam",
    "viet nam",
    "vietnam",
    "hà nội",
    "ha noi",
    "hanoi",
    "hồ chí minh",
    "ho chi minh",
    "tp hcm",
    "tphcm",
    "sài gòn",
    "sai gon",
    "saigon",
    "đà nẵng",
    "da nang",
    "hải phòng",
    "hai phong",
    "cần thơ",
    "can tho",
    "huế",
    "hue",
    "nha trang",
    "vũng tàu",
    "vung tau",
    "bình dương",
    "binh duong",
    "đồng nai",
    "dong nai",
    "cầu giấy",
    "cau giay",
    "bắc từ liêm",
    "bac tu liem",
    "nam từ liêm",
    "nam tu liem",
    "thủ đức",
    "thu duc",
)

VIETNAMESE_LANGUAGE_TERMS = (
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

CATEGORY_TERMS: dict[str, tuple[str, ...]] = {
    "training": (
        "học",
        "lớp",
        "dạy",
        "huấn luyện",
        "hlv",
        "giáo viên",
        "thầy dạy",
        "cô dạy",
        "người mới",
        "beginner",
        "coach",
        "coaching",
        "lesson",
        "lessons",
        "training",
        "practice",
        "tập luyện",
        "tập pickleball",
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
        "looking for",
        "need a",
        "need an",
        "anyone know",
        "recommend",
        "recommendation",
        "available",
    ),
}

# Search templates intentionally bias collection toward:
# - Vietnamese-language demand
# - explicit Vietnam locations
# - foreign-language content that explicitly mentions Vietnam
SCOPE_QUERY_TEMPLATES: tuple[tuple[str, str], ...] = (
    ("sân {seed}", "court"),
    ("tìm sân {seed}", "demand"),
    ("thuê sân {seed}", "court"),
    ("đặt sân {seed}", "court"),
    ("học {seed}", "training"),
    ("lớp {seed}", "training"),
    ("dạy {seed}", "training"),
    ("HLV {seed}", "training"),
    ("{seed} Hà Nội", "demand"),
    ("{seed} TP HCM", "demand"),
    ("{seed} Sài Gòn", "demand"),
    ("{seed} Đà Nẵng", "demand"),
    ("{seed} court Vietnam", "court"),
    ("{seed} training Vietnam", "training"),
    ("{seed} coach Vietnam", "training"),
    ("{seed} lessons Vietnam", "training"),
    ("{seed} court Hanoi", "court"),
    ("{seed} court Ho Chi Minh", "court"),
    ("{seed} court Da Nang", "court"),
)


def build_scope_queries(seed: str) -> list[dict[str, str]]:
    """Expand a seed such as ``pickleball`` into Vietnam-focused queries."""
    seed = seed.strip()
    if not seed:
        raise ValueError("seed query must not be empty")

    return [
        {
            "query": template.format(seed=seed),
            "intent": intent,
        }
        for template, intent in SCOPE_QUERY_TEMPLATES
    ]


def _contains_any(text: str, terms: tuple[str, ...]) -> list[str]:
    normalized = text.casefold()
    return [term for term in terms if term.casefold() in normalized]


def _looks_vietnamese(text: str) -> tuple[bool, list[str]]:
    """Detect useful Vietnamese-language signals without an ML dependency."""
    if not text:
        return False, []

    signals: list[str] = []

    if any(char in VIETNAMESE_SPECIFIC_CHARS for char in text.casefold()):
        signals.append("vietnamese_orthography")

    lexical_hits = _contains_any(text, VIETNAMESE_LANGUAGE_TERMS)
    signals.extend(f"vi_phrase:{term}" for term in lexical_hits[:3])

    return bool(signals), signals


def _classify_scope(text: str) -> tuple[list[str], list[str]]:
    """Classify text into the research topics: training/court/demand."""
    categories: list[str] = []
    signals: list[str] = []

    for category, terms in CATEGORY_TERMS.items():
        hits = _contains_any(text, terms)
        if not hits:
            continue
        categories.append(category)
        signals.extend(f"{category}:{term}" for term in hits[:3])

    return categories, signals


def _query_has_explicit_vietnam_location(query: str) -> bool:
    return bool(_contains_any(query, VIETNAM_LOCATION_TERMS))


def _query_is_vietnamese_collection_query(query: str) -> bool:
    normalized = query.casefold()
    prefixes = (
        "sân ",
        "tìm ",
        "thuê ",
        "đặt ",
        "học ",
        "lớp ",
        "dạy ",
        "hlv ",
    )
    return normalized.startswith(prefixes)


def evaluate_relevance(
    post: dict[str, Any],
    *,
    matched_query: str,
) -> dict[str, Any]:
    """Evaluate whether a raw post fits the Vietnam research scope.

    A post is accepted only when it:
    1. relates to training, courts, or demand; and
    2. has a Vietnam signal from its text/location or from an explicit
       Vietnam/location search query.

    Vietnamese search wording alone is kept as provenance but is not enough to
    accept unrelated foreign content.
    """
    text = (post.get("text") or "").strip()

    categories, category_signals = _classify_scope(text)
    location_hits = _contains_any(text, VIETNAM_LOCATION_TERMS)
    vietnamese_like, language_signals = _looks_vietnamese(text)
    explicit_vietnam_query = _query_has_explicit_vietnam_location(matched_query)
    vietnamese_query = _query_is_vietnamese_collection_query(matched_query)

    vietnam_signals: list[str] = []
    vietnam_signals.extend(f"location:{term}" for term in location_hits[:4])
    vietnam_signals.extend(language_signals)

    if explicit_vietnam_query:
        vietnam_signals.append("explicit_vietnam_query")
    elif vietnamese_query:
        vietnam_signals.append("vietnamese_search_query")

    has_scope_intent = bool(categories)
    has_vietnam_signal = (
        bool(location_hits) or vietnamese_like or explicit_vietnam_query
    )

    return {
        "is_relevant": has_scope_intent and has_vietnam_signal,
        "categories": categories,
        "scope_signals": category_signals,
        "vietnam_signals": vietnam_signals,
        "language_hint": "vi" if vietnamese_like else "unknown_or_non_vi",
    }
