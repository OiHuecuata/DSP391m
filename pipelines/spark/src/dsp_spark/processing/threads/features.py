"""Own-text exploratory features with spans and explicitly unevaluated labels."""

import re
import unicodedata
from datetime import datetime
from zoneinfo import ZoneInfo

from dsp_collector.normalizers.observation import digest, parse_json
from dsp_collector.sources.crawler.threads.vietnam_terms import CITY_SCOPES

METHOD = "threads-own-text-rules-v1"
INTENTS = {
    "SEEK_COURT": r"tìm sân|sân nào|sân.*trống",
    "BOOKING_INTENT": r"đặt sân|đặt lịch|thuê sân",
    "SEEK_PLAYERS": r"ghép kèo|tìm.*(?:bạn|người chơi)|cần thêm.*người",
    "SEEK_COACH": r"huấn luyện|tìm.*(?:thầy|lớp)|lớp cơ bản",
    "PRICE_INQUIRY": r"bao nhiêu|hỏi giá|giá thuê|giá.*giờ",
    "CAPACITY_COMPLAINT": r"hết chỗ|sân.*kín|không.*sân trống",
    "VENUE_FEEDBACK": r"mặt sân|đèn tối|sân trơn|tiện ích|dịch vụ",
    "SUPPLY_ADVERTISEMENT": r"nhận đặt lịch|liên hệ đặt sân|ưu đãi thuê",
    "SUPPLY_OPENING": r"khai trương|mở thêm.*sân",
    "SUPPLY_CLOSURE": r"ngừng kinh doanh|đóng cửa sân",
    "EVENT_TOURNAMENT": r"giải đấu|giải giao hữu|tournament",
    "GENERAL_DISCUSSION": r"pickleball",
    "IRRELEVANT": r"(?!)",
}


def content(row):
    text = unicodedata.normalize("NFKC", row.get("text_raw") or "")
    text = re.sub(r"\s+", " ", text).strip()
    related = "pickleball" in text.casefold()
    labels = [
        label
        for label, pattern in INTENTS.items()
        if re.search(pattern, text, re.IGNORECASE)
    ]
    if not related and not labels:
        labels = ["IRRELEVANT"]
    published = (
        datetime.fromisoformat(row["published_at"]).astimezone(
            ZoneInfo("Asia/Ho_Chi_Minh")
        )
        if row["published_at"]
        else None
    )
    raw = parse_json(row["raw_payload"])
    kind = row["record_type"]
    return {
        "content_id": row["native_record_key"],
        "platform": "threads",
        "content_type": kind,
        "post_id": (
            "threads:" + row["source_record_id"]
            if kind == "Post"
            else "threads:" + row["native_post_id"]
            if row["native_post_id"]
            else None
        ),
        "comment_id": (
            "threads:" + row["source_record_id"] if kind == "Comment" else None
        ),
        "parent_comment_id": (
            "threads:" + row["parent_comment_id"] if row["parent_comment_id"] else None
        ),
        "parent_content_id": row["parent_content_id"],
        "is_reply": bool(row["is_reply"]),
        "text_normalized": text,
        "comment_text_normalized": text if kind == "Comment" else None,
        "published_at": row["published_at"],
        "published_date_vn": published.date().isoformat() if published else None,
        "publication_hour_vn": published.hour if published else None,
        "language": row["language_raw"],
        "is_pickleball_related": related,
        "is_commercial": "SUPPLY_ADVERTISEMENT" in labels,
        "intent_labels": labels,
        "topic_labels": [
            label.lower()
            for label in labels
            if label not in ("GENERAL_DISCUSSION", "IRRELEVANT")
        ],
        "sentiment": None,
        "relevance_confidence": None,
        "classification_confidence": None,
        "classification_status": "EXPLORATORY_UNEVALUATED",
        "classifier_version": METHOD,
        "is_duplicate": False,
        "text_hash": digest(text),
        "hashtags": re.findall(r"#[\w]+", text),
        "character_count": len(text),
        "word_count": len(text.split()),
        "mentioned_venue_ids": [],
        "reply_extraction_status": (raw.get("detail") or {}).get(
            "reply_extraction_status", "UNKNOWN"
        ),
        "source_url": row["source_url"],
        "observation_id": row["observation_id"],
        "collection_run_id": row["collection_run_id"],
        "query_id": row["query_id"],
        "fetch_batch_id": row["fetch_batch_id"],
        "observed_at": row["observed_at"],
        "artifact_sha256": row["artifact_sha256"],
        "origin": row["origin"],
    }


def snapshot(row):
    observed = datetime.fromisoformat(row["observed_at"])
    published = (
        datetime.fromisoformat(row["published_at"]) if row["published_at"] else None
    )
    names = {
        "views": "view_count",
        "likes": "like_count",
        "comments": "reply_count",
        "shares": "share_count",
        "reposts": "repost_count",
        "quotes": "quote_count",
    }
    age = (observed - published).total_seconds() / 3600 if published else None
    return {
        "content_id": row["native_record_key"],
        "content_type": row["record_type"],
        "platform": "threads",
        "observed_at": row["observed_at"],
        "observation_id": row["observation_id"],
        "query_id": row["query_id"],
        "fetch_batch_id": row["fetch_batch_id"],
        "collection_run_id": row["collection_run_id"],
        "artifact_sha256": row["artifact_sha256"],
        **{key: row[value] for key, value in names.items()},
        "metrics_available": [
            key
            for key, value in names.items()
            if row["metric_availability"][value] == "AVAILABLE"
        ],
        "metric_availability_json": __import__("json").dumps(
            row["metric_availability"], sort_keys=True
        ),
        "content_age_hours": age,
        "engagement_total": None,
        "engagement_rate": None,
        "engagement_rate_basis": "UNCONFIRMED_PLATFORM_DEFINITION",
        "snapshot_status": (
            "INVALID_SOURCE_TIME" if age is not None and age < 0 else "OBSERVED"
        ),
        "origin": row["origin"],
    }


def geo_mentions(value, aliases=()):
    text = value["text_normalized"]
    reviewed = {item["alias"].casefold() for item in aliases}
    candidates = [
        {"alias": a, "legacy_key": scope.key}
        for scope in CITY_SCOPES
        for a in scope.aliases
        if a.casefold() not in reviewed
    ]
    candidates.extend(aliases)
    matches = []
    for item in candidates:
        for match in re.finditer(
            r"(?<!\w)" + re.escape(item["alias"]) + r"(?!\w)", text, re.IGNORECASE
        ):
            matches.append((match.start(), match.end(), match.group(), item))
    result, spans = [], set()
    for start, end, raw, item in sorted(
        matches, key=lambda x: (x[0], -(x[1] - x[0]), x[3].get("admin_version", ""))
    ):
        identity = (start, end, item.get("admin_version"))
        if identity in spans:
            continue
        spans.add(identity)
        resolved = bool(
            item.get("reference_version")
            and item.get("admin_unit_id")
            and item.get("reviewed")
        )
        result.append(
            {
                "mention_id": "sha256:"
                + digest(
                    [
                        value["content_id"],
                        value["text_hash"],
                        start,
                        end,
                        item.get("admin_version"),
                        METHOD,
                    ]
                ),
                "content_id": value["content_id"],
                "content_type": value["content_type"],
                "platform": value["platform"],
                "location_text_raw": raw,
                "address_text_raw": None,
                "location_role": "MENTION_UNCLASSIFIED",
                "old_province_id": (
                    item.get("admin_unit_id")
                    if resolved and item.get("admin_version") == "PRE_2025"
                    else None
                ),
                "old_district_id": None,
                "old_ward_id": None,
                "new_province_id": (
                    item.get("admin_unit_id")
                    if resolved and item.get("admin_version") == "POST_2025"
                    else None
                ),
                "new_ward_id": None,
                "latitude": None,
                "longitude": None,
                "admin_version": item.get("admin_version") if resolved else None,
                "admin_unit_id": item.get("admin_unit_id") if resolved else None,
                "reference_version": (
                    item.get("reference_version") if resolved else None
                ),
                "resolution_precision": "PROVINCE" if resolved else "UNRESOLVED",
                "resolution_status": "Resolved" if resolved else "Unresolved",
                "confidence": None,
                "evidence_span": raw,
                "span_start": start,
                "span_end": end,
                "extraction_method": METHOD,
                "published_date_vn": value["published_date_vn"],
                "observation_id": value["observation_id"],
                "legacy_alias_key": item.get("legacy_key"),
                "origin": value["origin"],
            }
        )
    return result


def intent_evidence(value, annotation=None):
    result = []
    for label in value["intent_labels"]:
        match = re.search(INTENTS[label], value["text_normalized"], re.IGNORECASE)
        span = match.group() if match else value["text_normalized"]
        qualified = (
            value["classification_status"] == "EVALUATED_QUALIFIED"
            and annotation
            and annotation["text_hash"] == value["text_hash"]
        )
        if qualified:
            span = annotation["evidence_spans"].get(label)
            if not span or span not in value["text_normalized"]:
                raise ValueError("Evaluated intent requires its own evidence span")
        people = re.search(
            r"(?:cần thêm|tìm|ghép)\s+(\d+)\s+(?:người|bạn)",
            value["text_normalized"],
            re.IGNORECASE,
        )
        duration = re.search(
            r"(?:thuê|đặt|chơi)\s+(\d+)\s*(giờ|phút)",
            value["text_normalized"],
            re.IGNORECASE,
        )
        result.append(
            {
                "evidence_id": "sha256:"
                + digest(
                    [value["content_id"], value["text_hash"], label, span, METHOD]
                ),
                "content_id": value["content_id"],
                "content_type": value["content_type"],
                "platform": "threads",
                "intent_type": label,
                "participant_count_mentioned": int(people[1]) if people else None,
                "requested_duration_minutes": (
                    int(duration[1]) * (60 if duration[2].casefold() == "giờ" else 1)
                    if duration
                    else None
                ),
                "requested_start_time": None,
                "requested_time_precision": "UNKNOWN",
                "price_budget_min_vnd": None,
                "price_budget_max_vnd": None,
                "intent_location_id": (
                    annotation.get("intent_locations", {}).get(label)
                    if qualified
                    else None
                ),
                "evidence_span": span,
                "confidence": None,
                "classifier_version": METHOD,
                "evaluation_reference": (
                    annotation["evaluation_reference"] if qualified else None
                ),
                "classification_status": (
                    "EVALUATED_QUALIFIED" if qualified else "EXPLORATORY_UNEVALUATED"
                ),
                "observation_id": value["observation_id"],
                "published_date_vn": value["published_date_vn"],
                "origin": value["origin"],
            }
        )
    return result
