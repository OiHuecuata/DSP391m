"""Independent content intelligence and cumulative engagement observations.

Rules are interpretable, uncalibrated extraction heuristics. Probabilities and
accuracy remain NULL until a labeled evaluation dataset exists.
"""

import re
from collections import defaultdict

from .catalog import VERSION
from .common import (
    content_id,
    digest,
    folded,
    instant,
    lineage,
    normalized,
    project,
    timestamp,
    vn_date,
)

INTENTS = {
    "SEEK_COURT": r"(?:tim|kiem|can)\s+(?:thue\s+)?san\b|co san nao\b|san nao (?:trong|con cho)",
    "BOOKING_INTENT": r"(?:can|muon|minh|toi|nhom)\s+(?:dat|thue)\s+(?:san|\d+\s+(?:tieng|gio))|dat san\s+(?:toi|ngay|mai|thu)",
    "SEEK_PLAYERS": r"(?:tim|can|kiem|tuyen)\s+(?:them\s+)?(?:\d+\s+)?(?:nguoi|ban|dong doi)\b|ghep keo",
    "SEEK_COACH": r"(?:tim|can|kiem)\s+(?:hlv|huan luyen vien|lop)|o dau co lop",
    "PRICE_INQUIRY": r"(?:gia|thue)\b.{0,30}(?:bao nhieu|the nao)|bao nhieu.{0,20}(?:gio|san)|hoi gia",
    "CAPACITY_COMPLAINT": r"(?:san.{0,25}(?:cung kin|het cho|khong con cho)|thieu san|khong dat duoc san)",
    "VENUE_FEEDBACK": r"mat san (?:tron|dep|tot|kem)|den (?:toi|sang|kem)|san (?:ban|sach|chat luong)|dich vu (?:tot|kem)",
    "SUPPLY_ADVERTISEMENT": r"nhan dat (?:lich|san)|lien he.{0,30}(?:dat san|thue san)|hotline.{0,30}\d|cho thue san|san.{0,25}uu dai",
    "SUPPLY_OPENING": r"khai truong.{0,40}(?:san|pickleball)|mo them\s+\d+\s+san|san.{0,30}moi khai truong",
    "SUPPLY_CLOSURE": r"san.{0,35}(?:dong cua|ngung (?:hoat dong|kinh doanh))",
    "EVENT_TOURNAMENT": r"(?:giai|su kien)\s+(?:dau|giao huu|pickleball)|tournament",
}
SUPPLY = {"SUPPLY_ADVERTISEMENT", "SUPPLY_OPENING", "SUPPLY_CLOSURE"}
NEGATIVE = re.compile(r"mat san tron|den toi|san ban|dich vu kem")
POSITIVE = re.compile(r"mat san dep|mat san tot|den sang|san sach|dich vu tot")


def classify(text):
    scan = folded(text)
    evidence = []
    for label, pattern in INTENTS.items():
        for match in re.finditer(pattern, scan):
            if re.search(
                r"(?:khong|chang|chua)\s+$",
                scan[max(0, match.start() - 12) : match.start()],
            ):
                continue
            evidence.append((label, match.start(), match.end()))
    commercial = any(label in SUPPLY for label, _, _ in evidence)
    if commercial:
        evidence = [
            e
            for e in evidence
            if e[0] not in ("SEEK_COURT", "BOOKING_INTENT", "PRICE_INQUIRY")
        ]
    explicit_topic = bool(re.search(r"\bpickle\s*ball\b|\bpickleball\b", scan))
    related = (
        True
        if explicit_topic
        else None if evidence or re.search(r"\bsan\b", scan) else False
    )
    if not evidence:
        evidence = [
            (
                (
                    "GENERAL_DISCUSSION"
                    if related
                    else "IRRELEVANT" if related is False else "GENERAL_DISCUSSION"
                ),
                0,
                len(text),
            )
        ]
    negative, positive = bool(NEGATIVE.search(scan)), bool(POSITIVE.search(scan))
    sentiment = (
        "MIXED"
        if negative and positive
        else "NEGATIVE" if negative else "POSITIVE" if positive else None
    )
    labels = sorted({e[0] for e in evidence})
    topics = sorted(
        {
            (
                "SUPPLY"
                if l in SUPPLY
                else (
                    "FACILITIES"
                    if l == "VENUE_FEEDBACK"
                    else (
                        "PRICE"
                        if l == "PRICE_INQUIRY"
                        else "EVENT" if l == "EVENT_TOURNAMENT" else "PLAYING_INTENT"
                    )
                )
            )
            for l in labels
            if l not in ("GENERAL_DISCUSSION", "IRRELEVANT")
        }
    )
    return {
        "is_pickleball_related": related,
        "is_commercial": commercial if related is not False else False,
        "intent_labels": labels,
        "topic_labels": topics,
        "sentiment": sentiment,
        "classification_confidence": None,
        "relevance_confidence": None,
        "classification_status": (
            "RULE_SUPPORTED_UNCALIBRATED" if related is not None else "UNCERTAIN_TOPIC"
        ),
        "extraction_method": "DETERMINISTIC_RULE_UNCALIBRATED",
    }, evidence


def build_content(rows, *, comment=False, cache=(), cache_spans=None):
    groups = defaultdict(list)
    saved = {r["content_id"]: r for r in cache}
    cache_spans = cache_spans or {}
    for row in rows:
        groups[(row["source_platform"], row["source_record_id"])].append(row)
    result, spans, reprocessed = [], {}, 0
    for (platform, sid), observations in sorted(groups.items()):
        observations.sort(
            key=lambda r: (instant(r["collected_at"]), r["observation_id"])
        )
        row = observations[-1]
        cid = content_id(platform, sid, comment=comment)
        text = (
            normalized(row.get("comment_text_raw"))
            if comment
            else normalized(
                " ".join(
                    dict.fromkeys(
                        v
                        for v in (
                            row.get("title_raw"),
                            row.get("text_raw"),
                            row.get("description_raw"),
                        )
                        if v
                    )
                )
            )
        )
        text_hash = digest(text)
        prior = saved.get(cid)
        if (
            prior
            and prior["text_hash"] == text_hash
            and prior["processing_version"] == VERSION
        ):
            classification = {
                k: v
                for k, v in prior.items()
                if k
                in (
                    "is_pickleball_related",
                    "is_commercial",
                    "intent_labels",
                    "topic_labels",
                    "sentiment",
                    "classification_confidence",
                    "relevance_confidence",
                    "classification_status",
                    "extraction_method",
                )
            }
            # Reuse persisted evidence offsets, too: metric-only changes must
            # not invoke the text classifier just to recover its spans.
            spans[cid] = cache_spans[cid] if cid in cache_spans else classify(text)[1]
        else:
            classification, spans[cid] = classify(text)
            reprocessed += 1
        published = timestamp(row.get("published_at"))
        base = dict(
            content_id=cid,
            platform=platform,
            source_record_id=sid,
            source_url=row.get("source_url"),
            content_type=(
                "Reply"
                if comment and row.get("is_reply")
                else "Comment" if comment else row.get("content_type", "Post").title()
            ),
            published_at=published,
            published_date_vn=vn_date(published),
            text_hash=text_hash,
            first_seen_at=timestamp(observations[0]["collected_at"]),
            last_seen_at=timestamp(row["collected_at"]),
            is_duplicate=False,
            duplicate_group_id=text_hash if text else None,
            lineage_ids=lineage(observations),
            **classification,
        )
        if comment:
            base.update(
                comment_id=cid,
                post_id=content_id(platform, row["native_post_id"]),
                parent_comment_id=(
                    content_id(platform, row["parent_comment_id"], comment=True)
                    if row.get("parent_comment_id")
                    else None
                ),
                comment_text_normalized=text,
                is_reply=bool(row.get("is_reply")),
            )
        else:
            base.update(
                post_id=cid,
                parent_post_id=(
                    content_id(platform, row["parent_content_id"])
                    if row.get("parent_content_id")
                    else None
                ),
                text_normalized=text,
                language=row.get("language_raw"),
            )
        result.append(
            project(
                "silver_social_comments" if comment else "silver_social_posts", base
            )
        )
    # Equal text alone is insufficient to declare two authors' messages copies.
    # Exact source permalink + text can identify duplicate collector identities.
    permalink_groups = defaultdict(list)
    for row in result:
        if row["source_url"]:
            permalink_groups[
                (row["platform"], row["source_url"], row["text_hash"])
            ].append(row)
    for group in permalink_groups.values():
        for row in sorted(group, key=lambda r: r["content_id"])[1:]:
            row["is_duplicate"] = True
    return result, spans, reprocessed


def intent_evidence(content, spans, mentions):
    text = content.get("text_normalized", content.get("comment_text_normalized", ""))
    scan = folded(text)
    participants = re.search(r"\b(\d{1,3})\s+nguoi\b", scan)
    duration = re.search(r"\b(\d{1,2}(?:[.,]\d)?)\s+(tieng|gio|phut)\b", scan)
    duration_minutes = (
        int(float(duration[1].replace(",", ".")) * (1 if duration[2] == "phut" else 60))
        if duration
        else None
    )
    time = re.search(
        r"(?:toi|ngay)\s+(?:mai|nay|thu\s+[a-z]+)|thu\s+(?:hai|ba|tu|nam|sau|bay)|chu nhat|\b\d{1,2}h(?:\d{2})?\b|\b\d{4}-\d{2}-\d{2}\b",
        scan,
    )
    budget = re.search(
        r"(?:ngan sach|budget|toi da)\s+(\d+(?:[.,]\d+)?)\s*(k|nghin|ngan|trieu|vnd)\b",
        scan,
    )
    budget_value = (
        float(budget[1].replace(",", "."))
        * {"k": 1000, "nghin": 1000, "ngan": 1000, "trieu": 1000000, "vnd": 1}[
            budget[2]
        ]
        if budget
        else None
    )
    # Retain ambiguous multi-place evidence without arbitrarily selecting one.
    linked_mention = mentions[0]["mention_id"] if len(mentions) == 1 else None
    result = []
    for label, start, end in spans:
        result.append(
            project(
                "silver_social_intent_evidence",
                {
                    "evidence_id": digest(
                        [content["content_id"], content["text_hash"], label, start, end]
                    ),
                    "content_id": content["content_id"],
                    "intent_type": label,
                    "participant_count_mentioned": (
                        int(participants[1])
                        if participants and int(participants[1]) > 0
                        else None
                    ),
                    "requested_duration_minutes": (
                        duration_minutes
                        if duration_minutes and duration_minutes > 0
                        else None
                    ),
                    "requested_time_precision": (
                        "TEXT_ONLY_UNRESOLVED" if time else "NOT_MENTIONED"
                    ),
                    "requested_time_text": (
                        text[time.start() : time.end()] if time else None
                    ),
                    "price_budget_max_vnd": budget_value,
                    "intent_location_id": linked_mention,
                    "evidence_span": text[start:end],
                    "span_start": start,
                    "span_end": end,
                    "extraction_method": "DETERMINISTIC_RULE_UNCALIBRATED",
                    "actual_booking_confirmed": False,
                    "lineage_ids": content["lineage_ids"],
                },
            )
        )
    return result


METRICS = {
    "views": "view_count",
    "likes": "like_count",
    "comments": "comment_count",
    "shares": "share_count",
    "reposts": "repost_count",
    "quotes": "quote_count",
    "replies": "reply_count",
}


def engagement_snapshots(rows, contents):
    content_lookup = {r["content_id"]: r for r in contents}
    grouped = defaultdict(list)
    for row in rows:
        cid = content_id(
            row["source_platform"],
            row["native_content_id"],
            comment=row["content_type"] == "comment",
        )
        # Orphan snapshots remain in Bronze; deletion or expiry cannot restore
        # removed content through a metric-only observation.
        if cid not in content_lookup:
            continue
        if row["metric_scope"] != "cumulative":
            raise ValueError("unsupported metric scope")
        grouped[(cid, timestamp(row["observed_at"]))].append(row)
    result = []
    for (cid, observed), observations in sorted(grouped.items()):
        row = max(
            observations,
            key=lambda r: (instant(r["collected_at"]), r["observation_id"]),
        )
        values = {k: row.get(native) for k, native in METRICS.items()}
        if any(
            v is not None and (type(v) is not int or v < 0) for v in values.values()
        ):
            raise ValueError("invalid nonnegative cumulative metric")
        conflicting = any(
            any(r.get(native) != row.get(native) for native in METRICS.values())
            for r in observations
        )
        created = content_lookup[cid]["published_at"]
        age = (
            (instant(observed) - instant(created)).total_seconds() / 3600
            if created
            else None
        )
        valid = not conflicting and (age is None or age >= 0)
        available = sorted(k for k, v in values.items() if v is not None)
        # No confirmed cross-platform definition is bundled. Do not fabricate
        # engagement totals or rates by treating unavailable shares as zero.
        snapshot = project(
            "silver_social_engagement_snapshots",
            dict(
                snapshot_id=digest([cid, observed]),
                content_id=cid,
                content_type=row["content_type"],
                platform=row["source_platform"],
                observed_at=observed,
                observed_date_vn=vn_date(observed),
                metric_scope="cumulative",
                content_age_hours=age if age is None or age >= 0 else None,
                metrics_available=available,
                metric_availability_json=row["metric_availability"],
                snapshot_status=(
                    "CONFLICTING"
                    if conflicting
                    else (
                        "INVALID_TIME"
                        if not valid
                        else "AVAILABLE" if available else "MISSING"
                    )
                ),
                delta_status="NO_PREDECESSOR",
                lineage_ids=lineage(observations),
                **values,
            ),
        )
        result.append(snapshot)
    by_content = defaultdict(list)
    for row in result:
        by_content[row["content_id"]].append(row)
    for group in by_content.values():
        previous = None
        for row in sorted(group, key=lambda r: r["observed_at"]):
            if row["snapshot_status"] not in ("AVAILABLE", "MISSING"):
                continue
            if previous:
                hours = (
                    instant(row["observed_at"]) - instant(previous["observed_at"])
                ).total_seconds() / 3600
                row.update(
                    previous_snapshot_id=previous["snapshot_id"], elapsed_hours=hours
                )
                for metric in ("views", "likes", "comments", "shares"):
                    row["delta_" + metric] = (
                        row[metric] - previous[metric]
                        if row[metric] is not None
                        and previous[metric] is not None
                        and hours > 0
                        else None
                    )
                row["views_per_hour"] = (
                    row["delta_views"] / hours
                    if row["delta_views"] is not None and hours > 0
                    else None
                )
                row["delta_status"] = (
                    "SOURCE_CORRECTION"
                    if any(
                        row["delta_" + k] is not None and row["delta_" + k] < 0
                        for k in ("views", "likes", "comments", "shares")
                    )
                    else (
                        "AVAILABLE"
                        if any(
                            row["delta_" + k] is not None
                            for k in ("views", "likes", "comments", "shares")
                        )
                        else "MISSING_INPUTS"
                    )
                )
            previous = row
    return result
