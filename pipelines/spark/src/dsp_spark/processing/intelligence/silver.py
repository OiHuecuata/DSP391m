"""Compose Silver transformations; no collector or transport dependencies."""

from collections import defaultdict

from .catalog import SILVER
from .common import digest
from .social import build_content, engagement_snapshots, intent_evidence
from .venues import build_venues, link_content_venues


def build_silver(datasets, geography, *, previous=None):
    previous = previous or {}
    result = {name: [] for name in SILVER}
    cached_spans = defaultdict(list)
    cached_content = {
        r["content_id"]: r
        for name in ("silver_social_posts", "silver_social_comments")
        for r in previous.get(name, [])
    }
    for evidence in previous.get("silver_social_intent_evidence", []):
        content = cached_content.get(evidence["content_id"])
        # A failed cross-table commit may leave current text beside older
        # evidence. Reuse spans only when their identity proves the same text
        # hash and processing version; otherwise repair them from source text.
        if (
            not content
            or evidence["processing_version"] != content["processing_version"]
            or evidence["evidence_id"]
            != digest(
                [
                    content["content_id"],
                    content["text_hash"],
                    evidence["intent_type"],
                    evidence["span_start"],
                    evidence["span_end"],
                ]
            )
        ):
            continue
        cached_spans[evidence["content_id"]].append(
            (evidence["intent_type"], evidence["span_start"], evidence["span_end"])
        )
    posts, post_spans, post_work = build_content(
        datasets.get("bronze_social_posts", []),
        cache=previous.get("silver_social_posts", []),
        cache_spans=cached_spans,
    )
    comments, comment_spans, comment_work = build_content(
        datasets.get("bronze_social_comments", []),
        comment=True,
        cache=previous.get("silver_social_comments", []),
        cache_spans=cached_spans,
    )
    contents = posts + comments
    result["silver_social_posts"], result["silver_social_comments"] = posts, comments
    result["dim_admin_unit"] = geography.units
    result["bridge_admin_crosswalk"] = geography.crosswalk
    result["dim_location_alias"] = geography.aliases
    # Venue evidence retains every permitted text observation, including text
    # later edited at the source. Current content remains one source entity.
    # This is cheap deterministic extraction, separate from cached NLP results.
    history_contents = []
    for dataset, is_comment in (
        ("bronze_social_posts", False),
        ("bronze_social_comments", True),
    ):
        for observation in datasets.get(dataset, []):
            historical, _, _ = build_content(
                [observation],
                comment=is_comment,
                cache=contents,
                cache_spans=post_spans | comment_spans,
            )
            for item in historical:
                history_contents.append(
                    {**item, "source_observation_id": observation["observation_id"]}
                )
    result.update(
        build_venues(
            datasets.get("bronze_court_observations", []),
            history_contents,
            geography,
            previous_links=previous.get("silver_venue_source_links", []),
            previous_candidates=previous.get("silver_venue_candidates", []),
        )
    )
    link_content_venues(contents, result["silver_court_venues"])
    grouped = defaultdict(list)
    for content in contents:
        labels = content["intent_labels"]
        role = (
            "SUPPLY"
            if any(l.startswith("SUPPLY_") for l in labels)
            else (
                "SEEK_COURT"
                if "SEEK_COURT" in labels
                else "EVENT" if "EVENT_TOURNAMENT" in labels else "UNSPECIFIED"
            )
        )
        mentions = geography.mentions(
            content,
            content.get("text_normalized", content.get("comment_text_normalized", "")),
            role=role,
        )
        grouped[content["content_id"]] = mentions
        result["silver_social_geo_mentions"].extend(mentions)
        spans = (post_spans | comment_spans)[content["content_id"]]
        result["silver_social_intent_evidence"].extend(
            intent_evidence(content, spans, mentions)
        )
    result["silver_social_engagement_snapshots"] = engagement_snapshots(
        datasets.get("bronze_engagement_snapshots", []), contents
    )
    return result, {
        "content_classifications_processed": post_work + comment_work,
        "content_classifications_reused": len(contents) - post_work - comment_work,
    }
