"""Source observations, shared by local ingestion and Kafka; no NLP or geography.

Immutable identity includes run and measurement time. Source hashes exclude
collection/query enrichment. No lifecycle event is inferred from search absence.
Policy files are operator-reviewed inputs; no retention duration is guessed.
"""

import hashlib
import json
import re
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from urllib.parse import urlencode

from jsonschema import Draft202012Validator, FormatChecker, ValidationError
from referencing import Registry, Resource

DATASETS = (
    "bronze_court_observations",
    "bronze_social_posts",
    "bronze_social_comments",
    "bronze_engagement_snapshots",
    "bronze_collection_runs",
    "bronze_source_changes",
)
METRICS = (
    "view_count",
    "like_count",
    "comment_count",
    "share_count",
    "repost_count",
    "quote_count",
    "reply_count",
)


def encode(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def decode(raw):
    def object_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON property: " + key)
            result[key] = value
        return result

    def reject_constant(value):
        raise ValueError("nonfinite JSON number: " + value)

    return json.loads(
        raw, object_pairs_hook=object_pairs, parse_constant=reject_constant
    )


def apply_source_policy(row, policies):
    policy = policies.get(row["source_platform"], {})
    if policy.get("storage_permission") is False:
        raise ValueError("source policy prohibits Bronze storage")
    if policy:
        row = {
            **row,
            "retention_policy_id": policy["retention_policy_id"],
            "retention_expires_at": (
                timestamp(
                    datetime.fromisoformat(timestamp(row["collected_at"]))
                    + timedelta(days=policy["retention_days"])
                )
                if policy.get("retention_days")
                else None
            ),
        }
    if row.get("retention_expires_at") and datetime.fromisoformat(
        row["retention_expires_at"]
    ) <= datetime.now(UTC):
        raise ValueError(
            "source policy prohibits ingesting expired data; refresh required"
        )
    return row


def digest(value):
    return "sha256:" + hashlib.sha256(encode(value).encode()).hexdigest()


def timestamp(value, *, required=False):
    if value is None and not required:
        return None
    if isinstance(value, datetime):
        when = value
        if when.utcoffset() is None:
            raise ValueError("timestamp must include timezone")
    elif type(value) in (int, float):
        when = datetime.fromtimestamp(value, UTC)
    elif isinstance(value, str):
        when = datetime.fromisoformat(value)
        if when.utcoffset() is None:
            raise ValueError("timestamp must include timezone")
    else:
        raise ValueError("invalid timestamp")
    return (
        when.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    )


def identity(value):
    if type(value) not in (str, int) or not str(value).strip():
        raise ValueError("missing stable source identifier")
    return str(value)


def counter(value):
    if type(value) is float and value.is_integer() and 0 <= value < 2**63:
        value = int(value)
    if isinstance(value, str) and value.isascii() and value.isdecimal():
        value = int(value)
    if value is None:
        return None
    if type(value) is not int or not 0 <= value < 2**63:
        raise ValueError("invalid engagement counter")
    return value


@lru_cache(maxsize=1)
def validators():
    root = next(
        (
            p / "contracts/data/bronze"
            for p in Path(__file__).resolve().parents
            if (p / "contracts/data/bronze/observation.v1.schema.json").exists()
        ),
        None,
    )
    root = root or files("dsp_contracts") / "data/bronze"
    names = ["observation"] + [
        n.removeprefix("bronze_").replace("_", "-") for n in DATASETS
    ]
    docs = {
        "file:///bronze/"
        + n
        + ".v1.schema.json": json.loads((root / (n + ".v1.schema.json")).read_text())
        for n in names
    }
    registry = Registry().with_resources(
        (uri, Resource.from_contents({**doc, "$id": uri})) for uri, doc in docs.items()
    )
    return {
        dataset: Draft202012Validator(
            docs[
                "file:///bronze/"
                + dataset.removeprefix("bronze_").replace("_", "-")
                + ".v1.schema.json"
            ],
            registry=registry,
            format_checker=FormatChecker(),
            _resolver=registry.resolver(
                "file:///bronze/"
                + dataset.removeprefix("bronze_").replace("_", "-")
                + ".v1.schema.json"
            ),
        )
        for dataset in DATASETS
    }


def validate_observation(dataset, row):
    validators()[dataset].validate(row)
    expected = digest(
        [
            dataset,
            row["source_platform"],
            row["source_record_id"],
            row["collection_run_id"],
            timestamp(row["collected_at"], required=True),
        ]
    )
    if row["observation_id"] != expected:
        raise ValueError("observation identity conflicts with lineage")
    identity(row["source_platform"])
    identity(row["source_record_id"])
    identity(row["collection_run_id"])
    native_field = {
        "bronze_court_observations": "source_venue_id",
        "bronze_social_posts": "native_post_id",
        "bronze_social_comments": "native_comment_id",
        "bronze_engagement_snapshots": "native_content_id",
    }.get(dataset)
    if native_field and row[native_field] != row["source_record_id"]:
        raise ValueError("native identifier conflicts with source_record_id")
    if (
        dataset == "bronze_court_observations"
        and row["court_count_raw"] is not None
        and re.fullmatch(r"-\d+(?:\.\d+)?", row["court_count_raw"])
    ):
        raise ValueError("negative source court count")
    if dataset == "bronze_social_comments" and row["is_reply"] != (
        row["parent_comment_id"] is not None
    ):
        raise ValueError("reply flag conflicts with parent comment evidence")
    if dataset == "bronze_engagement_snapshots":
        availability = decode(row["metric_availability"])
        if set(availability) != set(METRICS) or any(
            type(availability[k]) is not bool or availability[k] != (row[k] is not None)
            for k in METRICS
        ):
            raise ValueError("metric availability conflicts with measured counters")
        if timestamp(row["observed_at"]) != timestamp(row["collected_at"]):
            raise ValueError("metric measurement timestamp conflicts with observation")
    payload = decode(row["raw_payload"])
    if not isinstance(payload, dict):
        raise TypeError("raw source payload must be an object")
    check_secrets(payload)
    if digest(payload) != row["payload_hash"]:
        raise ValueError("payload hash conflicts with raw payload")
    for name in (
        "query_metadata",
        "metric_availability",
        "errors_json",
        "requests_json",
        "evidence_json",
        "ingestion_metadata",
        "amenities_raw",
    ):
        if row.get(name) is not None:
            decode(row[name])


def load_policies(path=None):
    if path is None:
        return {}
    policies = json.loads(Path(path).read_text())
    for source, policy in policies.items():
        identity(source)
        identity(policy.get("retention_policy_id"))
        if type(policy.get("storage_permission")) is not bool:
            raise ValueError("policy requires explicit storage_permission")
        days = policy.get("retention_days")
        if days is not None and (type(days) is not int or days <= 0):
            raise ValueError("retention_days must be positive or null")
        if policy.get("allowed_fields") not in (None, "*"):
            raise ValueError(
                "field-restricted source policies require a reviewed adapter"
            )
    return policies


def check_secrets(value):
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = re.sub(r"[^a-z0-9]", "", key.lower())
            if normalized in {
                "authorization",
                "cookie",
                "cookies",
                "password",
                "accesstoken",
                "refreshtoken",
                "apikey",
                "xgoogapikey",
                "authtoken",
                "token",
                "clientsecret",
                "sessionid",
                "csrftoken",
            }:
                raise ValueError("credential field is prohibited in Bronze")
            check_secrets(child)
    elif isinstance(value, list):
        for child in value:
            check_secrets(child)


def extract_observations(envelope, *, artifact_uri=None, policies=None):
    """Return six datasets and indexed errors, keeping successful partial data.

    Legacy projected counters are used only when no native payload is available.
    Historical zeros whose availability was lost cannot be reconstructed.
    """
    check_secrets(envelope)
    source = identity(envelope["source"])
    run = identity(envelope["run_id"])
    collected = timestamp(envelope["collected_at"], required=True)
    policy = (policies or {}).get(source, {})
    if policy.get("storage_permission") is False:
        raise ValueError("source policy prohibits Bronze storage")
    expires = (
        timestamp(
            datetime.fromisoformat(collected) + timedelta(days=policy["retention_days"])
        )
        if policy.get("retention_days")
        else None
    )
    if expires and datetime.fromisoformat(expires) <= datetime.now(UTC):
        raise ValueError(
            "source policy prohibits ingesting expired data; refresh required"
        )
    result = {name: [] for name in DATASETS}
    errors, seen = [], {}

    def base(
        dataset,
        payload,
        source_id,
        kind,
        *,
        queries=None,
        url=None,
        created=None,
        updated=None,
        observed_at=None,
    ):
        source_id = identity(source_id)
        when = (
            timestamp(observed_at, required=True)
            if observed_at is not None
            else collected
        )
        row = {
            "schema_version": "1",
            "observation_id": digest([dataset, source, source_id, run, when]),
            "source_platform": source,
            "source_record_id": source_id,
            "record_type": kind,
            "source_url": url,
            "collected_at": when,
            "source_created_at": timestamp(created),
            "source_updated_at": timestamp(updated),
            "collection_run_id": run,
            "query_id": (
                digest(
                    sorted(
                        {
                            str(q.get("query")) if isinstance(q, dict) else str(q)
                            for q in queries
                        }
                    )
                )
                if queries
                else None
            ),
            "query_metadata": encode(queries or []),
            "raw_payload": encode(payload),
            "payload_hash": digest(payload),
            "retention_policy_id": policy.get("retention_policy_id"),
            "retention_expires_at": expires,
            "artifact_uri": artifact_uri,
            "ingested_at": None,
            "ingestion_metadata": None,
        }
        return row

    def add(dataset, row):
        validate_observation(dataset, row)
        key = (dataset, row["observation_id"])
        if key in seen:
            if seen[key] != row:
                raise ValueError(
                    "conflicting duplicate observation within collection run"
                )
            return
        seen[key] = row
        result[dataset].append(row)

    def metrics(entity, values, observed_at=None):
        dataset = "bronze_engagement_snapshots"
        row = base(
            dataset,
            json.loads(entity["raw_payload"]),
            entity["source_record_id"],
            "Metric",
            queries=json.loads(entity["query_metadata"]),
            url=entity["source_url"],
            created=entity["source_created_at"],
            updated=entity["source_updated_at"],
            observed_at=observed_at or entity["collected_at"],
        )
        row.update(
            native_content_id=entity["source_record_id"],
            content_type=entity["record_type"].lower(),
            observed_at=row["collected_at"],
            metric_scope="cumulative",
            metric_availability=encode({k: values.get(k) is not None for k in METRICS}),
        )
        row.update({k: counter(values.get(k)) for k in METRICS})
        add(dataset, row)

    def social(payload, *, parent=None, parent_comment=None, queries=None):
        native = payload.get("raw_payload") or payload
        detail = payload.get("detail") or {}
        if not isinstance(native, dict) or not isinstance(detail, dict):
            raise TypeError("invalid native source object or detail object")
        if detail.get("raw_payload"):
            supplied = detail["raw_payload"]
            detail_id = supplied.get("pk") or supplied.get("id")
            if detail_id is not None and str(detail_id) == str(payload.get("id")):
                native = supplied
            else:
                errors.append(
                    {
                        "source_record_id": payload.get("id"),
                        "error": "detail identity conflicts with source post",
                    }
                )
                detail = {}
        if "raw_payload" not in payload and not detail.get("raw_payload"):
            native = {
                k: v
                for k, v in native.items()
                if k
                not in {
                    "matched_queries",
                    "query_contexts",
                    "categories",
                    "detected_locations",
                    "relevance",
                    "detail",
                    "detail_status",
                    "detail_error",
                    "_observed_at",
                    "_metrics_observed_at",
                    "capture_run_id",
                    "detail_coverage",
                }
            }
        source_id = payload.get("id") or native.get("pk") or native.get("id")
        source_id = identity(source_id)
        if source == "threads" and (
            not source_id.isascii() or not source_id.isdecimal() or int(source_id) <= 0
        ):
            raise ValueError("invalid Threads source identifier")
        if source == "youtube" and (
            (parent is None and re.fullmatch(r"[A-Za-z0-9_-]+", source_id) is None)
            or (parent is not None and any(c.isspace() for c in source_id))
        ):
            raise ValueError("invalid YouTube source identifier")
        snippet = native.get("snippet") or {}
        info = native.get("text_post_app_info") or {}
        is_comment = (
            parent is not None
            or payload.get("is_reply") is True
            or info.get("is_reply") is True
        )
        if source == "youtube":
            created = snippet.get("publishedAt")
            updated = snippet.get("updatedAt")
            text = (
                snippet.get("textOriginal", snippet.get("textDisplay"))
                if is_comment
                else None
            )
            url = "https://www.youtube.com/watch?" + urlencode(
                {"v": identity(parent), "lc": source_id}
                if is_comment
                else {"v": source_id}
            )
            values = {
                k: (snippet if is_comment else native.get("statistics", {})).get(
                    k.replace("_count", "Count")
                )
                for k in ("view_count", "like_count", "comment_count")
            }
        else:
            created, updated = native.get(
                "taken_at", payload.get("taken_at")
            ), native.get("updated_at")
            caption = native.get("caption") or {}
            text = caption.get("text", payload.get("text"))
            url = payload.get("url") or detail.get("url")
            values = {k: native.get(k) for k in METRICS}
            for key, native_key in (
                ("reply_count", "direct_reply_count"),
                ("repost_count", "repost_count"),
                ("quote_count", "quote_count"),
            ):
                values[key] = (
                    info.get(native_key)
                    if "raw_payload" in payload or detail.get("raw_payload")
                    else payload.get(key)
                )
            parent = (
                parent
                or payload.get("native_post_id")
                or (
                    info.get("reply_to_post_id")
                    if str(info.get("reply_to_post_id")) in known_roots
                    else None
                )
                or (
                    payload.get("reply_to_post_id")
                    if str(payload.get("reply_to_post_id")) in known_roots
                    else None
                )
            )
            if (
                "engagement" in payload
                and "raw_payload" not in payload
                and not detail.get("raw_payload")
            ):
                values.update(payload["engagement"])
        dataset = "bronze_social_comments" if is_comment else "bronze_social_posts"
        row = base(
            dataset,
            native,
            source_id,
            "Comment" if is_comment else "Video" if source == "youtube" else "Post",
            queries=queries
            or payload.get("query_contexts")
            or payload.get("matched_queries"),
            url=url,
            created=created,
            updated=updated,
            observed_at=(
                (detail.get("_observed_at") or payload.get("_observed_at"))
                if detail.get("raw_payload")
                else payload.get("_observed_at")
            ),
        )
        if is_comment:
            parent_comment = (
                parent_comment
                or snippet.get("parentId")
                or payload.get("parent_comment_id")
            )
            row.update(
                native_comment_id=identity(source_id),
                native_post_id=identity(parent),
                parent_comment_id=parent_comment,
                comment_text_raw=text,
                published_at=row["source_created_at"],
                updated_at=row["source_updated_at"],
                is_reply=parent_comment is not None,
            )
        else:
            row.update(
                native_post_id=identity(source_id),
                content_type="video" if source == "youtube" else "post",
                parent_content_id=payload.get("parent_content_id")
                or info.get("quoted_post_id"),
                title_raw=(
                    snippet.get("title") if source == "youtube" else native.get("title")
                ),
                text_raw=text if source != "youtube" else None,
                description_raw=(
                    snippet.get("description")
                    if source == "youtube"
                    else native.get("description")
                ),
                published_at=row["source_created_at"],
                language_raw=(
                    snippet.get("defaultLanguage")
                    if source == "youtube"
                    else native.get("language")
                ),
                source_permalink=url,
            )
        # Malformed optional counters must not erase valid source text. Keep
        # the native value in raw_payload and report its unusable measurement.
        for key, value in list(values.items()):
            try:
                values[key] = counter(value)
            except (ValueError, TypeError) as exc:
                errors.append(
                    {"source_record_id": source_id, "field": key, "error": str(exc)}
                )
                values[key] = None
        add(dataset, row)
        metrics(row, values, payload.get("_metrics_observed_at"))
        search_native = payload.get("raw_payload")
        search_time = payload.get("_observed_at")
        if (
            source == "threads"
            and search_native
            and detail.get("_observed_at")
            and search_time
            and timestamp(search_time) != row["collected_at"]
        ):
            search_values = {key: search_native.get(key) for key in METRICS}
            search_info = search_native.get("text_post_app_info") or {}
            for key, native_key in (
                ("reply_count", "direct_reply_count"),
                ("repost_count", "repost_count"),
                ("quote_count", "quote_count"),
            ):
                search_values[key] = search_info.get(native_key)
            for key, value in list(search_values.items()):
                try:
                    search_values[key] = counter(value)
                except (ValueError, TypeError) as exc:
                    errors.append(
                        {
                            "source_record_id": source_id,
                            "field": key,
                            "error": str(exc),
                            "capture": "search",
                        }
                    )
                    search_values[key] = None
            metrics(
                {**row, "raw_payload": encode(search_native)},
                search_values,
                search_time,
            )
        for reply in detail.get("visible_replies", []):
            try:
                social(
                    {
                        **reply,
                        "_observed_at": reply.get(
                            "_observed_at",
                            detail.get("_observed_at", payload.get("_observed_at")),
                        ),
                    },
                    parent=source_id,
                    parent_comment=reply.get("parent_comment_id"),
                    queries=queries or payload.get("matched_queries"),
                )
            except (
                ValueError,
                TypeError,
                KeyError,
                OverflowError,
                ValidationError,
            ) as exc:
                errors.append({"parent_id": source_id, "error": str(exc)})

    known_roots = set()
    root_candidates = list(envelope.get("posts", []))
    for evidence in envelope.get("search_evidence", []):
        root_candidates.extend(evidence.get("native", {}).get("all_posts", []))
    for candidate in root_candidates:
        if not isinstance(candidate, dict):
            continue
        raw = candidate.get("raw_payload") or candidate
        if not isinstance(raw, dict):
            continue
        info = raw.get("text_post_app_info") or {}
        if not isinstance(info, dict):
            continue
        if (
            info.get("is_reply") is False
            or not candidate.get("raw_payload")
            and candidate.get("is_reply") is False
        ):
            known_roots.add(str(candidate.get("id") or raw.get("pk")))
    inputs = (
        [(v, None) for v in envelope.get("videos", [])]
        + [(c.get("payload", {}), c) for c in envelope.get("comments", [])]
        if source == "youtube"
        else [(p, None) for p in envelope.get("posts", [])]
    )
    for index, (payload, wrapper) in enumerate(inputs):
        try:
            if source == "pbplus":
                try:
                    counter(payload.get("number_of_courts"))
                except ValueError as exc:
                    raise ValueError("invalid source court count") from exc
                sid = payload.get("id") or payload.get("url")
                row = base(
                    "bronze_court_observations",
                    payload,
                    sid,
                    "Venue",
                    queries=payload.get("matched_queries")
                    or [envelope.get("seed_query")],
                    url=payload.get("url")
                    or (
                        sid
                        if isinstance(sid, str)
                        and sid.startswith(("https://", "http://"))
                        else None
                    ),
                )
                row.update(
                    source_venue_id=identity(sid),
                    venue_name_raw=payload.get("court_name"),
                    address_raw=payload.get("address"),
                    latitude_raw=(
                        float(payload["latitude"])
                        if payload.get("latitude") is not None
                        else None
                    ),
                    longitude_raw=(
                        float(payload["longitude"])
                        if payload.get("longitude") is not None
                        else None
                    ),
                    court_count_raw=(
                        str(payload["number_of_courts"])
                        if payload.get("number_of_courts") is not None
                        else None
                    ),
                    price_raw=payload.get("price"),
                    amenities_raw=(
                        encode(payload["services"])
                        if payload.get("services") is not None
                        else None
                    ),
                    opening_hours_raw=(
                        encode(payload["opening_hours"])
                        if payload.get("opening_hours") is not None
                        else None
                    ),
                    venue_status_raw=payload.get("status"),
                    description_raw=payload.get("description"),
                )
                add("bronze_court_observations", row)
            else:
                parent = wrapper.get("video_id") if wrapper else None
                queries = (
                    envelope.get("video_queries", {}).get(
                        parent or payload.get("id"), []
                    )
                    if source == "youtube"
                    else payload.get("matched_queries")
                )
                social(
                    payload,
                    parent=parent,
                    parent_comment=(
                        wrapper.get("parent_comment_id") if wrapper else None
                    ),
                    queries=queries,
                )
        except (ValueError, TypeError, KeyError, OverflowError, ValidationError) as exc:
            errors.append(
                {"record_index": index, "error": str(exc), "raw_payload": payload}
            )
    for event in envelope.get("source_changes", []):
        try:
            if event.get("change_type") not in (
                "deleted",
                "unavailable",
            ) or not event.get("evidence"):
                raise ValueError("explicit lifecycle events require source evidence")
            evidence = event["evidence"]
            verified = bool(
                evidence.get("verified_endpoint") or evidence.get("source_url")
            )
            supported = (
                evidence.get("http_status") == 410
                or evidence.get("source_tombstone") is True
            )
            if not verified or (event["change_type"] == "deleted" and not supported):
                raise ValueError(
                    "deletion requires verified source tombstone or HTTP 410"
                )
            if event["change_type"] == "unavailable" and not (
                supported or evidence.get("http_status") in (403, 404)
            ):
                raise ValueError("unavailability requires verified source response")
            sid = identity(event.get("source_record_id"))
            row = base("bronze_source_changes", event, sid, "Change")
            row.update(
                change_type=event["change_type"],
                previous_payload_hash=None,
                current_payload_hash=None,
                previous_observation_id=None,
                entity_record_type=identity(event.get("record_type")),
                evidence_json=encode(event["evidence"]),
            )
            add("bronze_source_changes", row)
        except (ValueError, TypeError, KeyError, ValidationError) as exc:
            errors.append({"error": str(exc), "raw_payload": event})
    source_errors = list(envelope.get("errors", []))
    source_errors.extend(
        q
        for q in envelope.get("query_runs", [])
        if q.get("status") == "error"
        or source != "threads"
        and q.get("status") == "partial"
        or q.get("detail_failed_count", 0)
    )
    source_errors.extend(
        {"source_record_id": p.get("id"), "error": p.get("detail_error")}
        for p in envelope.get("posts", [])
        if p.get("detail_status") == "error"
    )
    accepted = sum(len(result[n]) for n in DATASETS[:3])
    metadata = {
        k: v
        for k, v in envelope.items()
        if k not in ("posts", "videos", "comments", "responses")
    }
    row = base(
        "bronze_collection_runs",
        metadata,
        run,
        "Run",
        queries=envelope.get("query_runs"),
    )
    row.update(
        started_at=timestamp(envelope.get("started_at")),
        ended_at=timestamp(envelope.get("ended_at", collected)),
        execution_status=(
            "partial"
            if accepted and (errors or source_errors)
            else "failed" if errors or source_errors else "success"
        ),
        records_returned=len(inputs)
        + sum(
            len((payload.get("detail") or {}).get("visible_replies", []))
            for payload, _ in inputs
        ),
        records_accepted=accepted,
        records_rejected=len(errors),
        errors_json=encode(source_errors + errors),
        requests_json=encode(envelope["requests"]) if "requests" in envelope else None,
        stopping_reason=envelope.get(
            "stopping_reason", envelope.get("collection_stopping_reason")
        ),
        ingestion_metadata=encode(
            {"policy_status": "configured" if policy else "unreviewed"}
        ),
    )
    if source == "threads":
        from .acquisition import threads_outcome

        outcome = threads_outcome(envelope)
        row["execution_status"] = (
            "partial"
            if accepted and errors
            else "failed" if errors else outcome["status"]
        )
        metadata["acquisition"] = outcome
        row["raw_payload"] = encode(metadata)
        row["payload_hash"] = digest(metadata)
    add("bronze_collection_runs", row)
    return result, errors


def extract_collection_status(status, *, artifact_uri=None, policies=None):
    """Project existing terminal crawler/worker status; never create source rows."""
    check_secrets(status)
    state = status.get("status", "").lower()
    if state not in ("success", "partial", "failed"):
        raise ValueError("collection status must be terminal")
    source, run = identity(status["source"]), identity(status["run_id"])
    ended = timestamp(
        status.get("finished_at") or status.get("completed_at"), required=True
    )
    metadata = status.get("metrics", {})
    queries = {
        k: status.get(k)
        for k in (
            "seed_query",
            "sort",
            "current_query",
            "last_query",
            "metadata",
            "collection_configuration",
        )
        if status.get(k) is not None
    }
    row = {
        "schema_version": "1",
        "observation_id": digest(["bronze_collection_runs", source, run, run, ended]),
        "source_platform": source,
        "source_record_id": run,
        "record_type": "Run",
        "source_url": None,
        "collected_at": ended,
        "source_created_at": None,
        "source_updated_at": None,
        "collection_run_id": run,
        "query_id": digest(queries) if queries else None,
        "query_metadata": encode(queries),
        "raw_payload": encode(status),
        "payload_hash": digest(status),
        "retention_policy_id": None,
        "retention_expires_at": None,
        "artifact_uri": artifact_uri,
        "ingested_at": None,
        "ingestion_metadata": encode({"status_projection": "existing_runtime_status"}),
        "started_at": timestamp(status.get("started_at")),
        "ended_at": ended,
        "execution_status": state,
        "records_returned": status.get("raw_count", metadata.get("discovered")),
        "records_accepted": status.get("collected_count", metadata.get("accepted")),
        "records_rejected": None,
        "errors_json": encode(
            [{"error": status["error"]}] if status.get("error") else []
        ),
        "requests_json": None,
        "stopping_reason": status.get("stage"),
    }
    row = apply_source_policy(row, policies or {})
    validate_observation("bronze_collection_runs", row)
    result = {name: [] for name in DATASETS}
    result["bronze_collection_runs"] = [row]
    return result, []
