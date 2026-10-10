"""Native Bronze projection; no I/O, collection, clocks or analytical inference.

Original envelope bytes are archived separately. Record payloads preserve all
captured JSON values, not the original whitespace of individual JSON subtrees.
Run-envelope scope is explicit when a legacy collector merged fetches/queries.
"""

import copy
import hashlib
import json
import math
import re
from datetime import UTC, datetime
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from urllib.parse import urlsplit

from jsonschema import Draft202012Validator, FormatChecker, validators

METRICS = (
    "view_count",
    "like_count",
    "comment_count",
    "share_count",
    "repost_count",
    "quote_count",
    "reply_count",
)
CONTEXT_FIELD = "native_observation_context"


def _json_value(value):
    if value is None or type(value) in (str, bool):
        return
    if type(value) is int and -(2**63) <= value < 2**63:
        return
    if type(value) is float and math.isfinite(value):
        return
    if type(value) is list:
        for child in value:
            _json_value(child)
        return
    if type(value) is dict and all(type(k) is str for k in value):
        for child in value.values():
            _json_value(child)
        return
    raise ValueError("Observation input must contain finite, bounded JSON values")


def canonical_json(value):
    _json_value(value)
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def digest(value):
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def parse_json(raw):
    def pairs(items):
        result = {}
        for k, v in items:
            if k in result:
                raise ValueError("Duplicate observation JSON property")
            result[k] = v
        return result

    def constant(_value):
        raise ValueError("Nonfinite observation JSON")

    value = json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
    _json_value(value)
    return value


@lru_cache(maxsize=1)
def observation_schema():
    relative = "data/bronze/record-observation.v1.schema.json"
    root = next(
        (
            p / "contracts"
            for p in Path(__file__).resolve().parents
            if (p / "contracts" / relative).is_file()
        ),
        None,
    )
    path = root / relative if root else files("dsp_contracts") / relative
    schema = parse_json(path.read_bytes())
    Draft202012Validator.check_schema(schema)
    return schema


def _validator(schema):
    checker = Draft202012Validator.TYPE_CHECKER.redefine(
        "integer",
        lambda _c, v: type(v) is int and -(2**63) <= v < 2**63,
    ).redefine("number", lambda _c, v: type(v) in (int, float) and math.isfinite(v))
    return validators.extend(Draft202012Validator, type_checker=checker)(
        schema, format_checker=FormatChecker()
    )


def timestamp(value):
    if value is None:
        return None
    if type(value) in (int, float):
        value = datetime.fromtimestamp(value, UTC)
    elif type(value) is str:
        value = datetime.fromisoformat(value)
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("Observation timestamp requires explicit timezone")
    return (
        value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    )


def run_envelope_context(
    *,
    permission_reference,
    retention_policy_id,
    origin="SYNTHETIC",
    retention_expires_at=None,
):
    """Explicit assertion for fixture or independently permitted saved replay.

    Never infer platform eligibility or permission from artifact accessibility.
    """
    return validate_context(
        {
            "origin": origin,
            "permission_reference": permission_reference,
            "retention_policy_id": retention_policy_id,
            "retention_expires_at": timestamp(retention_expires_at),
            "provenance_scope": "RUN_ENVELOPE",
            "fetch_batch_id": "run-envelope",
            "query_id": None,
            "observed_at": None,
        }
    )


def validate_context(context):
    _validator(observation_schema()["$defs"]["context"]).validate(context)
    if context["provenance_scope"] == "RUN_ENVELOPE":
        if (context["fetch_batch_id"], context["query_id"], context["observed_at"]) != (
            "run-envelope",
            None,
            None,
        ):
            raise ValueError(
                "Merged run envelope cannot assert a query or fetch timestamp"
            )
    elif context["observed_at"] is None:
        raise ValueError("FETCH_BATCH requires its recorded observation timestamp")
    if context["query_id"] is not None and not context["query_id"].strip():
        raise ValueError("Empty native query identity")
    for name in ("permission_reference", "retention_policy_id", "fetch_batch_id"):
        if not context[name].strip():
            raise ValueError("Empty observation provenance")
    return copy.deepcopy(context)


def prepare_envelope(envelope, context=None):
    """Add explicit opt-in metadata without modifying or relabeling native data."""
    value = copy.deepcopy(dict(envelope))
    existing = value.get(CONTEXT_FIELD)
    if context is not None:
        context = validate_context(context)
        if existing is not None and existing != context:
            raise ValueError("Replay observation context conflicts with acquisition")
        value[CONTEXT_FIELD] = context
    elif existing is not None:
        validate_context(existing)
    return value


def _secret_check(value):
    """Minimal credential guard; full source field policy remains R02B."""
    denied = {
        "password",
        "passwd",
        "cookie",
        "cookies",
        "authorization",
        "accesstoken",
        "refreshtoken",
        "apikey",
        "clientsecret",
    }
    if isinstance(value, dict):
        for key, child in value.items():
            name = re.sub(r"[^a-z0-9]", "", key.casefold())
            if name in denied and child not in (None, "", [], {}):
                raise ValueError("Credential field denied in native Bronze input")
            _secret_check(child)
    elif isinstance(value, list):
        for child in value:
            _secret_check(child)


def validate_native_input(envelope, path):
    """Optional collector admission before raw artifact publication."""
    if CONTEXT_FIELD not in envelope:
        return
    context = validate_context(envelope[CONTEXT_FIELD])
    if context["retention_expires_at"] and datetime.fromisoformat(
        context["retention_expires_at"]
    ) <= datetime.now(UTC):
        raise ValueError("Native replay retention expired")
    project_envelope(
        envelope,
        artifact_uri=Path(path).resolve().as_uri(),
        artifact_sha256=digest(envelope),
    )


def native_batch_metadata(envelope, path):
    if CONTEXT_FIELD not in envelope:
        return {}
    raw = Path(path).read_bytes()
    if parse_json(raw) != envelope:
        raise ValueError("Saved native acquisition artifact disagrees")
    return {
        "native_bronze_required": True,
        "native_bronze_artifact": {
            "artifact_uri": Path(path).resolve().as_uri(),
            "content_sha256": hashlib.sha256(raw).hexdigest(),
        },
    }


def _url(value):
    if value is None:
        return None
    p = urlsplit(value)
    if p.scheme not in ("http", "https") or not p.netloc or p.username or p.password:
        raise ValueError("Invalid native source URL")
    return value


def _identity(record):
    return [
        "record-observation.v1",
        record["source_platform"],
        record["record_type"],
        record["source_record_id"],
        record["collection_run_id"],
        record["query_id"],
        record["fetch_batch_id"],
        record["provenance_scope"],
    ]


def validate_observation(record, *, schema=None):
    _json_value(record)
    _validator(schema or observation_schema()).validate(record)
    raw = parse_json(record["raw_payload"])
    if not isinstance(raw, dict) or canonical_json(raw) != record["raw_payload"]:
        raise ValueError("Raw record must be a canonical JSON object")
    if digest(raw) != record["payload_hash"]:
        raise ValueError("Native payload hash disagrees")
    if (
        str(
            raw.get("id")
            or (raw.get("url") if record["source_platform"] == "pbplus" else "")
        )
        != record["source_record_id"]
    ):
        raise ValueError("Native identity disagrees with original payload")
    allowed = {
        "pbplus": ("Venue",),
        "threads": ("Post", "Comment"),
        "youtube": ("Video", "Comment"),
    }
    if record["record_type"] not in allowed[record["source_platform"]]:
        raise ValueError("Native record type disagrees with source")
    if "sha256:" + digest(_identity(record)) != record["observation_id"]:
        raise ValueError("Native observation identity disagrees")
    if (
        "sha256:"
        + digest(
            [
                record["source_platform"],
                record["record_type"],
                record["source_record_id"],
            ]
        )
        != record["native_record_key"]
    ):
        raise ValueError("Native content identity disagrees")
    for name in ("acquisition_provenance", "query_provenance"):
        if canonical_json(parse_json(record[name])) != record[name]:
            raise ValueError("Noncanonical observation provenance")
    if record["provenance_scope"] == "RUN_ENVELOPE":
        if (
            record["fetch_batch_id"] != "run-envelope"
            or record["query_id"] is not None
            or record["observed_at"] != record["collected_at"]
            or record["observation_time_basis"] != "RUN_COMPLETION"
        ):
            raise ValueError("Invented fetch provenance for run envelope")
    elif record["observation_time_basis"] != "FETCH_COMPLETION":
        raise ValueError("Fetch observation time basis disagrees")
    for name in METRICS:
        if (record[name] is not None) != (
            record["metric_availability"][name] == "AVAILABLE"
        ):
            raise ValueError("Native metric availability disagrees with value")
    measured = {"metric_availability": {}}
    _metrics(measured, raw, record["source_platform"], record["record_type"])
    if any(
        record[name] != measured[name] for name in (*METRICS, "metric_availability")
    ):
        raise ValueError("Native metrics disagree with raw source evidence")
    _secret_check(raw)
    _secret_check(parse_json(record["acquisition_provenance"]))
    _url(record["source_url"])
    uri = urlsplit(record["artifact_uri"])
    if (
        uri.scheme != "file"
        or uri.netloc not in ("", "localhost")
        or not uri.path.startswith("/")
        or uri.query
        or uri.fragment
    ):
        raise ValueError(
            "This native ingestion increment requires a local immutable artifact"
        )
    return record


def _counter(value):
    if type(value) is str and value.isascii() and value.isdecimal():
        value = int(value)
    return value if type(value) is int and 0 <= value < 2**63 else None


def _metrics(row, raw, platform, kind):
    if platform == "youtube":
        native = (
            raw.get("snippet", {}) if kind == "Comment" else raw.get("statistics", {})
        )
        names = (
            {"like_count": "likeCount"}
            if kind == "Comment"
            else {
                "view_count": "viewCount",
                "like_count": "likeCount",
                "comment_count": "commentCount",
            }
        )
    elif platform == "threads":
        native = raw.get("raw_payload")
        if native is None and isinstance(raw.get("detail"), dict):
            native = raw["detail"].get("raw_payload")
        if native is not None:
            info = native.get("text_post_app_info") or {}
            native = {
                "like_count": native.get("like_count"),
                "unavailable": native.get("like_and_view_counts_disabled"),
                "reply_count": info.get("direct_reply_count"),
                "repost_count": info.get("repost_count"),
                "quote_count": info.get("quote_count"),
            }
            names = {
                n: n
                for n in ("like_count", "reply_count", "repost_count", "quote_count")
            }
        else:
            # Legacy projections default missing metrics to zero. Never claim
            # an observed zero without native evidence or explicit availability.
            native = raw.get("engagement", raw)
            names = {
                n: n
                for n in ("like_count", "reply_count", "repost_count", "quote_count")
            }
            row["_legacy_metrics"] = True
    else:
        native, names = {}, {}
    for name in METRICS:
        value = native.get(names.get(name))
        count = _counter(value)
        state = (
            "NOT_PROVIDED"
            if value is None
            else "INVALID" if count is None else "AVAILABLE"
        )
        if (
            platform == "threads"
            and name == "like_count"
            and native.get("unavailable") is True
        ):
            state, count = "UNAVAILABLE", None
        elif row.get("_legacy_metrics") and count == 0:
            state, count = "UNKNOWN", None
        row[name] = count
        row["metric_availability"][name] = state
    row.pop("_legacy_metrics", None)


def project_envelope(envelope, *, artifact_uri, artifact_sha256, context=None):
    """Return deterministic native rows, preserving captured source/run/parents.

    A malformed source identity or conflicting duplicate rejects the entire
    native batch; it is not silently discarded or given a synthetic source ID.
    """
    envelope = prepare_envelope(envelope, context)
    if CONTEXT_FIELD not in envelope:
        raise ValueError(
            "Native Bronze requires explicit permitted replay or synthetic context"
        )
    context = validate_context(envelope[CONTEXT_FIELD])
    _json_value(envelope)
    _secret_check(envelope)
    platform = envelope["source"]
    if (
        platform not in ("pbplus", "threads", "youtube")
        or not isinstance(envelope.get("run_id"), str)
        or not envelope["run_id"].strip()
    ):
        raise ValueError("Native source/acquisition run required")
    at = timestamp(envelope["collected_at"])
    metadata = {
        k: v
        for k, v in envelope.items()
        if k not in ("posts", "videos", "comments", "responses", CONTEXT_FIELD)
    }
    payloads = []
    if platform == "youtube":
        payloads += [
            (r, "Video", f"/videos/{i}", None) for i, r in enumerate(envelope["videos"])
        ]
        payloads += [
            (r["payload"], "Comment", f"/comments/{i}/payload", r["video_id"])
            for i, r in enumerate(envelope["comments"])
        ]
    else:
        if type(envelope.get("count")) is not int or envelope["count"] != len(
            envelope["posts"]
        ):
            raise ValueError("Native envelope count disagrees")
        for i, r in enumerate(envelope["posts"]):
            payloads.append(
                (r, "Venue" if platform == "pbplus" else "Post", f"/posts/{i}", None)
            )
            detail = r.get("detail") or {}
            for j, reply in enumerate(detail.get("visible_replies", [])):
                parent = str(r["id"])
                if str(reply.get("reply_to_post_id")) != parent:
                    raise ValueError("Captured Threads reply parent disagrees")
                payloads.append(
                    (reply, "Comment", f"/posts/{i}/detail/visible_replies/{j}", parent)
                )
    result = {}
    for raw, kind, pointer, parent in payloads:
        if not isinstance(raw, dict):
            raise TypeError("Native payload is not an object")
        identity = raw.get("id") or (raw.get("url") if platform == "pbplus" else None)
        if platform == "threads":
            identity = str(identity) if type(identity) in (str, int) else ""
            if not re.fullmatch(r"[1-9][0-9]*", identity):
                raise ValueError("Missing native Threads identity")
            for native in (
                raw.get("raw_payload"),
                (raw.get("detail") or {}).get("raw_payload"),
            ):
                if (
                    native is not None
                    and str(native.get("pk", native.get("id"))) != identity
                ):
                    raise ValueError("Threads native/detail identity disagrees")
        elif platform == "youtube":
            if not isinstance(identity, str) or not re.fullmatch(
                r"[A-Za-z0-9_-]+", identity
            ):
                raise ValueError("Missing native YouTube identity")
            if kind == "Comment" and (
                not isinstance(parent, str)
                or not re.fullmatch(r"[A-Za-z0-9_-]+", parent)
            ):
                raise ValueError("Missing native parent video")
        else:
            identity = _url(identity)
            if identity is None:
                raise ValueError("Missing native PBPlus listing identity")
        row = {name: None for name in observation_schema()["properties"]}
        row.update(
            schema_version="1",
            source_platform=platform,
            source_record_id=identity,
            record_type=kind,
            collection_run_id=envelope["run_id"],
            collected_at=at,
            artifact_uri=artifact_uri,
            artifact_sha256=artifact_sha256,
            source_payload_path=pointer,
            raw_payload=canonical_json(raw),
            payload_hash=digest(raw),
            acquisition_provenance=canonical_json(metadata),
            query_provenance=canonical_json(
                {
                    "query_contexts": raw.get("query_contexts", []),
                    "matched_queries": raw.get("matched_queries", []),
                    "video_queries": envelope.get("video_queries", {}).get(
                        parent or identity, []
                    ),
                }
            ),
            **{k: v for k, v in context.items() if k != "observed_at"},
        )
        row["observed_at"] = context["observed_at"] or at
        row["observation_time_basis"] = (
            "RUN_COMPLETION"
            if context["provenance_scope"] == "RUN_ENVELOPE"
            else "FETCH_COMPLETION"
        )
        row["native_record_key"] = "sha256:" + digest([platform, kind, identity])
        row["observation_id"] = "sha256:" + digest(_identity(row))
        row["native_content_id"] = identity
        row["content_type"] = kind
        row["metric_scope"] = (
            "SOURCE_RECORD_CUMULATIVE" if platform != "pbplus" else None
        )
        row["metric_availability"] = {m: "NOT_PROVIDED" for m in METRICS}
        snippet = raw.get("snippet", {})
        if platform == "pbplus":
            row.update(
                source_url=_url(raw.get("url")) or identity,
                source_venue_id=identity,
                venue_name_raw=raw.get("court_name"),
                address_raw=raw.get("address"),
                court_count_raw=(
                    None
                    if raw.get("number_of_courts") is None
                    else str(raw["number_of_courts"])
                ),
                latitude_raw=raw.get("latitude"),
                longitude_raw=raw.get("longitude"),
                price_raw=raw.get("price_raw", raw.get("price")),
                amenities_raw=(
                    canonical_json(raw["services"]) if "services" in raw else None
                ),
                opening_hours_raw=(
                    canonical_json(raw["opening_hours"])
                    if "opening_hours" in raw
                    else None
                ),
                venue_status_raw=raw.get("venue_status"),
                description_raw=raw.get("description"),
                legacy_record_id="pbplus:" + identity,
            )
        elif platform == "youtube":
            comment = kind == "Comment"
            row.update(
                source_url="https://www.youtube.com/watch?v="
                + (parent or identity)
                + (f"&lc={identity}" if comment else ""),
                native_post_id=parent if comment else identity,
                native_comment_id=identity if comment else None,
                parent_content_id=snippet.get("parentId") or parent,
                parent_comment_id=snippet.get("parentId"),
                is_reply=bool(snippet.get("parentId")) if comment else None,
                title_raw=None if comment else snippet.get("title"),
                description_raw=None if comment else snippet.get("description"),
                comment_text_raw=(
                    snippet.get("textOriginal", snippet.get("textDisplay"))
                    if comment
                    else None
                ),
                text_raw=(
                    snippet.get("textOriginal", snippet.get("textDisplay"))
                    if comment
                    else None
                ),
                language_raw=snippet.get(
                    "defaultAudioLanguage", snippet.get("defaultLanguage")
                ),
                source_created_at=timestamp(snippet.get("publishedAt")),
                source_updated_at=timestamp(snippet.get("updatedAt")),
                legacy_record_id=(
                    ("youtube-comment:" if comment else "youtube:") + identity
                    if not snippet.get("parentId")
                    else None
                ),
            )
        else:
            detail = raw.get("detail") or {}
            info = (raw.get("raw_payload") or detail.get("raw_payload") or {}).get(
                "text_post_app_info"
            ) or {}
            row.update(
                source_url=_url(detail.get("url") or raw.get("url")),
                native_post_id=parent if kind == "Comment" else identity,
                native_comment_id=identity if kind == "Comment" else None,
                parent_content_id=info.get("reply_to_post_id") or parent,
                parent_comment_id=None,
                is_reply=True if kind == "Comment" else raw.get("is_reply"),
                text_raw=detail.get("text", raw.get("text")),
                comment_text_raw=raw.get("text") if kind == "Comment" else None,
                language_raw=(raw.get("relevance") or {}).get("language_hint"),
                source_created_at=timestamp(
                    detail.get("taken_at", raw.get("taken_at"))
                ),
                legacy_record_id="threads:" + identity if kind != "Comment" else None,
            )
        row["published_at"], row["updated_at"] = (
            row["source_created_at"],
            row["source_updated_at"],
        )
        row["source_permalink"] = row["source_url"]
        _metrics(row, raw, platform, kind)
        validate_observation(row)
        previous = result.get(row["observation_id"])
        if previous is not None:
            if {k: v for k, v in previous.items() if k != "source_payload_path"} != {
                k: v for k, v in row.items() if k != "source_payload_path"
            }:
                raise ValueError("Conflicting native observation identity inside batch")
        else:
            result[row["observation_id"]] = row
    return [result[k] for k in sorted(result)]
