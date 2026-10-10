"""R02A physical record model, explicitly versioned for operational S3 fetches.

All 62 columns retain their meanings. v1 projection and contracts are unchanged.
v2 adds permitted live origin, native language/views/updates and nested replies.
"""

import copy
from datetime import UTC, datetime
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from urllib.parse import urlsplit

from ..sources.crawler.threads.rich_fields import metrics
from . import observation as v1


@lru_cache(maxsize=1)
def schema():
    name = "data/bronze/record-observation.v2.schema.json"
    root = next(
        (
            p / "contracts"
            for p in Path(__file__).resolve().parents
            if (p / "contracts" / name).is_file()
        ),
        None,
    )
    return v1.parse_json(
        (root / name if root else files("dsp_contracts") / name).read_bytes()
    )


def identity(row):
    return "sha256:" + v1.digest(["record-observation.v2", *v1._identity(row)[1:]])


def validate(row):
    v1._validator(schema()).validate(row)
    raw = v1.parse_json(row["raw_payload"])
    if (
        row["observation_id"] != identity(row)
        or row["payload_hash"] != v1.digest(raw)
        or v1.canonical_json(raw) != row["raw_payload"]
    ):
        raise ValueError("v2 native identity/hash mismatch")
    if str(raw.get("id")) != row["source_record_id"] or row[
        "native_record_key"
    ] != "sha256:" + v1.digest(
        ["threads", row["record_type"], row["source_record_id"]]
    ):
        raise ValueError("v2 native source mismatch")
    native = (
        (raw.get("detail") or {}).get("raw_payload") or raw.get("raw_payload") or {}
    )
    values, states = metrics(native)
    if any(row[k] != values[k] for k in values) or row["metric_availability"] != states:
        raise ValueError("v2 metric evidence mismatch")
    uri = urlsplit(row["artifact_uri"])
    if (
        uri.scheme not in ("file", "s3", "s3a")
        or uri.query
        or uri.fragment
        or uri.username
        or uri.password
        or not uri.path.startswith("/")
    ):
        raise ValueError("Unsafe native artifact URI")
    if uri.scheme != "file" and not uri.netloc:
        raise ValueError("S3 bucket required")
    expiry = row["retention_expires_at"]
    if expiry and datetime.fromisoformat(expiry) <= datetime.now(UTC):
        raise PermissionError("Observation retention expired")
    v1._secret_check(raw)
    return row


def project(envelope, *, artifact_uri, artifact_sha256):
    context = copy.deepcopy(envelope[v1.CONTEXT_FIELD])
    for name in ("observed_at", "retention_expires_at"):
        context[name] = v1.timestamp(context[name])
    v1._validator(schema()["$defs"]["context"]).validate(context)
    if context["provenance_scope"] != "FETCH_BATCH":
        raise ValueError("v2 requires recorded fetch scope")
    originals = []
    for index, post in enumerate(envelope["posts"]):
        info = (post.get("raw_payload") or {}).get("text_post_app_info") or {}
        originals.append(
            (
                post,
                f"/posts/{index}",
                None,
                info.get("reply_to_post_id"),
                bool(info.get("is_reply", post.get("is_reply"))),
            )
        )
        replies = (post.get("detail") or {}).get("visible_replies", [])
        root = str(post["id"])
        known = {root} | {str(r["id"]) for r in replies}
        for j, reply in enumerate(replies):
            parent = str(reply.get("reply_to_post_id"))
            if parent not in known or parent == str(reply["id"]):
                raise ValueError("Reply parent missing from captured hierarchy")
            originals.append(
                (
                    reply,
                    f"/posts/{index}/detail/visible_replies/{j}",
                    root,
                    parent,
                    True,
                )
            )
    result = {}
    for raw, pointer, root, parent, is_reply in originals:
        compatible = copy.deepcopy(envelope)
        compatible["posts"], compatible["count"] = [copy.deepcopy(raw)], 1
        if compatible["posts"][0].get("detail"):
            compatible["posts"][0]["detail"]["visible_replies"] = []
        compatible[v1.CONTEXT_FIELD] = {**context, "origin": "SYNTHETIC"}
        row = v1.project_envelope(
            compatible,
            artifact_uri="file:///v2-projection-only",
            artifact_sha256=artifact_sha256,
        )[0]
        native = (
            (raw.get("detail") or {}).get("raw_payload") or raw.get("raw_payload") or {}
        )
        values, states = metrics(native)
        kind = "Comment" if is_reply else "Post"
        row.update(
            schema_version="2",
            artifact_uri=artifact_uri,
            origin=context["origin"],
            record_type=kind,
            content_type=kind,
            raw_payload=v1.canonical_json(raw),
            payload_hash=v1.digest(raw),
            source_payload_path=pointer,
            acquisition_provenance=v1.canonical_json(
                {k: v for k, v in envelope.items() if k != "posts"}
            ),
            native_record_key="sha256:" + v1.digest(["threads", kind, str(raw["id"])]),
            native_post_id=root if is_reply else str(raw["id"]),
            native_comment_id=str(raw["id"]) if is_reply else None,
            parent_content_id=str(parent) if parent else None,
            parent_comment_id=(
                str(parent) if is_reply and root and str(parent) != root else None
            ),
            comment_text_raw=raw.get("text") if is_reply else None,
            language_raw=native.get("language"),
            source_updated_at=v1.timestamp(
                native.get("edited_at", native.get("updated_at"))
            ),
            metric_availability=states,
            legacy_record_id=None,
            **values,
        )
        row["updated_at"] = row["source_updated_at"]
        row["observation_id"] = identity(row)
        validate(row)
        previous = result.get(row["observation_id"])
        if previous and {
            k: v for k, v in previous.items() if k != "source_payload_path"
        } != {k: v for k, v in row.items() if k != "source_payload_path"}:
            raise ValueError("Conflicting fetch observation")
        result[row["observation_id"]] = row
    return [result[k] for k in sorted(result)]
