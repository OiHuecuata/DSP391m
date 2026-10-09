"""Strict immutable processing requests and separate local acknowledgement files.

No scheduling, claim/lease state machine, source admission or publication is
performed here. Persistence uses the existing analytical ownership guard.
"""

import hashlib
import json
import math
import os
import re
import tempfile
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import unquote, urlsplit
from uuid import uuid4

from jsonschema import Draft202012Validator, FormatChecker, ValidationError, validators
from referencing import Registry, Resource

from .analytical_refresh import analytical_ownership, canonical_targets

ROOT = Path(__file__).resolve().parents[2]
ID_PATTERN = re.compile(r"sha256:([0-9a-f]{64})\Z")


def _json_types(value):
    if type(value) is dict:
        if any(type(key) is not str for key in value):
            raise TypeError("JSON object keys must be strings")
        for child in value.values():
            _json_types(child)
    elif type(value) is list:
        for child in value:
            _json_types(child)
    elif type(value) is float:
        if not math.isfinite(value):
            raise ValueError("Nonfinite JSON number")
    elif type(value) not in (str, int, bool, type(None)):
        raise TypeError("Unsupported JSON value type")


def canonical_json(value):
    """Typed UTF-8 JSON: sorted object keys, compact separators, no coercion."""
    _json_types(value)
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON property")
        result[key] = value
    return result


def _nonfinite(_value):
    raise ValueError("Nonfinite JSON number")


def parse_json(raw):
    value = json.loads(raw, object_pairs_hook=_pairs, parse_constant=_nonfinite)
    _json_types(value)
    return value


def content_hash(value):
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _identity(value):
    return "sha256:" + content_hash(value)


def _copy(value):
    return parse_json(canonical_json(value))


def _timestamp(value):
    if value is None:
        return None
    when = datetime.fromisoformat(value) if type(value) is str else value
    if not isinstance(when, datetime) or when.utcoffset() is None:
        raise ValueError("Timestamp requires an explicit timezone")
    return (
        when.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    )


@lru_cache(maxsize=1)
def _schemas():
    docs = {}
    for name in ("manifest", "ack"):
        path = ROOT / f"contracts/orchestration/processing-{name}.v1.schema.json"
        document = parse_json(path.read_bytes())
        Draft202012Validator.check_schema(document)
        docs[name] = document
    registry = Registry().with_resources(
        (doc["$id"], Resource.from_contents(doc)) for doc in docs.values()
    )
    checker = Draft202012Validator.TYPE_CHECKER.redefine(
        "integer", lambda _c, v: type(v) is int and -(2**63) <= v < 2**63
    ).redefine("number", lambda _c, v: type(v) in (int, float) and math.isfinite(v))
    strict = validators.extend(Draft202012Validator, type_checker=checker)
    return {
        name: strict(doc, registry=registry, format_checker=FormatChecker())
        for name, doc in docs.items()
    }


def _schema(value, kind):
    _json_types(value)
    try:
        _schemas()[kind].validate(value)
    except ValidationError as error:
        # Do not echo rejected payload values, URLs or exception messages.
        raise ValueError(f"Invalid {kind} contract: {error.validator}") from None


def _uri(value):
    parsed = urlsplit(value)
    decoded = unquote(parsed.path)
    if (
        parsed.scheme not in ("file", "s3a", "https")
        or parsed.query
        or parsed.fragment
        or parsed.username
        or parsed.password
        or ".." in decoded.split("/")
        or "\\" in decoded
        or any(ord(c) < 32 or ord(c) == 127 for c in value + decoded)
        or not decoded.startswith("/")
        or (parsed.scheme == "file" and parsed.netloc not in ("", "localhost"))
        or (parsed.scheme != "file" and not parsed.netloc)
    ):
        raise ValueError("Unsafe input/output reference URI")


def _reference(ref):
    if ref["kind"] != "kafka_ack":
        _uri(ref["artifact_uri"] if "artifact_uri" in ref else ref["uri"])
    if ref["kind"] == "kafka_ack":
        if ref["last_offset"] < ref["first_offset"]:
            raise ValueError("Kafka acknowledgement range is reversed")
        if ref["acknowledged_count"] > ref["last_offset"] - ref["first_offset"] + 1:
            raise ValueError("Kafka acknowledged count exceeds its range")
    if (
        ref["kind"] == "observations"
        and not ref.get("source_record_id")
        and not ref.get("observation_range")
    ):
        raise ValueError("Observation reference needs a native ID or range")


def target_reference(config, *, include_group=True):
    targets = canonical_targets(config, include_group=include_group)
    return targets.identity, {
        "base_uri": targets.base.as_uri(),
        "resources": list(targets.resources),
        "include_group": include_group,
    }


def _check_targets(value):
    fields = {"base": value["targets"]["base_uri"]}
    warehouse = None
    for resource in value["targets"]["resources"]:
        if resource.startswith("delta:"):
            fields["path_" + str(len(fields))] = resource.removeprefix("delta:")
        elif resource.startswith("clickhouse:"):
            if warehouse is not None:
                raise ValueError("Multiple serving target declarations")
            parts = parse_json(resource.removeprefix("clickhouse:"))
            if (
                type(parts) is not list
                or len(parts) != 3
                or type(parts[0]) is not str
                or "@" in parts[0]
            ):
                raise ValueError("Invalid serving target declaration")
            warehouse = SimpleNamespace(host=parts[0], port=parts[1], database=parts[2])
    config = SimpleNamespace(
        spark=SimpleNamespace(paths=SimpleNamespace(**fields)), warehouse=warehouse
    )
    identity, expected = target_reference(
        config, include_group=value["targets"]["include_group"]
    )
    if identity != value["target_identity"] or expected != value["targets"]:
        raise ValueError("Analytical targets are not canonical or identity disagrees")


def _logical_request(value):
    return {
        name: value[name]
        for name in (
            "manifest_version",
            "input_order",
            "inputs",
            "ingestion_refs",
            "target_identity",
            "processing_spec_hash",
            "acquisition",
            "readiness",
        )
    }


def validate_manifest(value):
    _schema(value, "manifest")
    _check_targets(value)
    refs = value["inputs"] + value["ingestion_refs"]
    if value["input_order"] == "UNORDERED":
        for name in ("inputs", "ingestion_refs"):
            if value[name] != sorted(value[name], key=canonical_json):
                raise ValueError("Unordered references must be canonically sorted")
    runs = value["acquisition"]["runs"]
    keys = [(r["source_platform"], r["collection_run_id"]) for r in runs]
    if len(set(keys)) != len(keys) or runs != sorted(runs, key=canonical_json):
        raise ValueError("Acquisition runs must be unique and canonically sorted")
    outcomes = {r["outcome"] for r in runs}
    expected = next(iter(outcomes)) if len(outcomes) == 1 else "partial"
    if value["acquisition"]["outcome"] != expected:
        raise ValueError("Acquisition outcome disagrees with source runs")
    counts = [r["accepted_records"] for r in runs]
    total = sum(counts) if all(c is not None for c in counts) else None
    if value["acquisition"]["accepted_records"] != total:
        raise ValueError("Acquisition count disagrees with available source counts")
    for ref in refs:
        _reference(ref)
        if (ref["source_platform"], ref["collection_run_id"]) not in keys:
            raise ValueError("Input lineage is absent from acquisition runs")
    if value["readiness"] == "COLLECTION_FAILED" and expected != "failed":
        raise ValueError("COLLECTION_FAILED requires failed acquisition")
    if (
        expected == "failed"
        and value["readiness"] == "PARTIAL_READY"
        and (
            not refs
            or not (total or any((r.get("record_count") or 0) > 0 for r in refs))
        )
    ):
        raise ValueError("Failed acquisition has no known usable input")
    if value["processing_spec_hash"] != _identity(value["processing_spec"]):
        raise ValueError("Processing specification hash disagrees")
    if value["manifest_id"] != _identity(_logical_request(value)):
        raise ValueError("Logical manifest identity disagrees")
    if value["payload_sha256"] != content_hash(
        {k: v for k, v in value.items() if k != "payload_sha256"}
    ):
        raise ValueError("Manifest payload checksum disagrees")
    return _copy(value)


def build_manifest(
    *,
    inputs,
    acquisition,
    processing_spec,
    config,
    readiness,
    created_at,
    ingestion_refs=(),
    input_order="UNORDERED",
    metadata=None,
    include_group=True,
):
    inputs, ingestion_refs = _copy(list(inputs)), _copy(list(ingestion_refs))
    if input_order == "UNORDERED":
        inputs.sort(key=canonical_json)
        ingestion_refs.sort(key=canonical_json)
    acquisition, processing_spec = _copy(acquisition), _copy(processing_spec)
    acquisition["runs"] = sorted(acquisition["runs"], key=canonical_json)
    target_id, targets = target_reference(config, include_group=include_group)
    value = {
        "manifest_version": "1",
        "manifest_id": "",
        "processing_spec_hash": _identity(processing_spec),
        "payload_sha256": "",
        "input_order": input_order,
        "inputs": inputs,
        "ingestion_refs": ingestion_refs,
        "acquisition": acquisition,
        "target_identity": target_id,
        "targets": targets,
        "processing_spec": processing_spec,
        "readiness": readiness,
        "created_at": _timestamp(created_at),
        "metadata": {} if metadata is None else _copy(metadata),
    }
    value["manifest_id"] = _identity(_logical_request(value))
    value["payload_sha256"] = content_hash(
        {k: v for k, v in value.items() if k != "payload_sha256"}
    )
    return validate_manifest(value)


def artifact_reference(
    path,
    *,
    source_platform,
    collection_run_id,
    schema_version,
    record_count=None,
    policy_reference=None,
):
    """Hash existing local bytes without changing their serialization or run ID."""
    path = Path(path).resolve()
    return {
        "kind": "artifact",
        "source_platform": source_platform,
        "collection_run_id": collection_run_id,
        "artifact_uri": path.as_uri(),
        "content_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "artifact_schema_version": schema_version,
        "record_count": record_count,
        "policy_reference": policy_reference,
    }


def new_attempt_id():
    return "attempt:" + uuid4().hex


def safe_error(_exception=None, *, code="UNKNOWN_ERROR", retryable=False):
    """Never retain exception text, headers, raw response bodies or URLs."""
    return {"code": code, "retryable": retryable}


def build_ack(
    manifest,
    *,
    attempt_id,
    stage,
    outcome,
    started_at=None,
    completed_at=None,
    input_refs=(),
    output_refs=(),
    error=None,
    owner_reference=None,
    publication_generation_reference=None,
):
    manifest = validate_manifest(manifest)
    value = {
        "ack_version": "1",
        "ack_id": "",
        "manifest_id": manifest["manifest_id"],
        "attempt_id": attempt_id,
        "stage": stage,
        "outcome": outcome,
        "collection_outcome": manifest["acquisition"]["outcome"],
        "started_at": _timestamp(started_at),
        "completed_at": _timestamp(completed_at),
        "input_refs": _copy(list(input_refs)),
        "output_refs": _copy(list(output_refs)),
        "error": None if error is None else _copy(error),
        "owner_reference": None if owner_reference is None else _copy(owner_reference),
        "publication_generation_reference": publication_generation_reference,
    }
    value["ack_id"] = _identity({k: v for k, v in value.items() if k != "ack_id"})
    value = validate_ack(value)
    _ack_lineage(value, manifest)
    return value


def validate_ack(value):
    _schema(value, "ack")
    if (
        value["started_at"] is not None
        and value["completed_at"] is not None
        and value["completed_at"] < value["started_at"]
    ):
        raise ValueError("Acknowledgement completion precedes start")
    for ref in value["input_refs"]:
        _reference(ref)
    for ref in value["output_refs"]:
        _uri(ref["uri"])
    if value["ack_id"] != _identity({k: v for k, v in value.items() if k != "ack_id"}):
        raise ValueError("Acknowledgement identity disagrees")
    return _copy(value)


def _ack_lineage(value, manifest):
    if (
        value["manifest_id"] != manifest["manifest_id"]
        or value["collection_outcome"] != manifest["acquisition"]["outcome"]
    ):
        raise ValueError("Acknowledgement cannot rewrite acquisition lineage")
    declared = {
        canonical_json(r) for r in manifest["inputs"] + manifest["ingestion_refs"]
    }
    if any(canonical_json(ref) not in declared for ref in value["input_refs"]):
        raise ValueError("Acknowledged inputs are absent from the manifest")
    # Owner references are historical facts, never capabilities. A legitimate
    # outer owner may cover a superset of this request's physical targets.


def _safe_chain(path):
    for part in (path, *path.parents):
        if part.is_symlink():
            raise ValueError("Manifest state paths cannot contain symlinks")


def _directory_sync(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _directory(path):
    _safe_chain(path)
    # Also flush existing ancestors: a previous failed mkdir/fsync may have
    # left visible directory entries that need reconciliation on retry.
    if path.parent != path:
        _directory(path.parent)
    if path.exists():
        if not path.is_dir():
            raise ValueError("Manifest directory is not a directory")
        _directory_sync(path.parent)
        return
    try:
        path.mkdir(mode=0o700)
    except FileExistsError:
        _safe_chain(path)
        if not path.is_dir():
            raise ValueError("Manifest directory is not a directory") from None
    _directory_sync(path.parent)


def _id_hex(identity):
    match = ID_PATTERN.fullmatch(identity) if type(identity) is str else None
    if match is None:
        raise ValueError("Unsafe record identity")
    return match[1]


class LocalProcessingStore:
    """Immutable control metadata only; every publication uses existing ownership.

    Successful writes request file and directory fsync on the local filesystem.
    If fsync fails after link publication, the record may be visible: reload and
    retry identical content to reconcile. This is not hardware crash-proof or a
    multi-file transaction. The state root must be trusted, not renamed by peers.
    """

    def __init__(self, root, config, *, include_group=True):
        path = Path(root).expanduser()
        if ".." in path.parts or "://" in str(root):
            raise ValueError("Unsafe manifest state root")
        self.root = path.absolute()
        _safe_chain(self.root)
        self.config, self.include_group = config, include_group
        target_id, targets = target_reference(config, include_group=include_group)
        self.target_id, self.targets = target_id, targets
        base = canonical_targets(config, include_group=include_group).base
        paths = [base] + [
            Path(unquote(urlsplit(r.removeprefix("delta:")).path))
            for r in targets["resources"]
            if r.startswith("delta:")
        ]
        if any(self.root == p or self.root.is_relative_to(p) for p in paths):
            raise ValueError("Control state must be outside analytical storage")

    def _target(self, manifest):
        identity, targets = target_reference(
            self.config, include_group=self.include_group
        )
        if (identity, targets) != (self.target_id, self.targets) or (
            manifest["target_identity"],
            manifest["targets"],
        ) != (identity, targets):
            raise ValueError("Manifest does not match this store's analytical targets")

    def _read(self, path, kind):
        _safe_chain(path)
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "rb") as file:
            value = parse_json(file.read())
        return validate_manifest(value) if kind == "manifest" else validate_ack(value)

    def _publish(self, path, value, kind):
        _directory(path.parent)
        descriptor, name = tempfile.mkstemp(
            prefix="." + path.stem + ".", suffix=".tmp", dir=path.parent
        )
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, "wb") as file:
                file.write((canonical_json(value) + "\n").encode("utf-8"))
                file.flush()
                os.fsync(file.fileno())
            try:
                os.link(temporary, path, follow_symlinks=False)
            except FileExistsError:
                existing = self._read(path, kind)
                if canonical_json(existing) != canonical_json(value):
                    raise FileExistsError(
                        "Conflicting immutable record already exists"
                    ) from None
                # Retry also reconciles an earlier visible-but-unacknowledged link.
                with os.fdopen(
                    os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb"
                ) as file:
                    os.fsync(file.fileno())
            _directory_sync(path.parent)
        finally:
            temporary.unlink(missing_ok=True)
        return path

    def save_manifest(self, manifest, *, ownership=None):
        value = validate_manifest(manifest)
        with analytical_ownership(
            self.config, ownership=ownership, include_group=self.include_group
        ):
            self._target(value)
            path = self.root / "manifests" / (_id_hex(value["manifest_id"]) + ".json")
            return self._publish(path, value, "manifest")

    def load_manifest(self, manifest_id):
        path = self.root / "manifests" / (_id_hex(manifest_id) + ".json")
        value = self._read(path, "manifest")
        if value["manifest_id"] != manifest_id:
            raise ValueError("Saved manifest path and identity disagree")
        self._target(value)
        return value

    def append_ack(self, acknowledgement, *, ownership=None):
        value = validate_ack(acknowledgement)
        with analytical_ownership(
            self.config, ownership=ownership, include_group=self.include_group
        ):
            manifest = self.load_manifest(value["manifest_id"])
            _ack_lineage(value, manifest)
            path = (
                self.root
                / "acks"
                / _id_hex(value["manifest_id"])
                / (_id_hex(value["ack_id"]) + ".json")
            )
            return self._publish(path, value, "ack")

    def load_acknowledgements(self, manifest_id):
        manifest = self.load_manifest(manifest_id)
        directory = self.root / "acks" / _id_hex(manifest_id)
        _safe_chain(directory)
        if not directory.exists():
            return []
        result = []
        for path in sorted(directory.glob("*.json")):
            value = self._read(path, "ack")
            if path.stem != _id_hex(value["ack_id"]):
                raise ValueError("Saved acknowledgement lineage or identity disagrees")
            _ack_lineage(value, manifest)
            result.append(value)
        return result
