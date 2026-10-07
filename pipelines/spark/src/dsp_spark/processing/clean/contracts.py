"""Limited contract-to-StructType mapping and distributed structural validation.

Canonical timestamps stay strings so precision and original values survive.
Only lakehouse/Kafka timestamps use Spark TimestampType. No canonicalization.
"""

import hashlib
import json
import math
import re
from datetime import datetime
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from urllib.parse import urldefrag, urljoin

from jsonschema import Draft202012Validator, FormatChecker, ValidationError, validators
from pyspark.sql import types as T
from referencing import Registry, Resource

CONTRACT_FILES = {
    "location_market_signal_snapshot": "data/gold/location-market-signal-snapshot.v1.schema.json",
    "location_reference": "reference/vietnam-locations.v1.schema.json",
    "court_supply_by_location": "data/gold/court-supply-by-location.v1.schema.json",
    "social": "data/silver/social-post.v1.schema.json",
    "social_activity_daily": "data/gold/social-activity-daily.v1.schema.json",
    "court": "data/silver/court.v1.schema.json",
    "event": "kafka/social-post-event.v1.schema.json",
    "threads_bronze": "data/bronze/threads-crawl.v1.schema.json",
    "pbplus_bronze": "data/bronze/pbplus-crawl.v1.schema.json",
}


@lru_cache(maxsize=1)
def contract_documents() -> str:
    # Source checkouts use authoritative repository files. The build process
    # includes byte-identical copies as wheel resources for managed execution.
    root = next(
        (
            parent / "contracts"
            for parent in Path(__file__).resolve().parents
            if (parent / "contracts/data/silver/social-post.v1.schema.json").is_file()
        ),
        None,
    )
    resources = root if root is not None else files("dsp_contracts")
    return json.dumps(
        {
            name: json.loads((resources / filename).read_text(encoding="utf-8"))
            for name, filename in CONTRACT_FILES.items()
        }
    )


def _documents(serialized: str):
    # A virtual stable URI avoids executor filesystem access and remote retrieval.
    docs = json.loads(serialized)
    return {
        "file:///dsp-contracts/"
        + CONTRACT_FILES[name]: {
            **schema,
            "$id": "file:///dsp-contracts/" + CONTRACT_FILES[name],
        }
        for name, schema in docs.items()
    }


def _resolve(node, uri, documents):
    while "$ref" in node:
        uri, fragment = urldefrag(urljoin(uri, node["$ref"]))
        node = documents[uri]
        for part in fragment.removeprefix("/").split("/") if fragment else []:
            node = node[part.replace("~1", "/").replace("~0", "~")]
    return node, uri


def spark_schema(kind: str) -> T.StructType:
    documents = _documents(contract_documents())
    uri = "file:///dsp-contracts/" + CONTRACT_FILES[kind]

    def convert(node, base):
        node, base = _resolve(node, base, documents)
        kinds = node.get("type")
        kinds = kinds if isinstance(kinds, list) else [kinds]
        concrete = [kind for kind in kinds if kind != "null"]
        if len(concrete) != 1:
            raise ValueError(f"unsupported contract type union: {kinds}")
        kind = concrete[0]
        nullable = "null" in kinds
        scalars = {
            "string": T.StringType,
            "integer": T.LongType,
            "number": T.DoubleType,
            "boolean": T.BooleanType,
        }
        if kind in scalars:
            return scalars[kind](), nullable
        if kind == "array":
            child, contains_null = convert(node["items"], base)
            return T.ArrayType(child, containsNull=contains_null), nullable
        if kind == "object" and "properties" in node:
            fields = []
            for name, child in node["properties"].items():
                data_type, child_null = convert(child, base)
                fields.append(
                    T.StructField(
                        name,
                        data_type,
                        child_null or name not in node.get("required", []),
                    )
                )
            return T.StructType(fields), nullable
        raise ValueError(f"unsupported contract construct: {kind}")

    result, _ = convert(documents[uri], uri)
    if not isinstance(result, T.StructType):
        raise TypeError("root contract must be an object")
    return result


def _reject_constant(value):
    raise ValueError(f"nonfinite JSON number: {value}")


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON property: {key}")
        result[key] = value
    return result


def parse_json(raw: str):
    return json.loads(raw, parse_constant=_reject_constant, object_pairs_hook=_object)


@lru_cache(maxsize=2)
def _validators(serialized: str):
    documents = _documents(serialized)
    registry = Registry().with_resources(
        (uri, Resource.from_contents(schema)) for uri, schema in documents.items()
    )
    formats = FormatChecker()

    @formats.checks("date-time", raises=(ValueError, TypeError))
    def aware_datetime(value):
        return not isinstance(value, str) or (
            re.fullmatch(
                r"\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-]\d{2}:\d{2})",
                value,
            )
            is not None
            and datetime.fromisoformat(value.upper()).utcoffset() is not None
        )

    # Reject values Spark could otherwise truncate, overflow or coerce to null.
    checker = Draft202012Validator.TYPE_CHECKER.redefine(
        "integer",
        lambda _checker, value: type(value) is int and -(2**63) <= value < 2**63,
    )
    checker = checker.redefine(
        "number",
        lambda _checker, value: type(value) in (int, float) and math.isfinite(value),
    )
    bounded = validators.extend(Draft202012Validator, type_checker=checker)
    return {
        name: bounded(
            documents["file:///dsp-contracts/" + filename],
            registry=registry,
            format_checker=formats,
        )
        for name, filename in CONTRACT_FILES.items()
    }


def validate(value, kind: str, *, documents: str | None = None) -> None:
    _validators(documents or contract_documents())[kind].validate(value)
    if kind.endswith("_bronze") and value["count"] != len(value["posts"]):
        raise ValueError("Bronze count does not match posts")
    if kind == "event":
        for name in ("source", "run_id", "record_id"):
            if value[name] != value["payload"][name]:
                raise ValueError(f"event {name} conflicts with payload")
        identity = json.dumps(
            [value["run_id"], value["record_id"]],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        expected = "sha256:" + hashlib.sha256(identity.encode("utf-8")).hexdigest()
        if value["event_id"] != expected:
            raise ValueError("event_id conflicts with observation identity")


def validation_reason(raw: str, kind: str, documents: str) -> str | None:
    try:
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8")
        validate(parse_json(raw), kind, documents=documents)
    except ValidationError as exc:
        return f"contract violation at {exc.json_path}: {exc.message}"
    except (ValueError, TypeError, OverflowError) as exc:
        return str(exc)
    return None
