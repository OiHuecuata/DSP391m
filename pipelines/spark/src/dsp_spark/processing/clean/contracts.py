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
    "observation_v2": "data/bronze/record-observation.v2.schema.json",
    "observation_artifact_v2": "data/bronze/observation-artifact.v2.schema.json",
    "observation_artifact": "data/bronze/observation-artifact.v1.schema.json",
    "observation": "data/bronze/record-observation.v1.schema.json",
    "youtube_bronze": "data/bronze/youtube-api.v1.schema.json",
    "social_source_activity_daily": "data/gold/social-source-activity-daily.v1.schema.json",
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

# Register extensions centrally so executor processes resolve the same contracts.
for _name in (
    "silver_social_posts", "silver_social_comments", "silver_social_engagement_snapshots",
    "silver_social_geo_mentions", "silver_social_intent_evidence",
    "court_supply_by_admin_version_v2", "social_engagement_daily_v1", "social_intent_daily_v2",
    "social_comment_analytics_daily_v1", "social_activity_by_admin_version_daily",
    "national_location_coverage_v1", "venue_community_feedback_weekly_v1",
    "location_term_usage_monthly_v1", "venue_discovery_trends_v1",
    "location_market_research_monthly_v1", "threads_content_eda_v1", "threads_collection_eda_v1",
):
    CONTRACT_FILES[_name] = f"data/{'silver' if _name.startswith('silver_') else 'gold'}/{_name}.schema.json"


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
    if kind == "observation":
        from dsp_collector.normalizers.observation import validate_observation

        validate_observation(
            value, schema=json.loads(documents or contract_documents())[kind]
        )
    if kind in ("threads_bronze", "pbplus_bronze") and value["count"] != len(
        value["posts"]
    ):
        raise ValueError("Bronze count does not match posts")
    if kind == "event":
        for name in ("source", "run_id", "record_id"):
            if value[name] != value["payload"][name]:
                raise ValueError(f"event {name} conflicts with payload")
        if value["source"] in ("youtube", "youtube_comment"):
            prefix = (
                "youtube-comment:"
                if value["source"] == "youtube_comment"
                else "youtube:"
            )
            if value["record_id"] != prefix + value["payload"]["source_record_id"]:
                raise ValueError("YouTube identity conflicts with source_record_id")
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
