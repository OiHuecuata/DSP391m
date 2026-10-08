"""Publish native source measures separately from the market snapshot."""

import json
from datetime import date
from uuid import uuid4

from ..processing.clean.contracts import spark_schema, validate
from .clickhouse import MAX_LOCAL_ROWS, serving_types

KIND = "social_source_activity_daily"
ORDER = "activity_date, location_id NULLS FIRST, location_name, source"


def ordered(rows):
    result, seen = [], set()
    for row in rows:
        row = dict(row)
        validate(row, KIND)
        if not 1900 <= date.fromisoformat(row["activity_date"]).year <= 2299:
            raise ValueError("Source activity date outside serving range")
        key = tuple(
            row[k] for k in ("activity_date", "location_id", "location_name", "source")
        )
        if key in seen:
            raise ValueError("Duplicate source activity group")
        seen.add(key)
        result.append(row)
        if len(result) > MAX_LOCAL_ROWS:
            raise ValueError("Source serving row limit exceeded")
    return sorted(
        result,
        key=lambda r: (
            r["activity_date"],
            r["location_id"] or "",
            r["location_name"],
            r["source"],
        ),
    )


def read_gold(spark, path):
    frame = spark.read.format("delta").load(path)
    if {f.name: f.dataType for f in frame.schema} != {
        f.name: f.dataType for f in spark_schema(KIND)
    }:
        raise ValueError("Incompatible source activity Gold schema")
    return ordered(r.asDict() for r in frame.toLocalIterator())


def read_served(client, table=KIND):
    columns = [
        (
            "formatDateTime(gold_generated_at, '%Y-%m-%dT%H:%i:%S.%fZ', 'UTC') AS gold_generated_at"
            if name == "gold_generated_at"
            else name
        )
        for name in serving_types(KIND)
    ]
    return client.query(
        f"SELECT {', '.join(columns)} FROM {client.settings.database}.{table} ORDER BY {ORDER}"
    )


def replace_source_serving(client, rows):
    rows = ordered(rows)
    db = client.settings.database
    types = serving_types(KIND)
    if client.query(
        "SELECT engine FROM system.databases WHERE name = {db:String}",
        params={"param_db": db},
    ) != [{"engine": "Atomic"}]:
        raise ValueError("Source publication requires Atomic database")
    columns = ", ".join(f"{name} {kind}" for name, kind in types.items())
    client.execute(
        f"CREATE TABLE IF NOT EXISTS {db}.{KIND} ({columns}) ENGINE = MergeTree ORDER BY (activity_date, source, location_name)"
    )
    described = client.query(f"DESCRIBE TABLE {db}.{KIND}")
    if {r["name"]: r["type"] for r in described} != types or len(described) != len(
        types
    ):
        raise ValueError("Incompatible source serving schema")
    stage = f"{KIND}_load_{uuid4().hex}"
    client.execute(f"CREATE TABLE {db}.{stage} AS {db}.{KIND}")
    try:
        if rows:
            client.execute(
                f"INSERT INTO {db}.{stage} ({', '.join(types)}) FORMAT JSONEachRow",
                data="\n".join(
                    json.dumps(r, ensure_ascii=False, allow_nan=False) for r in rows
                )
                + "\n",
            )
        if read_served(client, stage) != rows:
            raise ValueError("Source serving staged values mismatch")
        client.execute(f"EXCHANGE TABLES {db}.{KIND} AND {db}.{stage}")
        if read_served(client) != rows:
            raise ValueError("Source serving publication values mismatch")
    finally:
        client.execute(f"DROP TABLE IF EXISTS {db}.{stage}")
    return {
        "status": "success",
        "loaded_rows": len(rows),
        "table": f"{db}.{KIND}",
        "all_fields_match": True,
    }
