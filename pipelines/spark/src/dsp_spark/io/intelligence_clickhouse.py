"""Publish validated Gold tables using the existing Atomic staging pattern."""

import json
from uuid import uuid4

from ..jobs.batch.build_intelligence import (
    assert_generation,
    read_manifest,
    rows_from_delta,
)
from ..processing.intelligence.catalog import GOLD
from ..processing.intelligence.quality import table_digest, validate_tables
from .clickhouse import MAX_LOCAL_ROWS, serving_types


def canonical_served(client, table, kind):
    from ..processing.clean.contracts import contract_documents

    contract = json.loads(contract_documents())[kind]
    columns = []
    for name, spec in contract["properties"].items():
        if spec.get("format") == "date-time":
            columns.append(
                f"formatDateTime({name}, '%Y-%m-%dT%H:%i:%S.%fZ', 'UTC') AS {name}"
            )
        else:
            columns.append(name)
    rows = client.query(
        f"SELECT {', '.join(columns)} FROM {client.settings.database}.{table}"
    )
    # JSONEachRow can serialize a Float64 whole number as an integer literal.
    # Restore the declared type before comparing its canonical content digest.
    for row in rows:
        for name, spec in contract["properties"].items():
            kinds = spec["type"] if isinstance(spec["type"], list) else [spec["type"]]
            if "number" in kinds and row[name] is not None:
                row[name] = float(row[name])
    return rows


def replace_intelligence_serving(client, kind, rows):
    if kind not in GOLD:
        raise ValueError("unsupported analytical table")
    if len(rows) > MAX_LOCAL_ROWS:
        raise ValueError("serving row limit exceeded")
    validate_tables({kind: rows})
    db = client.settings.database
    if client.query(
        "SELECT engine FROM system.databases WHERE name = {db:String}",
        params={"param_db": db},
    ) != [{"engine": "Atomic"}]:
        raise ValueError("intelligence serving requires Atomic database")
    types = serving_types(kind)
    client.execute(
        f"CREATE TABLE IF NOT EXISTS {db}.{kind} ({', '.join(f'{n} {t}' for n,t in types.items())}) ENGINE = MergeTree ORDER BY tuple()"
    )
    described = client.query(f"DESCRIBE TABLE {db}.{kind}")
    actual_types = {r["name"]: r["type"] for r in described}
    previous_types = {
        n: (
            "Nullable(Float64)"
            if t.startswith("Nullable(Decimal(")
            else "Float64" if t.startswith("Decimal(") else t
        )
        for n, t in types.items()
    }
    if actual_types == previous_types and actual_types != types:
        # Explicit migration of only these newly introduced intelligence tables.
        # Unknown schemas and all existing v1 contracts still fail closed.
        for name, expected in types.items():
            if actual_types[name] != expected:
                client.execute(
                    f"ALTER TABLE {db}.{kind} MODIFY COLUMN {name} {expected}"
                )
        described = client.query(f"DESCRIBE TABLE {db}.{kind}")
    if {r["name"]: r["type"] for r in described} != types or len(described) != len(
        types
    ):
        raise ValueError(
            "incompatible intelligence serving schema; explicit migration required"
        )
    stage = f"{kind}_load_{uuid4().hex}"
    client.execute(f"CREATE TABLE {db}.{stage} AS {db}.{kind}")
    try:
        if rows:
            client.execute(
                f"INSERT INTO {db}.{stage} ({', '.join(types)}) FORMAT JSONEachRow",
                data="\n".join(
                    json.dumps(r, ensure_ascii=False, allow_nan=False) for r in rows
                )
                + "\n",
            )
        if table_digest(canonical_served(client, stage, kind)) != table_digest(rows):
            raise ValueError("staged ClickHouse rows differ from validated Gold")
        client.execute(f"EXCHANGE TABLES {db}.{kind} AND {db}.{stage}")
    finally:
        client.execute(f"DROP TABLE IF EXISTS {db}.{stage}")
    return {"table": kind, "loaded_rows": len(rows), "status": "VALIDATED"}


def publish_intelligence(spark, settings, client):
    manifest = read_manifest(spark, settings)
    assert_generation(spark, settings, manifest)
    # Validate all sources before any publication; never publish partial builds.
    tables = {
        n: rows_from_delta(spark, getattr(settings.paths, n), limit=MAX_LOCAL_ROWS)
        for n in GOLD
    }
    validate_tables(tables)
    for name, rows in tables.items():
        if table_digest(rows) != manifest["after"][name]["digest"]:
            raise ValueError(f"Gold content differs from manifest: {name}")
    return [
        replace_intelligence_serving(client, name, rows)
        for name, rows in tables.items()
    ]
