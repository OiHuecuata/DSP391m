"""Immutable research generations, reconciled before qualified catalog publication."""

import json

from dsp_collector.normalizers.observation import canonical_json, digest

from ..processing.clean.contracts import contract_documents, validate
from ..processing.threads.contracts import PREVIEW, RESEARCH, path
from .delta import read_delta

MAX_ROWS = 100_000


def types(kind):
    result = {}
    for name, node in json.loads(contract_documents())[kind]["properties"].items():
        kinds = node["type"] if isinstance(node["type"], list) else [node["type"]]
        concrete = next(k for k in kinds if k != "null")
        physical = {
            "string": "String",
            "integer": "Int64",
            "number": "Float64",
            "boolean": "Bool",
            "array": "Array(String)",
        }[concrete]
        result[name] = f"Nullable({physical})" if "null" in kinds else physical
    return result


def _rows(spark, settings, kind):
    result = []
    for row in read_delta(spark, path(settings, kind)).toLocalIterator():
        value = row.asDict(recursive=True)
        validate(value, kind)
        result.append(value)
        if len(result) > MAX_ROWS:
            raise ValueError("Research publication exceeds bounded serving size")
    return sorted(result, key=canonical_json)


def migrate(client):
    db = client.settings.database
    client.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
    client.execute(
        f"CREATE TABLE IF NOT EXISTS {db}.research_generations (generation_id String, scope String, status String, completed_at String, manifest_json String) ENGINE=MergeTree ORDER BY generation_id"
    )
    for kind in RESEARCH + PREVIEW:
        fields = types(kind)
        columns = ", ".join(f"{name} {value}" for name, value in fields.items())
        client.execute(
            f"CREATE TABLE IF NOT EXISTS {db}.research_{kind} (generation_id String, {columns}) ENGINE=MergeTree ORDER BY generation_id"
        )
        actual = {
            r["name"]: r["type"]
            for r in client.query(f"DESCRIBE TABLE {db}.research_{kind}")
        }
        if actual != {"generation_id": "String", **fields}:
            raise ValueError("Research serving schema drift")


def read_generation(client, kind, generation):
    if kind not in RESEARCH + PREVIEW:
        raise ValueError("Unregistered publication table")
    columns = ",".join(types(kind))
    rows = client.query(
        f"SELECT {columns} FROM {client.settings.database}.research_{kind} WHERE generation_id={{generation:String}}",
        params={"param_generation": generation},
    )
    return sorted(rows, key=canonical_json)


def publish(spark, settings, client, metadata):
    migrate(client)
    data = {name: _rows(spark, settings, name) for name in RESEARCH + PREVIEW}
    signatures = {
        name: {"row_count": len(rows), "sha256": digest(rows)}
        for name, rows in data.items()
    }
    generation = "sha256:" + digest(
        ["threads-research-generation-v1", metadata, signatures]
    )
    # This Threads slice qualifies only its available components. A complete
    # national generation also needs all ten independently validated datasets.
    if metadata["scope"] != "THREADS_EXPLORATORY":
        raise ValueError(
            "This component publisher serves qualified Threads exploratory generations only"
        )
    status = "INACTIVE_PARTIAL"
    manifest = {
        **metadata,
        "generation_id": generation,
        "status": status,
        "tables": signatures,
    }
    existing = client.query(
        f"SELECT manifest_json FROM {client.settings.database}.research_generations WHERE generation_id={{generation:String}}",
        params={"param_generation": generation},
    )
    if existing:
        if len(existing) != 1 or json.loads(existing[0]["manifest_json"]) != manifest:
            raise ValueError("Conflicting generation catalog")
        for name, rows in data.items():
            if read_generation(client, name, generation) != rows:
                raise ValueError("Published generation parity failure")
        return manifest
    for name, rows in data.items():
        table = f"{client.settings.database}.research_{name}"
        # An uncertain previous insert is inactive and can be reconciled safely.
        current = read_generation(client, name, generation)
        if current != rows:
            client.execute(
                f"ALTER TABLE {table} DELETE WHERE generation_id={{generation:String}} SETTINGS mutations_sync=2",
                params={"param_generation": generation},
            )
            if rows:
                payload = (
                    "\n".join(
                        canonical_json({"generation_id": generation, **r}) for r in rows
                    )
                    + "\n"
                )
                client.execute(f"INSERT INTO {table} FORMAT JSONEachRow", data=payload)
        restored = read_generation(client, name, generation)
        if restored != rows or digest(restored) != signatures[name]["sha256"]:
            raise ValueError("ClickHouse/Delta full-row reconciliation failed")
    catalog = {
        "generation_id": generation,
        "scope": metadata["scope"],
        "status": status,
        "completed_at": metadata["as_of"],
        "manifest_json": canonical_json(manifest),
    }
    client.execute(
        f"INSERT INTO {client.settings.database}.research_generations FORMAT JSONEachRow",
        data=canonical_json(catalog) + "\n",
    )
    restored = client.query(
        f"SELECT manifest_json FROM {client.settings.database}.research_generations WHERE generation_id={{generation:String}}",
        params={"param_generation": generation},
    )
    if len(restored) != 1 or json.loads(restored[0]["manifest_json"]) != manifest:
        raise ValueError("Generation publication acknowledgement uncertain")
    return manifest
