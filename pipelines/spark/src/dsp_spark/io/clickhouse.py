"""Typed Gold serving with validated staging and atomic whole-table publication.

One loader at a time. A retry replaces the full current state, not snapshot history.
An ambiguous network failure at publication must be retried; no exactly-once claim.
"""

import json
import logging
from datetime import date, datetime
from pathlib import Path
from uuid import uuid4

import httpx

from ..processing.clean.contracts import contract_documents, spark_schema, validate
from ..runtime.clickhouse import ClickHouseSettings

KIND = "location_market_signal_snapshot"
TABLE = KIND
MAX_LOCAL_ROWS = 100_000
logger = logging.getLogger(__name__)


def serving_types(kind=KIND):
    result = {}
    contract = json.loads(contract_documents())[kind]
    for name, spec in contract["properties"].items():
        kinds = spec["type"] if isinstance(spec["type"], list) else [spec["type"]]
        kind = next(k for k in kinds if k != "null")
        mapped = {
            "string": "String",
            "integer": "Int64",
            "number": "Float64",
            "boolean": "Bool",
            "array": "Array(String)",
        }[kind]
        if spec.get("format") == "date":
            mapped = "Date32"
        elif spec.get("format") == "date-time":
            mapped = "DateTime64(6, 'UTC')"
        elif spec.get("x-dsp-decimal"):
            precision, scale = spec["x-dsp-decimal"]
            mapped = f"Decimal({precision}, {scale})"
        result[name] = f"Nullable({mapped})" if "null" in kinds else mapped
    return result


def validate_source_schema(schema):
    expected = {f.name: f.dataType for f in spark_schema(KIND)}
    actual = {f.name: f.dataType for f in schema}
    # Delta reads all fields as nullable. Validate actual values before writing;
    # do not cast incompatible types or discard extra columns.
    if actual != expected or len(schema) != len(expected):
        raise ValueError(f"Incompatible Gold serving schema: {schema.simpleString()}")


def validated_rows(rows):
    result = []
    identities = set()
    windows = set()
    for source in rows:
        row = dict(source)
        validate(row, KIND)
        for name in ("snapshot_date", "window_start_date", "window_end_date"):
            if not 1900 <= date.fromisoformat(row[name]).year <= 2299:
                raise ValueError(f"{name} outside ClickHouse Date32 range")
        if not 1900 <= datetime.fromisoformat(row["gold_generated_at"]).year <= 2299:
            raise ValueError("gold_generated_at outside ClickHouse DateTime64 range")
        start = date.fromisoformat(row["window_start_date"])
        end = date.fromisoformat(row["window_end_date"])
        if (
            row["snapshot_date"] != row["window_end_date"]
            or (end - start).days + 1 != row["window_days"]
        ):
            raise ValueError("Gold snapshot/window metadata disagree")
        identity = (row["snapshot_date"], row["location_id"])
        if identity in identities:
            raise ValueError(f"Duplicate Gold serving identity: {identity}")
        identities.add(identity)
        windows.add(
            tuple(
                row[n]
                for n in (
                    "snapshot_date",
                    "window_start_date",
                    "window_end_date",
                    "window_days",
                )
            )
        )
        result.append(row)
        if len(result) > MAX_LOCAL_ROWS:
            raise ValueError(f"Local serving loader exceeds {MAX_LOCAL_ROWS} rows")
    if len(windows) > 1:
        raise ValueError("Gold must contain one current snapshot/window")
    return sorted(result, key=lambda r: r["location_id"])


def read_gold_rows(spark, path):
    frame = spark.read.format("delta").load(path)
    validate_source_schema(frame.schema)
    return validated_rows(r.asDict() for r in frame.toLocalIterator())


class ClickHouseClient:
    def __init__(self, settings: ClickHouseSettings):
        self.settings = settings
        host = settings.host
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        self.http = httpx.Client(
            base_url=f"http://{host}:{settings.port}",
            auth=(settings.user, settings.password),
            timeout=settings.timeout_seconds,
            trust_env=False,
        )

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.http.close()

    def execute(self, sql, *, data="", params=None):
        options = {
            "wait_end_of_query": "1",
            "output_format_json_quote_64bit_integers": "0",
            "date_time_input_format": "best_effort",
            "input_format_null_as_default": "0",
            "input_format_defaults_for_omitted_fields": "0",
            "async_insert": "0",
            **(params or {}),
        }
        response = self.http.post(
            "/", params={"query": sql, **options}, content=data.encode("utf-8")
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"ClickHouse HTTP {response.status_code}: {response.text[:2000]}"
            )
        return response.text

    def query(self, sql, *, params=None):
        return [
            json.loads(line)
            for line in self.execute(
                sql + " FORMAT JSONEachRow", params=params
            ).splitlines()
            if line
        ]

    def check_schema(self, table):
        fields = self.query(f"DESCRIBE TABLE {self.settings.database}.{table}")
        actual = {r["name"]: r["type"] for r in fields}
        if actual != serving_types() or len(fields) != len(actual):
            raise ValueError(f"Incompatible ClickHouse serving schema: {actual}")

    def read_rows(self, table=TABLE):
        columns = [
            (
                "formatDateTime(gold_generated_at, '%Y-%m-%dT%H:%i:%S.%fZ', 'UTC') AS gold_generated_at"
                if name == "gold_generated_at"
                else name
            )
            for name in serving_types()
        ]
        return self.query(
            f"SELECT {', '.join(columns)} FROM {self.settings.database}.{table} ORDER BY location_id"
        )


def migrate(client):
    root = next(
        p
        for p in Path(__file__).resolve().parents
        if (p / "infra/clickhouse/migrations").is_dir()
    )
    ddl = (
        root / "infra/clickhouse/migrations/001_location_market_signal_snapshot.sql"
    ).read_text()
    for statement in ddl.replace("{database}", client.settings.database).split(";"):
        if statement.strip():
            client.execute(statement)
    client.check_schema(TABLE)


def replace_serving(client, rows):
    rows = validated_rows(rows)
    client.check_schema(TABLE)
    db = client.settings.database
    engines = client.query(
        "SELECT engine FROM system.databases WHERE name = {db:String}",
        params={"param_db": db},
    )
    if engines != [{"engine": "Atomic"}]:
        raise ValueError("Serving publication requires an Atomic ClickHouse database")
    stage = f"{TABLE}_load_{uuid4().hex}"
    client.execute(f"CREATE TABLE {db}.{stage} AS {db}.{TABLE}")
    try:
        if rows:
            payload = (
                "\n".join(
                    json.dumps(row, ensure_ascii=False, allow_nan=False) for row in rows
                )
                + "\n"
            )
            client.execute(
                f"INSERT INTO {db}.{stage} ({', '.join(serving_types())}) FORMAT JSONEachRow",
                data=payload,
            )
        # Compare all typed values, not just a count, before exposing the data.
        if client.read_rows(stage) != rows:
            raise ValueError("ClickHouse staged rows differ from validated Delta Gold")
        client.execute(f"EXCHANGE TABLES {db}.{TABLE} AND {db}.{stage}")
    finally:
        try:
            client.execute(f"DROP TABLE IF EXISTS {db}.{stage}")
        except Exception:
            logger.exception(
                "Staging cleanup failed for %s; retry the full load and inspect leftovers",
                stage,
            )
    return {
        "status": "success",
        "loaded_rows": len(rows),
        "table": f"{db}.{TABLE}",
        "reload": "atomic_full_replacement",
    }
