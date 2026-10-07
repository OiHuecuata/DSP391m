"""Real saved Delta -> repeated serving publication -> SQL -> Go service/API.

Never crawls or rebuilds source Gold. Null fixtures use a temporary Delta table
and isolated ClickHouse database; they never replace the real serving dataset.
"""

import json
import os
import socket
import subprocess
import tempfile
import time
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import httpx

from ...io.clickhouse import (
    TABLE,
    ClickHouseClient,
    migrate,
    read_gold_rows,
    replace_serving,
    serving_types,
)
from ...processing.clean.contracts import spark_schema
from ...runtime.clickhouse import ClickHouseSettings
from ...runtime.config import SparkSettings
from ...runtime.spark import create_spark


def require(condition, message):
    if not condition:
        raise RuntimeError(f"ClickHouse smoke failed: {message}")


@contextmanager
def backend(binary, settings, log_path):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = {
        **os.environ,
        "SERVER_PORT": str(port),
        "CLICKHOUSE_HOST": settings.host,
        "CLICKHOUSE_PORT": str(settings.port),
        "CLICKHOUSE_DATABASE": settings.database,
        "CLICKHOUSE_USER": settings.user,
        "CLICKHOUSE_PASSWORD": settings.password,
        "GIN_MODE": "release",
    }
    with log_path.open("w") as log:
        process = subprocess.Popen([str(binary)], env=env, stdout=log, stderr=log)
        try:
            with httpx.Client(
                base_url=f"http://127.0.0.1:{port}", timeout=15, trust_env=False
            ) as http:
                deadline = time.monotonic() + 15
                while True:
                    require(process.poll() is None, f"backend exited; see {log_path}")
                    try:
                        if http.get("/health").status_code == 200:
                            break
                    except httpx.TransportError:
                        pass
                    require(time.monotonic() < deadline, "backend readiness timeout")
                    time.sleep(0.1)
                yield http
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def verify_api(http, rows):
    path = "/api/v1/analytics/location-market-signals"
    response = http.get(path)
    require(response.status_code == 200, f"latest API: {response.text}")
    payload = response.json()
    require(payload["rows"] == rows, "latest API differs from Delta Gold")
    response = http.get(path, params={"snapshot_date": rows[0]["snapshot_date"]})
    require(
        response.status_code == 200 and response.json() == payload, "dated API differs"
    )
    require(
        http.get(path, params={"snapshot_date": "bad"}).status_code == 400,
        "invalid date API",
    )
    absent = "1900-01-01" if rows[0]["snapshot_date"] != "1900-01-01" else "2299-12-31"
    require(
        http.get(path, params={"snapshot_date": absent}).status_code == 404,
        "absent snapshot API",
    )
    return payload


def null_fixtures(source):
    social_only = dict(source[0])
    social_only.update(
        has_social_signal=True,
        has_supply_signal=False,
        social_post_count=1,
        social_like_count=2,
        social_reply_count=3,
        social_repost_count=0,
        social_quote_count=0,
        social_engagement_total=5,
    )
    for name, kind in serving_types().items():
        if kind.startswith("Nullable"):
            social_only[name] = None
    unknown_supply = dict(source[-1])
    unknown_supply.update(
        has_social_signal=False,
        has_supply_signal=True,
        venue_count=2,
        known_court_count=None,
        venues_with_known_court_count=0,
        venues_without_known_court_count=2,
        court_count_coverage_ratio=0.0,
        social_activity_per_venue=0.0,
        social_engagement_per_venue=0.0,
    )
    for name in unknown_supply:
        if name.startswith("social_") and name not in (
            "social_activity_per_venue",
            "social_engagement_per_venue",
        ):
            unknown_supply[name] = 0
    # The fixture needs distinct valid identities even for a one-location source.
    if unknown_supply["location_id"] == social_only["location_id"]:
        unknown_supply.update(
            location_id=(
                "vn-hue" if social_only["location_id"] != "vn-hue" else "vn-hanoi"
            ),
            location_name="Huế" if social_only["location_id"] != "vn-hue" else "Hà Nội",
        )
    return [social_only, unknown_supply]


def main():
    root = next(
        p
        for p in Path(__file__).resolve().parents
        if (p / "apps/backend/go.mod").is_file()
    )
    output = root / ".local/clickhouse-smoke.json"
    output.parent.mkdir(exist_ok=True)
    settings = SparkSettings.from_env()
    connection = ClickHouseSettings.from_env()
    spark = create_spark(settings)
    try:
        source = read_gold_rows(spark, settings.paths.location_market_signal_snapshot)
        require(bool(source), "real saved Delta Gold is empty")
        with tempfile.TemporaryDirectory(prefix="dsp-clickhouse-smoke-") as temp:
            temp = Path(temp)
            binary = temp / "backend"
            subprocess.run(
                ["go", "build", "-o", str(binary), "./cmd/server"],
                cwd=root / "apps/backend",
                check=True,
            )
            with ClickHouseClient(connection) as client:
                migrate(client)
                first = replace_serving(client, source)
                require(client.read_rows() == source, "first load differs from source")
                reloaded_source = read_gold_rows(
                    spark, settings.paths.location_market_signal_snapshot
                )
                require(reloaded_source == source, "source changed during smoke")
                second = replace_serving(client, reloaded_source)
                served = client.read_rows()
                require(served == source, "rerun differs from source")
                sql = f"SELECT count() AS rows, uniqExact(tuple(snapshot_date, location_id)) AS identities, countIf(has_social_signal AND has_supply_signal) AS both_signal, countIf(NOT has_social_signal AND has_supply_signal) AS supply_only, countIf(has_social_signal AND NOT has_supply_signal) AS social_only FROM {connection.database}.{TABLE}"
                summary = client.query(sql)[0]
                require(
                    summary["rows"] == summary["identities"] == len(source),
                    "duplicate serving rows",
                )
                with backend(binary, connection, temp / "backend-real.log") as http:
                    api = verify_api(http, source)

                fixture_settings = replace(
                    connection, database="dsp_smoke_" + uuid4().hex
                )
                with ClickHouseClient(fixture_settings) as fixture_client:
                    try:
                        migrate(fixture_client)
                        fixture_path = (temp / "null-gold").as_uri()
                        spark.createDataFrame(
                            null_fixtures(source), spark_schema(TABLE)
                        ).write.format("delta").save(fixture_path)
                        fixtures = read_gold_rows(spark, fixture_path)
                        replace_serving(fixture_client, fixtures)
                        replace_serving(fixture_client, fixtures)
                        require(
                            fixture_client.read_rows() == fixtures,
                            "nullable Delta fixture roundtrip",
                        )
                        with backend(
                            binary, fixture_settings, temp / "backend-fixture.log"
                        ) as http:
                            fixture_api = verify_api(http, fixtures)
                        # Verify stale rows disappear and an empty canonical state clears serving.
                        replace_serving(fixture_client, fixtures[:1])
                        require(
                            fixture_client.read_rows() == fixtures[:1],
                            "stale row retained",
                        )
                        replace_serving(fixture_client, [])
                        require(
                            fixture_client.read_rows() == [],
                            "empty reload retained rows",
                        )
                    finally:
                        fixture_client.execute(
                            f"DROP DATABASE IF EXISTS {fixture_settings.database}"
                        )

                report = {
                    "status": "success",
                    "source_delta_path": settings.paths.location_market_signal_snapshot,
                    "source_rows": len(source),
                    "first_load": first,
                    "rerun": second,
                    "sql": sql,
                    "sql_result": summary,
                    "all_23_fields_match_delta": served == source,
                    "source_gold_rows": source,
                    "api_response": api,
                    "isolated_nullable_delta_fixture_api": fixture_api,
                    "stale_and_empty_reload_verified": True,
                }
                output.write_text(
                    json.dumps(report, ensure_ascii=False, indent=2) + "\n"
                )
                print(
                    json.dumps(
                        {"status": "success", "evidence": str(output), **summary},
                        ensure_ascii=False,
                    )
                )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
