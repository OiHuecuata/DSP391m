# Delta Gold to ClickHouse local verification

Executed October 7, 2026 using the existing saved local Delta Gold, without
crawling, rebuilding Gold, or remote AWS/Databricks. Full source rows, SQL summary,
and actual Go API responses are recorded in ignored `.local/clickhouse-smoke.json`.

`make smoke-clickhouse` passed. Source:
`pipelines/spark/.local/lakehouse/gold/location_market_signal_snapshot`.
The serving table exists with all 23 typed contract columns. Delta has **8 rows**;
first load **8 rows**; rerun **8 rows / 8 distinct (snapshot_date, location_id)
identities**. SQL returns **2 both-signal, 6 supply-only, 0 social-only**.
All 23 fields of all eight SQL/API rows equal Delta, including dates, bools,
floats, null handling, and accented names.

Snapshot: `2026-08-13`; inclusive window: `2026-07-15`–`2026-08-13`, 30 days.
Generation timestamp survives as `2026-10-06T04:18:13.933724Z`. Snapshot labels
social analysis; supply is the source state at Gold build time, not reconstructed
historical supply.

Representative SQL/backend values compared against Delta:

| Location | Posts | Engagement | Venues | Known courts | Known-count venues | Unknown-count venues | Coverage | Posts/venue | Engagement/venue |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Hà Nội | 0 | 0 | 126 | 1524 | 107 | 19 | 0.8492063492063492 | 0 | 0 |
| TP. Hồ Chí Minh | 1 | 7 | 104 | 548 | 104 | 0 | 1 | 0.009615384615384616 | 0.0673076923076923 |
| Nha Trang | 1 | 18 | 10 | 45 | 10 | 0 | 1 | 0.1 | 1.8 |

Executed SQL summary:

```sql
SELECT count() AS rows,
       uniqExact(tuple(snapshot_date, location_id)) AS identities,
       countIf(has_social_signal AND has_supply_signal) AS both_signal,
       countIf(NOT has_social_signal AND has_supply_signal) AS supply_only,
       countIf(has_social_signal AND NOT has_supply_signal) AS social_only
FROM dsp391m.location_market_signal_snapshot
```

Result: `rows=8, identities=8, both_signal=2, supply_only=6, social_only=0`.
Latest and `?snapshot_date=2026-08-13` Go API reads return HTTP 200 with
`snapshot_date="2026-08-13"` and eight complete Gold rows. Selected fragment
from the actual Nha Trang API row:

```json
{"location_id":"vn-nha-trang","location_name":"Nha Trang","social_post_count":1,"social_engagement_total":18,"venue_count":10,"known_court_count":45,"social_activity_per_venue":0.1,"social_engagement_per_venue":1.8}
```

Invalid date returns 400; valid absent snapshot returns 404.

No nullable supply/ratio value is null in the real dataset. Null transport was
additionally verified with **isolated fixtures**, written to temporary Delta and
loaded twice into a temporary ClickHouse database. A social-only fixture preserves
all seven nullable supply/ratio columns as null in SQL and the actual Go API.
An all-unknown-count fixture preserves `known_court_count=null`, `venue_count=2`,
known-count venues `0`, unknown-count venues `2`, coverage `0`, and zero social
metrics/ratios. These are fixtures, not observed source facts. Reload with one
fixture removes the stale second row; empty reload clears the table. The temporary
database was removed; real serving remains at eight rows.

The existing full Spark suite passes **106 tests**, including nine new serving
tests. Backend repository/config/service-through-API tests and `go vet` pass.
Formatting, Ruff, and whitespace checks pass. Local sockets and the Go cache
required execution outside the restricted sandbox. Existing Spark tests emit
socket resource warnings but finish successfully.

**Delta Lake = analytical source of truth. ClickHouse = serving/query layer.**
Social evidence remains a text-mentioned-place activity proxy and PBPlus incomplete
known source supply. No score, ranking, gap model, recommendation, snapshot history,
or upstream redesign was introduced. S3A exists without verified AWS write/read;
offline Databricks validation is covered by the existing suite, while remote
Databricks execution remains unverified. No Airflow work was added. Serving assumes
one loader, a small in-memory dataset, and local HTTP transport.

## Commands executed

All completed successfully; logs for this run are in `/tmp/dsp391m-spark-tests.log`,
`/tmp/dsp391m-clickhouse-smoke.log`, and `/tmp/dsp391m-clickhouse-load.log`.

```sh
docker compose up -d --wait --wait-timeout 120 clickhouse
make clickhouse-migrate
PYTHONPATH=pipelines/spark/src poetry run python -m unittest discover -s pipelines/spark/test -p test_clickhouse.py
make backend-test backend-vet
make spark-test
make smoke-clickhouse
make load-market-signals-clickhouse
gofmt -l apps/backend/cmd/server/main.go apps/backend/internal/config/*.go apps/backend/internal/domain/market_signal.go apps/backend/internal/repository/clickhouse/*.go apps/backend/internal/services/analytics/*.go apps/backend/internal/api/market_signal*.go
poetry run black --workers 1 --check pipelines/spark/src/dsp_spark/io/clickhouse.py pipelines/spark/src/dsp_spark/runtime/clickhouse.py pipelines/spark/src/dsp_spark/jobs/batch/load_market_signals_clickhouse.py pipelines/spark/src/dsp_spark/jobs/batch/smoke_clickhouse.py pipelines/spark/test/test_clickhouse.py
poetry run ruff check pipelines/spark/src/dsp_spark/io/clickhouse.py pipelines/spark/src/dsp_spark/runtime/clickhouse.py pipelines/spark/src/dsp_spark/jobs/batch/load_market_signals_clickhouse.py pipelines/spark/src/dsp_spark/jobs/batch/smoke_clickhouse.py pipelines/spark/test/test_clickhouse.py
git diff --check
```

The documented `docker compose exec -T clickhouse clickhouse-client --query`
SQL example also ran and returned all eight locations with the reported metrics.
Go formatting returned no filenames; Black left all five files unchanged; Ruff
reported all checks passed. Backend tests and vet passed; Spark ran 106 tests
successfully. The focused serving unit run passed nine tests.

## Git observation

No staging, restore, reset, checkout, stash, or commit command was run by this task.
The initial and final staged diffs are byte-identical and empty. Index entries
are **not byte-identical to the initial entries**: concurrent commit `5fefc18`
(`Build Databricks and Spark layer`) appeared during implementation, including
the Compose edit already made by this task. The external commit was preserved;
index entries remained unchanged from the subsequent observation through the
final comparison. No attempt was made to undo another actor's Git changes.
