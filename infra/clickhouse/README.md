# Local analytical serving

**Delta Lake = analytical source of truth. ClickHouse = serving/query layer.**
Only typed Gold `location_market_signal_snapshot` is loaded here. Social counts
describe interest/activity associated with text-mentioned locations, not measured
demand or verified user/post locations. Supply describes known PBPlus observations,
not complete national supply. Unknown court counts and undefined ratios remain null.
There is no score, ranking, gap model, or recommendation methodology.

## Start, initialize, and load

From the repository root, using the existing Poetry environment and Java 17:

```sh
make clickhouse-up
make clickhouse-status
make clickhouse-migrate
make load-market-signals-clickhouse
make backend-run
```

Compose pins `clickhouse/clickhouse-server:25.8.16.34`, publishes HTTP on loopback,
checks `/ping`, and retains data in `clickhouse-data`. `make clickhouse-down` stops
only this service and retains its volume. Initialization explicitly applies
[`001_location_market_signal_snapshot.sql`](migrations/001_location_market_signal_snapshot.sql)
with a validated database substitution and then checks column names/types. It
works on new/existing volumes and is safe to rerun. Existing incompatible tables
fail validation; they are not silently altered. Loading requires the migration.

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `CLICKHOUSE_HOST` | `localhost` | Loader/backend HTTP host |
| `CLICKHOUSE_PORT` | `8123` | Compose published HTTP port and client port |
| `CLICKHOUSE_DATABASE` | `dsp391m` | Simple SQL identifier |
| `CLICKHOUSE_USER` | `dsp391m` | Local user |
| `CLICKHOUSE_PASSWORD` | empty | Loopback development default; configure your own if needed |

Compose reads `.env`; export the same values for Python/Go commands, which do
not read `.env` automatically. `.env.example` contains no real credentials.
The loader uses existing `DSP_LAKEHOUSE_BASE_URI` and optional
`DSP_DELTA_LOCATION_MARKET_SIGNAL_SNAPSHOT_PATH`, defaulting to
`pipelines/spark/.local/lakehouse/gold/location_market_signal_snapshot`.
No AWS, Databricks runtime, or recrawl is required. `SERVER_PORT` overrides
backend port 8080. Python requests time out after 30 seconds; Go requests have
a context/client timeout of 10 seconds.

## Table and reload semantics

Default table: `dsp391m.location_market_signal_snapshot`, in an **Atomic** database,
using **MergeTree**, ordered by `(snapshot_date, location_id, window_start_date,
window_end_date, window_days, has_social_signal, has_supply_signal)`.
Snapshot/location are primary reads; window metadata/presence flags remain in
the ordering key. There is no partitioning/secondary index for this small table;
presence-only filtering may scan it. Canonical location-ID response order has
no ranking semantics.

All 23 contract columns remain typed: `String` for IDs/names/version, `Int64`
for counts/window days, `Date32` for dates, `Bool` for presence,
`Nullable(Int64)` for supply counts, `Nullable(Float64)` for ratios, and
`DateTime64(6, 'UTC')` for generation time. Dates/timestamps convert back to
contract strings, including six fractional digits. Nonfinite numbers and integer
overflow fail validation. Date/generation years outside 1900–2299 fail before insertion.

Gold replaces its entire table with one row per location and one shared
snapshot/window. Serving identity is `(snapshot_date, location_id)`; extra window
metadata is preserved and checked for consistency, rather than permitting several
windows for one current snapshot. There is no synthetic ID or snapshot history.
A dated read can return only the currently served date.

The loader checks exact source columns/types without coercion, validates every
value and identity, loads a unique private staging table synchronously, and compares
every staged value to Delta before publication. Atomic
[`EXCHANGE TABLES`](https://clickhouse.com/docs/sql-reference/statements/exchange)
publishes the full replacement; cleanup removes the previous generation.
Reruns do not append duplicates, stale locations disappear, and an empty source
clears serving. Failure before exchange preserves the previous served state.
A network failure around publication is ambiguous: retry the complete load to
converge. Cleanup failure is logged; inspect leftover `_load_` tables after an
interrupted process.

Use **one loader at a time**. MergeTree ordering does not enforce uniqueness;
source validation/full replacement do. Do not append manually. This is repeatable
publication, not distributed exactly-once delivery. The loader stages in driver
memory and fails above 100,000 rows. Scaling, writer coordination, history, remote
TLS transport, and orchestration are future work.

## SQL and API

```sh
docker compose exec -T clickhouse clickhouse-client --query \
  'SELECT snapshot_date, location_name, social_post_count, social_engagement_total, venue_count, known_court_count FROM dsp391m.location_market_signal_snapshot ORDER BY location_id'
curl --fail 'http://localhost:8080/api/v1/analytics/location-market-signals'
curl --fail 'http://localhost:8080/api/v1/analytics/location-market-signals?snapshot_date=2026-08-13'
```

The container client uses the configured user/password environment. Adjust the
SQL database if overridden. Response:
`{"snapshot_date":"YYYY-MM-DD","rows":[...23 Gold fields per row...]}`.
JSON preserves nulls and zero distinctly. Invalid/empty/repeated dates return 400,
absent/empty snapshots 404, dependency failures the existing generic JSON error
convention with 500. `/health` is backend liveness; `/api/v1/system/status` retains
the existing collector implementation and placeholder infrastructure health entries.

The backend uses HTTP `JSONEachRow`, typed date parameters, and standard Go
HTTP/JSON libraries. SQL stays in the ClickHouse repository; analytics serves
already computed Gold; the handler handles HTTP. No new dependency was required.

## Tests and real smoke

```sh
make smoke-clickhouse
make backend-test backend-vet
make spark-test
```

Smoke starts ClickHouse/applies DDL, reads saved real Delta Gold, loads twice,
compares all columns/unique identities through SQL, then builds and starts a
temporary real Go backend on an available port. It verifies latest/dated/invalid/
absent-date APIs. Temporary Delta fixtures and an isolated ClickHouse database
verify nullable counts/ratios, zeros, stale-row removal, and empty reload. Fixtures
never overwrite real Gold/serving. Temporary processes/databases are cleaned up.
Smoke replaces and leaves the real serving table available, without mutating Gold.
An empty real source fails smoke; the production loader supports empty replacement.

Executed evidence: ignored `.local/clickhouse-smoke.json` and
[October 7 verification](local-smoke.md). S3A configuration exists, but actual
AWS S3 write/read remains unverified. Offline Databricks rendering/validation
is prepared; remote execution remains unverified. Airflow is outside this phase.
