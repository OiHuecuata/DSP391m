# Local location market signal verification

Verified October 6, 2026, using saved real canonical collector artifacts. The
previously verified Delta tables were absent from this workspace's default
lakehouse and searches under the repository, home directory and `/tmp`. They
were recreated locally through the existing Silver ingestion and Gold jobs,
without crawling, network source collection, geocoding or cloud execution.
The analytical job itself reads only Gold and the existing unresolved quality
artifact; no Silver court records are read by that job.

Inputs restored from these existing read-only artifacts:

- `apps/collector/.local/silver/social_posts/4290feb5e7754b9cb525570dfd934465.json`: six observations, five logical location-associated post contributions, five daily Gold rows; zero unmapped mentions.
- `apps/collector/.local/silver/courts/c332d92238334ccdb349ab92aab14f2a.json`: 337 logical venues; 285 resolved and 52 unresolved; eight Gold locations. Unresolved reasons: 51 unmapped addresses and one missing address.

## Actual analysis

The inclusive default window is **2026-07-15 through 2026-08-13**, 30 days.
Maximum social Gold activity date, not current date, chooses the end.
`snapshot_date=2026-08-13` labels the social analysis; supply is the latest known
source state at the October 6 build, not supply reconstructed for August.

Eight output locations: **two with both signals, zero social-only, six
supply-only**. Only two of the five social post contributions fall inside the
window. Window totals: two posts, 12 likes, 13 replies, zero reposts, zero quotes,
and 25 engagements. This extremely small sample supports no market recommendation.
Social locations are text mentions, not verified user/post locations or demand
origins. Social activity is a proxy/signal, not measured demand.

| Location | Posts | Likes | Replies | Engagement | Venues | Known courts | Known-count venues | Unknown-count venues | Count coverage | Posts/venue | Engagement/venue |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Bình Dương | 0 | 0 | 0 | 0 | 5 | 30 | 5 | 0 | 1.000000 | 0.000000 | 0.000000 |
| Cần Thơ | 0 | 0 | 0 | 0 | 1 | 4 | 1 | 0 | 1.000000 | 0.000000 | 0.000000 |
| Đà Nẵng | 0 | 0 | 0 | 0 | 24 | 114 | 24 | 0 | 1.000000 | 0.000000 | 0.000000 |
| Hải Phòng | 0 | 0 | 0 | 0 | 12 | 64 | 12 | 0 | 1.000000 | 0.000000 | 0.000000 |
| Hà Nội | 0 | 0 | 0 | 0 | 126 | 1524 | 107 | 19 | 0.849206 | 0.000000 | 0.000000 |
| TP. Hồ Chí Minh | 1 | 2 | 5 | 7 | 104 | 548 | 104 | 0 | 1.000000 | 0.009615 | 0.067308 |
| Huế | 0 | 0 | 0 | 0 | 3 | 13 | 3 | 0 | 1.000000 | 0.000000 | 0.000000 |
| Nha Trang | 1 | 10 | 8 | 18 | 10 | 45 | 10 | 0 | 1.000000 | 0.100000 | 1.800000 |

Every output row has `has_supply_signal=true`. Only TP. Hồ Chí Minh and Nha Trang
have `has_social_signal=true`. Ratios are rounded here; Delta retains doubles.
All real known-court totals are present; fixture tests verify all-unknown nulls.

## Separate coverage measures

- Location resolution: `285 / 337 = 0.8456973294` (84.57%). The 52 unresolved venues remain separate quality rows, never allocated to the eight locations.
- Court-count data coverage in Hà Nội: `107 / 126 = 0.8492063492` (84.92%); 19 venue counts are unknown. All other resolved locations have count coverage 1.0.
- Across resolved source venues: 266 have court counts and 19 do not (`266 / 285 = 93.33%`). The known court sum is 2,442; it excludes unknown court counts and unresolved venue locations.

Neither metric measures complete market or national supply coverage. PBPlus is
one source, not a nationally complete venue inventory. Activity/venue and
engagement/venue ratios describe these specific sources and the selected window.

## Persistence and validation

The target reopened in a new Spark session. Two consecutive actual Delta builds
produced identical analytical values, including dates and presence flags; only
`gold_generated_at` changed. The normal Make build and inspection targets also
ran against the restored local tables. Latest output:

`pipelines/spark/.local/lakehouse/gold/location_market_signal_snapshot/`

The complete build results and scalar rows are retained as the ignored artifact
`.local/market-signal-smoke.json`.

Validation completed:

- `make format` and `make lint`.
- Collector suite: 203 tests, one skipped; no failures.
- Spark suite, including all Gold and offline Databricks tests: 97 tests; no failures. This includes 15 new analytical/configuration tests and two new managed-adapter tests.
- `poetry check --lock` and `git diff --check`.
- Databricks 0.3.0 wheel build, isolated imports/catalog/contract resource checks, and offline market job rendering with explicit fixture configuration. Contract bytes match the repository; managed-session handoff is mocked, not remotely executed.
- Git index entry comparison before/after: identical; no staging, resetting, checkout, stashing or commits.

Spark uses local sockets, so JVM checks ran outside the restricted sandbox.
No explicit AWS test URI or remote Databricks execution was used. AWS S3 and
remote managed runtime execution remain unverified.

## Remaining limits and next step

The social sample is very small; places are mentions, not verified demand
origins. PBPlus supply is incomplete and 52 court venues remain unresolved.
There is no demand score, gap score, ranking, recommendation, population/capacity
inference, ClickHouse, Airflow, ML or snapshot history. Empty unresolved quality
artifacts cannot carry a row generation timestamp, and source quality/supply are
separate Delta writes; their freshness needs coordinated source rebuilds.

Only after this verified descriptive table, a separate task can define an
explicit documented unmet-demand candidate methodology or load the table into
ClickHouse as a serving layer. Neither is implemented here.
