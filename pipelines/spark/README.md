# Bronze/Silver/Gold lakehouse slice

The collector owns canonicalization. These jobs validate the existing contracts,
retain canonical fields, and persist observations in Delta without normalizing
Threads or PBPlus again. Gold reports daily activity associated with mentioned places and latest source-address court supply.
There are no measured-demand scores or ML jobs. The existing Gold can be served
through ClickHouse and refreshed through Airflow. For the finite complete local
workflow, see [one-command E2E](../../orchestration/local/README.md).

## Runtime

Use Python 3.12 and Java 17 or 21. Install `poetry install --no-root`.
The lock pins PySpark 4.0.2 and Delta 4.0.1, a pair supported by the
[Delta compatibility matrix](https://docs.delta.io/releases/).
[Spark 4.0.2](https://spark.apache.org/docs/4.0.2/) uses Scala 2.13;
its release build pins Scala 2.13.16 and Hadoop 3.4.1.

Set `JAVA_HOME` to your JDK, or supply `DSP_JAVA_HOME` to Make. Make falls back to
`.local/tools/java17` if `JAVA_HOME` is unset. Java is an external prerequisite,
not a committed binary or Python dependency. This workspace has a portable
Temurin 17 installation at that ignored path.

Spark resolves `io.delta:delta-spark_2.13:4.0.1`; the streaming runtime also
resolves `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2`.
S3 paths add `org.apache.hadoop:hadoop-aws:3.4.1`, whose POM selects the AWS SDK
and other dependencies. Do not independently replace bundled Hadoop/AWS JARs.
First startup needs Maven access. Later runs use Ivy's cache.
`DSP_SPARK_IVY_DIR` can select the cache. `DSP_SPARK_JARS` accepts an explicit
comma-separated, previously resolved compatible JAR bundle for offline runs.

The Python source paths are `pipelines/spark/src:apps/collector/src` (set by
Make). Runtime configuration imports only the existing collector topic constant;
Spark processing does not import collector normalizers or publisher behavior.
Contract documents are bundled into validation UDFs, so executors do not need
filesystem access to the repository contracts.

## Storage and settings

By default the lakehouse lives under ignored `pipelines/spark/.local/lakehouse`.
Set `DSP_LAKEHOUSE_BASE_URI` to an absolute `file:///...` or `s3a://bucket/prefix`:

```text
<base>/
  bronze/threads/
  bronze/pbplus/
  silver/social_posts/
  silver/courts/
  gold/social_activity_daily/
  gold/court_supply_by_location/
  gold/location_market_signal_snapshot/
  quality/unresolved_court_locations/
  rejected/social_posts/
  checkpoints/social_posts_kafka_to_delta/
```

`DSP_DELTA_THREADS_BRONZE_PATH`, `DSP_DELTA_PBPLUS_BRONZE_PATH`,
`DSP_DELTA_SOCIAL_POSTS_PATH`, `DSP_DELTA_COURTS_PATH`,
`DSP_DELTA_REJECTED_SOCIAL_PATH`, `DSP_DELTA_SOCIAL_ACTIVITY_DAILY_PATH` and
`DSP_DELTA_COURT_SUPPLY_BY_LOCATION_PATH`,
`DSP_DELTA_UNRESOLVED_COURT_LOCATIONS_PATH`,
`DSP_DELTA_LOCATION_MARKET_SIGNAL_SNAPSHOT_PATH` and
`DSP_SPARK_SOCIAL_CHECKPOINT_PATH` optionally
override individual derived paths. Keep every table and checkpoint distinct.

Other configuration:

| Variable | Default |
| --- | --- |
| `DSP_SPARK_MASTER` | `local[2]` |
| `DSP_SPARK_APP_NAME` | `dsp391-lakehouse` |
| `DSP_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `DSP_KAFKA_SOCIAL_POST_TOPIC` | Existing collector topic constant |
| `DSP_KAFKA_STARTING_OFFSETS` | `earliest`, used only for a new checkpoint |
| `DSP_SPARK_TRIGGER_INTERVAL` | `10 seconds` |
| `DSP_MARKET_SIGNAL_WINDOW_DAYS` | `30`, positive integer |
| `DSP_MARKET_SIGNAL_END_DATE` | Unset; maximum Gold `activity_date` |

S3A uses the AWS SDK v2 default credential chain: conventional environment,
`AWS_PROFILE`/shared configuration, web identity and instance/role credentials.
Use `AWS_REGION`/`AWS_DEFAULT_REGION`; `DSP_S3_ENDPOINT` and
`DSP_S3_PATH_STYLE=true` are optional explicit endpoint settings. No secrets are
embedded in Spark configuration or source. AWS SDK dependencies resolve only
when configured lakehouse/checkpoint paths require S3A.

Cloud verification requires an explicit `DSP_LAKEHOUSE_TEST_URI` and usable
credentials. These jobs never invent or create an S3 bucket. Local Delta tests
need neither Kafka nor AWS credentials.

## Developer workflow

From the repository root, using existing artifacts as read-only inputs:

```sh
make spark-test
make spark-bronze-threads SPARK_ARTIFACT="path/to/threads-bronze.json"
make spark-bronze-pbplus SPARK_ARTIFACT="path/to/pbplus-bronze.json"
make spark-pbplus-silver SPARK_ARTIFACT="path/to/courts-silver.json"
make kafka-up
make kafka-topic
make spark-social-stream-once
make delta-inspect-social
make delta-inspect-courts
```

`make spark-social-stream` runs continuously. `spark-social-stream-once` uses
Structured Streaming's AvailableNow trigger to drain available data and stop.
Restart with the same table paths and checkpoint to resume Kafka offsets.
Do not delete a checkpoint merely to retry a failed batch.
No Spark command recrawls either source or starts Kafka implicitly.

## Persistence and recovery semantics

Bronze stores one row per `(source, run_id)`, including the original complete
UTF-8 JSON text, its collection timestamp, artifact URI and ingestion timestamp.
Whitespace, CRLF and property ordering survive. Open-ended source fields are
retained inside `payload_json`; no flattening loses source detail.

PBPlus validates the complete Silver batch before writing, explodes records
with the contract-derived Court schema, and adds lakehouse ingestion time and
artifact URI. Invalid structure or conflicting batch lineage fails before any
write; a valid empty batch initializes a schema-correct Court Silver table.
Later empty batches validate its schema and leave existing observations intact.

Threads validates original Kafka JSON before Spark parsing can coerce or drop
fields. It checks the event/payload contracts, envelope lineage, deterministic
event identity and Kafka key. It preserves the payload and Kafka topic,
partition, offset and timestamp. Invalid JSON, payloads, identities or keys go
to `rejected/social_posts`, with original value bytes/text, Kafka metadata,
reason and rejection timestamp. A rejection-write failure stops the batch.
The streaming job initializes schema-correct Social Silver at startup, even when
Kafka has no messages and `foreachBatch` never runs. A batch with zero accepted
events also initializes empty Silver; malformed events still go to rejection
storage. Neither empty ingestion path creates fake records or clears prior data.

All canonical date-time fields remain strings to preserve their exact values;
added lakehouse/Kafka times use Spark timestamps with the UTC session timezone.
Only constructs present in the canonical Silver/event contracts are converted
into StructTypes. Integer overflow/coercion and nonfinite numeric values are
rejected before a typed write.

Delta uses insert-only MERGE with schema evolution disabled. Keys are:

- Bronze: `(source, run_id)`.
- Threads: `event_id` (observation identity derived from run + entity).
- PBPlus: `(run_id, record_id)`.
- Rejections: `(kafka_topic, kafka_partition, kafka_offset)`.

The first persisted observation wins replay; replay does not update its values
or ingestion metadata. Same entity in a different run is a separate valid row.
Jobs assume one writer per table; concurrent initialization/conflicting writes
surface errors for retry. Rejected and accepted writes are separate Delta
transactions. Retrying a partially completed foreachBatch is safe for both
merge keys. This is at-least-once input plus idempotent merge, without an
end-to-end exactly-once claim. Kafka data-loss detection remains enabled.

## Gold social activity

`make spark-gold-social` reads the current Silver snapshot and atomically
replaces the entire `gold/social_activity_daily` Delta table. Inspect it with
`make delta-inspect-gold-social`. This small first table deliberately uses a full
rebuild: newer observations update counters and stale date/location groups are
removed. An empty eligible input replaces Gold with an empty table. No changes
are made to Silver. Use one Gold writer at a time.

Before aggregation, select one observation per logical `record_id`, ordered by:

1. `collected_at` timestamp descending.
2. `emitted_at` timestamp descending.
3. `lakehouse_ingested_at` descending.
4. `event_id` descending as a stable tie-breaker.

Collection time takes precedence so later transport replay of an older run
cannot replace a newer collection. Valid Silver has unique event IDs; exact
replay duplicates carry identical canonical content. Selection happens before
eligibility filtering: a latest observation without publication time/location
mentions does not resurrect an older observation's values.

`activity_date` is the UTC calendar component of canonical `published_at`.
Null publication times are excluded; collection time is never substituted.
Distinct `detected_locations` are exploded and exact aliases are resolved to
`location_id` and accented `location_name` before deduplication per logical post/place.
Empty lists/null or empty mentions produce no group. Unmapped mentions retain
their original name and a null ID, preserving activity contributions; the build
reports unmapped mentions. These are text-derived mentions, not verified post origins
or validated market demand. The job explicitly recognizes only the legacy Gold
schema without `location_id` for atomic migration on rebuild; other drift fails.

Each selected post contributes once to each distinct mentioned location.
`post_count` counts distinct logical posts in the date/location group; a
`unique_post_count` would be redundant. Nullable engagement counters contribute
zero to sums; this is an aggregation policy, not proof of an observed zero.
`engagement_total` is the unweighted sum of likes, replies, reposts and quotes.
First/last publication times remain canonical UTC strings. `gold_generated_at`
is rebuild metadata; all content metrics are deterministic for a Silver snapshot.
The Gold schema derives from its authoritative JSON contract and each output
row is validated before the Delta replacement.

Databricks preparation uses the same Gold job through a managed-session adapter.
See `infra/databricks/README.md` for wheel packaging, explicit configuration and
local validation. No remote Databricks or cloud S3 execution is claimed.

## Versioned named-place reference

`configs/reference/vietnam_locations.v1.json` contains ten named-place entries
and explicit aliases copied from collector `CITY_SCOPES`. Validate it with
`make location-reference-validate`. The Draft 2020-12 contract is
`contracts/reference/vietnam-locations.v1.schema.json`; runtime additionally
rejects duplicate IDs, empty normalized aliases and aliases colliding across IDs.
NFKC Unicode normalization, casefolding, punctuation-to-spaces and whitespace
collapse apply only to matching. Accents are retained; ASCII variants are explicit
aliases. There is no fuzzy matching, boundary map, sub-city rollup, administrative
merger equivalence or coordinate inference. The reference is deliberately small.

## Gold court supply and quality

```sh
make spark-gold-court-supply
make delta-inspect-gold-court-supply
```

The job reads Silver courts without crawling or enriching them. Latest logical
venues are selected per `record_id`, in descending order of timestamp-parsed
`collected_at`, `geocoded_at` (null last), `normalized_at`, then
`lakehouse_ingested_at` and lexical `run_id`. Collection precedes enrichment and
replay. Valid Silver has unique `(run_id, record_id)` observations.

Court Silver has no structured administrative location. The resolver examines
explicit comma-separated address components and source municipality/province
prefixes. In observed PBPlus page text it reads the final `Địa chỉ :` section and
removes known non-address annotations. It does not match arbitrary substrings
in page bodies or street names, map provinces to cities, or use coordinates.
Missing, unmapped, ambiguous and `court_name_fallback` addresses stay unresolved.
`address_source=null` permits source-supplied text but does not establish verified
address provenance. Social keys represent mentions; court keys represent source
address evidence. A shared key never makes those evidence types equivalent.

Resolved venues form `gold/court_supply_by_location`, under its authoritative
JSON contract. `venue_count` counts distinct latest venue entities, not all runs.
`known_court_count` sums only non-null source counts and stays null if every count
is unknown. Explicit zero counts remain known zero; null is missing knowledge,
not an observed zero or one. Known/unknown venue counts describe coverage.
`latest_source_observed_at` is the latest collection timestamp in each group.
No geolocated count is added because coordinates do not verify a named region.

Latest unresolved venues retain all Silver fields plus nullable location fields,
a reason and `quality_generated_at` in `quality/unresolved_court_locations`.
These are valid Court records with unresolved address evidence, not rejected
Courts. The build reports logical/resolved/unresolved counts, percentage (null
for empty input), and reason counts. Gold and quality each use full atomic Delta
replacement with strict schemas. They are separate transactions; after partial
failure rerun the job. Only generated timestamps vary for unchanged Silver.
PBPlus coverage does not establish national completeness. Use one writer at a time.

## Location market signal snapshot

```sh
make spark-gold-market-signal
make delta-inspect-gold-market-signal
DSP_MARKET_SIGNAL_WINDOW_DAYS=7 DSP_MARKET_SIGNAL_END_DATE=2026-08-13 make spark-gold-market-signal
```

This first combined analytical product reads existing Gold `social_activity_daily`
and `court_supply_by_location`, validates both contracts and canonical names,
aggregates social metrics inside an explicit window, and performs a full outer
join on `location_id`. It does not read Silver records or deduplicate logical
posts again. The pure transformation lives in
`processing/aggregate/location_market_signal_snapshot.py`; Delta I/O and quality
reporting live in `jobs/batch/build_location_market_signal_snapshot.py`.

The Draft 2020-12 contract is
`contracts/data/gold/location-market-signal-snapshot.v1.schema.json`. All columns
are scalar. Social counts are 64-bit integers; coverage and activity/venue ratios
are nullable doubles. Dates and canonical UTC generation timestamps remain strings.
The table has one row per catalog ID and is atomically replaced on each build,
including removal of stale rows. There is no snapshot history. Unchanged inputs
and window preserve every analytical metric; only `gold_generated_at` varies.

Window end defaults to maximum `activity_date` across social Gold, never the
wall-clock date. Start is end minus `window_days - 1`. Both boundaries are
inclusive: `window_start_date <= activity_date <= window_end_date`.
`snapshot_date` equals the selected end date and labels the analysis reference
date; it is not the supply observation date or the build date. If social Gold
is empty, an explicit valid `DSP_MARKET_SIGNAL_END_DATE` is required. With that
override, supply-only rows are produced; both empty inputs produce an empty,
contract-shaped Delta table. Invalid/empty configuration or an underflowing date
range fails clearly. A missing input Delta table is an error, distinct from an
existing empty table.

Social evidence is text-mentioned-place activity, a proxy/signal, not measured
actual demand or verified user/post origins. The presence flag means a Gold
social row exists inside the window. Counts are summed directly from Gold:
posts, likes, replies, reposts, quotes and total engagement. Multiple mentioned
locations can receive one post's contribution, so a cross-location sum is not
necessarily a unique-post count. Supply is the latest known source state at build
time, not historical supply reconstructed for the social dates. PBPlus does not
represent complete national supply, verified market coverage or court capacity.

| Evidence | Social fields | Supply fields | Ratios |
| --- | --- | --- | --- |
| Both inputs | Summed window counts | Preserved source totals | Calculated for positive venue count |
| Social only | Summed window counts | All null | All null |
| Supply only | Zero counts, social flag false | Preserved source totals | Activity/engagement per venue zero; count coverage from supply |

`has_supply_signal` records presence of a supply input row. `known_court_count`
stays null when all venue court counts are unknown. An explicit known zero stays
zero. `venues_without_known_court_count` is preserved without estimating missing
courts. `court_count_coverage_ratio` is known-count venues divided by venue count:
a measure of court-count data availability. `social_activity_per_venue` is post
count divided by venue count; `social_engagement_per_venue` uses total engagement.
A missing or nonpositive denominator produces null, never infinity or NaN. The
current source supply contract rejects nonpositive venue counts before job output;
the pure ratio calculation also handles zero safely. No court-count denominator,
score, ranking, capacity estimate or recommendation is introduced.

The catalog supplies output names. Only social rows with a non-null canonical
`location_id` contribute to the market snapshot. Null-ID mentions remain in
`social_activity_daily` with their original names and metrics as unresolved
quality evidence; raw mentions and observation lineage also remain in Silver.
Country (`Vietnam`), district/sub-city (`Cầu Giấy`, `Bắc Từ Liêm`, `Nam Từ Liêm`,
`Thủ Đức`) and arbitrary unsupported mentions are not mapped to a city/province.
Mixed and out-of-window unresolved evidence cannot poison canonical publication.
Build JSON reports `unresolved_social_location_rows`,
`unresolved_social_location_rows_in_window`, and `social_activity_source_path`
so exclusions are visible and the preserved evidence can be inspected. These
counts are daily location rows, not unique posts across locations.

Nonnull unknown IDs, canonical name disagreements and duplicate canonical
daily social/supply keys still fail before replacement. Contract validation
continues to cover all input Gold rows, including unresolved evidence. Window
end still defaults to the maximum date in the entire social Gold input, including
unresolved mentions; if that input is empty, the explicit end date is required.

Build JSON reports `supply_total_logical_venues`, `supply_resolved_venues`,
`supply_unresolved_venues`, and `supply_location_resolution_ratio` separately from
location rows. Resolved total is summed from Gold supply; unresolved total comes
from the existing `quality/unresolved_court_locations` artifact, never Silver or
allocation across locations. Missing quality means total/unresolved/resolution
ratio are null with `supply_quality_available=false`, not a claim of zero missing
venues. Observable mixed/stale generation timestamps, resolved quality rows or
duplicate unresolved logical IDs fail before output. Empty quality artifacts have
no row timestamp, so their generation cannot be compared; these source artifacts
remain separate Delta transactions and need a single coordinated writer.

For the actual local verification and limitations, see
[the smoke report](market-signal-local-smoke.md). For managed packaging,
see [the Databricks adapter](../../infra/databricks/README.md). Cloud AWS S3 and
remote Databricks execution remain unverified.

## ClickHouse serving

The current market-signal Delta Gold can be published to typed ClickHouse and
read through the Go analytics API. Delta remains the analytical source of truth;
ClickHouse is the serving/query layer. See [local commands and reload semantics](../../infra/clickhouse/README.md)
and [the executed Delta → ClickHouse → API smoke](../../infra/clickhouse/local-smoke.md).
