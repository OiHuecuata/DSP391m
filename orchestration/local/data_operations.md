# Local Data Operations

Run Kafka/ClickHouse as usual (`make kafka-up kafka-topic clickhouse-up`), install the Python
and frontend dependencies, and start `make backend-run` and `make frontend-run`.
Open `/data-operations`. The existing tracker remains at `/`.

The Go backend reads the ignored repository `.env` without overriding exported
environment variables. Store `YOUTUBE_API_KEY` there or export it to the backend.
Never use a `NEXT_PUBLIC_` variable for credentials. API responses expose only
`youtube.configured`. Restart the backend after changing the key. YouTube requests carry the key in `X-Goog-Api-Key`, not the
URL. API error telemetry stores reason/status codes rather than error messages.

## Control and persistence

`POST /api/v1/collection/start` accepts a `sources` object keyed by `threads`,
`youtube`, and `pbplus`. Each selection contains `enabled` and `interval_seconds`.
Start persists desired recurring state, schedules enabled idle sources immediately,
and returns HTTP 202. Intervals run from completion, not from collection start.
Defaults: Threads 12 hours, YouTube 6 hours, PBPlus 24 hours (disabled initially).
The minimum interval is 10 seconds for isolated bounded acceptance only.

`POST /api/v1/collection/stop` stops future scheduling and queued work. The active
cycle completes normally; the UI shows STOPPING until its transactions finish.
`POST /api/v1/collection/run` accepts `{"sources":["youtube"]}` (or multiple
sources) without enabling recurring collection. A running/queued source returns
409. `PATCH /api/v1/collection/config` saves source selections/cadence without
starting an idle controller. `POST /api/v1/collection/retry` accepts `{"run_id":...}`
and reuses immutable saved collection artifacts and observation IDs. Run history
includes execution attempts; repeated publication can duplicate Kafka events,
but Silver merges on stable event identity. Retry cannot recover acquisition that
failed before artifacts existed; use Run Now for a new bounded acquisition.

The backend owns the scheduler independently of HTTP/browser sessions. It persists
atomic, fsynced `.local/data-ops/control.json`, retains 100 recent execution attempts,
and preserves every artifact under `.local/data-ops/runs/<run_id>`. A process lock
rejects a second controller using the same state directory. All source cycles are
serialized, including Gold and serving replacement. A lakehouse writer lock also
coordinates the finite local pipeline and live accumulation processor. Standalone Spark CLI jobs and the existing Airflow DAG still require operator coordination.

Graceful backend shutdown stops scheduling and waits for the active cycle, while
preserving desired continuous state. On restart, a surviving worker is identified
by PID plus its script/run ID and reported RECOVERING; its summary is reconciled
after exit. A missing worker is marked FAILED, never left falsely RUNNING. A rare
crash before PID persistence can leave an untracked worker; the shared writer lock
prevents conflicting writes, and its immutable artifacts remain available.
Continuous scheduling resumes from persisted due times. A YouTube quota-exhausted
run backs off at least 24 hours; exact remaining API quota is never claimed.

Settings: `DSP_REPO_ROOT`, `COLLECTION_STATE_DIR`, `COLLECTION_CONFIG`,
`COLLECTION_PYTHON`, `DSP_LAKEHOUSE_BASE_URI`, existing `DSP_DELTA_*`, Kafka, and
ClickHouse variables. The default worker policy is
`configs/collection/data_ops.v1.json`; API/control selections never contain secrets.
The local runner explicitly requires filesystem-backed Delta. No S3 or remote
Databricks execution is enabled.

## Data and native measures

YouTube uses official `search.list`, batched `videos.list` (up to 50 IDs), and
public top-level `commentThreads.list`. Configurable publication windows, query
count, pages, video count, total/search/comment request budgets, and comment bounds
apply independently. Every request attempt counts toward the budget. Repeated
page tokens stop a query. Comments-disabled responses preserve the video and other
comments. Replies are not collected; reply-request telemetry remains zero.
Budgets are request counts, not exact project quota units or remaining quota.
See the official [pagination guide](https://developers.google.com/youtube/v3/guides/implementation/pagination),
[video API](https://developers.google.com/youtube/v3/docs/videos/list),
[comment API](https://developers.google.com/youtube/v3/docs/commentThreads/list),
and [quota guidance](https://developers.google.com/youtube/v3/determine_quota_cost).

Bronze YouTube stores query history and native successful API responses, video and
comment payloads, request counters, and safe errors. It can normalize offline.
YouTube videos use source `youtube` and identity `youtube:<video_id>`; comments
use `youtube_comment` and `youtube-comment:<comment_id>`, with `parent_record_id`.
Optional nullable Silver additions are `view_count`, `parent_record_id`,
`video_duration`, and `tags`. Videos retain title+description as text, views,
likes, total native comment count, channel, duration, and tags. Comments retain
their own text, author, publication time, and likes. Unknown metrics stay null.
The streaming job performs an explicit type-checked nullable Delta migration;
existing observations and files remain intact.

`gold/social_activity_daily` and the 23-field market snapshot continue to describe
Threads activity and PBPlus supply. `gold/social_source_activity_daily` groups
text-mentioned locations by publication date and source, keeping Threads
engagement, YouTube views, video likes/native comment counts, and comment likes
in separate fields. Native unknown metrics stay null. Its ClickHouse table has
the same name; staging compares every typed field before atomic exchange.
A fresh YouTube/PBPlus-only cycle initializes missing empty Silver tables and uses
UTC today as the market window end when no Threads publication date is available.
Serving migrations are checked before loading. The two serving tables publish separately; a cycle failure can leave one newer
than the other, which is visible in run stages and recoverable by replay.
Locations are text mentions, never verified author/commenter residence. YouTube
comments without their own geographic text do not inherit a video's location.

## Observability and verification

GET endpoints: `/api/v1/data-ops/{status,lakehouse,warehouse,runs}` and
`/api/v1/collection/status`. The dashboard polls every 3 seconds; infrastructure
inspection caches for 15 seconds. Delta metadata reconstructs active files from
JSON transaction logs, reading `numRecords` statistics without scanning Parquet.
When old logs have been pruned and checkpoint-aware inspection is required, rows
are honestly unknown. Physical size includes history/log/checkpoint files.
ClickHouse probes current table existence and actual counts/snapshot metadata;
Gold generation time is distinct from controller-observed successful load time.

Structured run progress tracks query/request counters and processing stages.
Successful completion requires Silver payload/identity comparison, Gold builds,
and all-field ClickHouse comparisons. Container health alone never establishes a
healthy pipeline. Threads source caps/staleness/detail failures remain visible;
source exceptions, Kafka delivery failures, Spark/Delta failures, and warehouse
failures are preserved as failed/partial cycles with safe summaries. Detailed
redacted exception traces remain server-side.

For bounded live acceptance, launch the backend with an ignored isolated
`COLLECTION_CONFIG`, then run `.venv/bin/python scripts/smoke_data_ops.py` with
the frontend on port 3100. It clicks Start in Chromium, closes the browser,
observes two distinct live cycles per source, opens a new browser to Stop after
the fourth cycle begins, waits for completion, and verifies a third cycle is
prevented. It restores normal intervals with the configuration API. Evidence,
screenshots, raw data, Delta, logs, and checkpoints stay ignored under `.local`.
Do not run other standalone Delta writers during acceptance.
