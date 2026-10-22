# Silver & Gold Data Requirements v1.0 — implementation report

Verified on 2026-10-12 (Asia/Ho_Chi_Minh), processing revision `requirements-v1.0-r5`. The repository now materializes all 13 specified Silver datasets and all six P0/four P1 Gold datasets from real available Bronze observations. All ten new Gold tables were staged, compared and published to the existing local ClickHouse service. Replay left output versions and profiles unchanged. This is a working upgrade with explicit remaining data and implementation limits, **not a claim of complete Data Requirements v1.0 or national market coverage**.

[Machine-readable executed evidence](evidence/silver_gold_upgrade_2026-10-12.json) · [Baseline requirements matrix](silver_gold_requirements_matrix.md) · [Metric formulas, denominators and lineage](silver_gold_metric_definitions.md)

## A. Changes made

Paths beginning `processing/`, `runtime/`, `io/` or `jobs/` below are relative to `pipelines/spark/src/dsp_spark/`.

| Files | Responsibility |
|---|---|
| `.gitignore`, `docs/data/silver_gold_*.md`, `docs/data/evidence/silver_gold_upgrade_2026-10-12.json` | Include the reviewable matrix, metric definitions, report and aggregate verification evidence; other private documents/runtime data remain ignored |
| `processing/intelligence/{catalog,common}.py` | Dataset grains, explicit NULL schemas, deterministic identities, UTC/Vietnam dates, policy/expiry/tombstone filtering |
| `processing/intelligence/{social,silver}.py` | Independent posts/comments/replies, conservative multi-label classification, text cache, intent spans and cumulative snapshots; historical social venue evidence |
| `processing/intelligence/venues.py` | Stable canonical entities/source links, exact name-plus-address/POI matching, ambiguous candidates, conflicting evidence, counts/prices/amenities and supported wards |
| `processing/intelligence/geography.py` | Reuse provenance-tagged repository references, dual versions and three interpretation modes, forward-only crosswalks, unknown locality mentions |
| `processing/intelligence/{gold,quality}.py` | Ten analytical aggregates, distinct national totals, grain/reference/lineage/numeric validation and measured profiles |
| `contracts/data/silver/*_*.schema.json`, `contracts/data/gold/*_*.schema.json` | Thirteen Silver and ten Gold contracts; canonical UTC/date representations and DECIMAL VND storage metadata |
| `processing/clean/contracts.py`, `runtime/config.py` | Register contracts and separate v3 paths; exact money storage types; prefer configured/bundled/system Java |
| `io/delta.py`, `jobs/batch/build_intelligence.py` | Keyed changed-row synchronization, explicit r1–r3 Double-to-DECIMAL migration only for new tables, replay fingerprint, validated generation, policy/retention publication guards, bounded historical backfill |
| `io/intelligence_clickhouse.py`, `io/clickhouse.py`, `jobs/batch/load_intelligence_clickhouse.py` | Typed Atomic staging/exchange, values/digest comparison, explicit new-table monetary migration; preserve v1 serving |
| `processing/clean/source_lifecycle.py`, existing v1 court/social Gold jobs, `jobs/batch/bronze_retention.py` | Verified deletion, source expiry and configured exclusions filter legacy consumer projections; the existing retention plan lists new derived datasets without changing v1 contracts |
| `orchestration/local/{pipeline,collection_worker}.py`, Airflow DAG/runner/smoke | Sequence validated Silver/Gold before serving; reuse local writer lock for Airflow tasks; maintain failure gates and existing status logging |
| `Makefile`, `infra/databricks/{build_wheel,run_social_activity_daily}.py` | Finite build/backfill/test/load commands; packaged references/contracts and managed-session intelligence entrypoint |
| `pipelines/spark/test/test_intelligence*.py`, `test_databricks_execution.py` | Twenty-eight final acceptance tests plus managed-session regression |

Existing unrelated edits to collection configuration and Makefile were preserved. No git commit was created. Bronze observations and legacy v1 table schemas were not reset.

## B. Dataset coverage matrix

Schema COMPLETE means the checked-in contract and actual Delta schema validate. Transformation status reflects supported working behavior, not merely schema existence. COMPLETE rows still represent the observed sample; PARTIAL explicitly includes limitations listed in the last column. Every new table was populated with production observations and executed tests. Synthetic acceptance fixtures remain in isolated temporary lakehouses.

| Dataset | Grain | Schema | Transformation | Production rows | Tests | Remaining gap / interpretation |
|---|---|---|---|---:|---|---|
| `silver_venue_candidates` | evidence candidate | COMPLETE | PARTIAL | 1,741 | PASS | narrow named-venue rules; no licensed POI geocoder or independent open POI production observations |
| `silver_court_venues` | physical venue | COMPLETE | PARTIAL | 349 | PASS | physical-entity matching works; authoritative subregions/POIs and verified-venue evidence remain incomplete |
| `silver_venue_evidence` | venue observation or social extraction | COMPLETE | COMPLETE | 1,741 | PASS | source fields and historical social-text observations retained; production sources are the collected subset |
| `silver_venue_source_links` | source entity to canonical venue | COMPLETE | COMPLETE | 349 | PASS | replay-stable source links; multi-source acceptance uses explicitly synthetic fixtures |
| `silver_social_posts` | current source post or video | COMPLETE | PARTIAL | 302 | PASS | uncalibrated rules; language/sentiment/uncertain relevance retain NULL |
| `silver_social_comments` | current source comment or reply | COMPLETE | PARTIAL | 4 | PASS | independent comment/reply intelligence works; only four production entities; deep classifiers uncalibrated |
| `silver_social_engagement_snapshots` | content x observed instant (cumulative) | COMPLETE | PARTIAL | 1,360 | PASS | latest/deltas/age work; confirmed engagement-total/rate definitions and thresholds absent |
| `silver_social_geo_mentions` | content x text span x semantic role | COMPLETE | PARTIAL | 233 | PASS | provincial forward mappings and unresolved localities work; full NER, boundaries and subregions missing |
| `silver_geocoding_attempts` | candidate x address x reference version | COMPLETE | PARTIAL | 1,741 | PASS | actual local-gazetteer attempts and dependencies persisted; external POI geocoding BLOCKED |
| `silver_social_intent_evidence` | content x intent x explicit span x optional mention | COMPLETE | PARTIAL | 383 | PASS | spans/participants/durations/max-budget supported; relative times unresolved; broader exact-time and budget-range extraction remains |
| `dim_admin_unit` | administrative unit x version | COMPLETE | PARTIAL | 100 | PASS | 63/34 internal province IDs plus three wards; official codes, geometry and complete historical validity absent |
| `bridge_admin_crosswalk` | old x new unit mapping | COMPLETE | PARTIAL | 63 | PASS | 63 supported province crosswalks; mechanics support evidence-bearing splits; real ward crosswalk missing |
| `dim_location_alias` | alias x unit x effective period | COMPLETE | PARTIAL | 170 | PASS | curated aliases operational; exhaustive historical and subregion aliases missing |
| `court_supply_by_admin_version_v2` | location x admin version x snapshot | COMPLETE | COMPLETE | 22 | PASS | validated distinct-venue supply transformation; observed probable supply, not verified national census |
| `social_engagement_daily_v1` | location x version x platform x VN publication date | COMPLETE | PARTIAL | 158 | PASS | selected native counters and coverage work; total/rate semantics and high-engagement thresholds unconfirmed |
| `social_intent_daily_v2` | location x version x platform x VN publication date x label | COMPLETE | COMPLETE | 220 | PASS | disjoint post/comment/reply distinct counts and overlapping labels; classifier accuracy unmeasured |
| `social_comment_analytics_daily_v1` | comment geography x version x platform x VN comment date | COMPLETE | COMPLETE | 4 | PASS | independent comment scope and denominators; reported totals unallocatable and sentiment coverage limited |
| `social_activity_by_admin_version_daily` | location x version x VN publication date | COMPLETE | COMPLETE | 153 | PASS | distinct location/version groups and independently deduplicated NATIONAL totals |
| `national_location_coverage_v1` | location x version x source x observed collection day | COMPLETE | PARTIAL | 885 | PASS | actual run/query logs and MISSING scopes; plan adapters and record/query attribution incomplete |
| `venue_community_feedback_weekly_v1` | canonical venue x VN publication week | COMPLETE | PARTIAL | 13 | PASS | exact venue mentions and supported feedback; broader price/service-complaint classifiers missing |
| `location_term_usage_monthly_v1` | location x raw term x platform x month | COMPLETE | COMPLETE | 87 | PASS | raw spans, distinct content, version labels and explicit term-share denominators |
| `venue_discovery_trends_v1` | location x version x first-observed month | COMPLETE | COMPLETE | 24 | PASS | stable first-observed month, verification separation; discovery never asserted as opening date |
| `location_market_research_monthly_v1` | location x version x VN publication month | COMPLETE | PARTIAL | 96 | PASS | validated content/snapshot joins work; historical supply snapshots and classifier/source completeness limited |
| `court_supply_by_location` (retained v1) | location × current supply snapshot | COMPLETE | COMPLETE | 8 | PASS | Legacy semantics preserved; new analytical semantics are in separate tables |
| `social_activity_daily` (retained v1) | location × day (existing v1 date semantics) | COMPLETE | COMPLETE | 47 | PASS | Legacy semantics preserved; new analytical semantics are in separate tables |
| `location_market_signal_snapshot` (retained v1) | location × snapshot; default rolling 30 days | COMPLETE | COMPLETE | 8 | PASS | Legacy semantics preserved; new analytical semantics are in separate tables |

## C. Observed backfill and quality

| Authoritative Bronze dataset | Observations before | After | Distinct source IDs before |
|---|---:|---:|---:|
| `bronze_court_observations` | 1,687 | 1,687 | 337 |
| `bronze_social_posts` | 1,346 | 1,346 | 302 |
| `bronze_social_comments` | 14 | 14 | 4 |
| `bronze_engagement_snapshots` | 1,360 | 1,360 | 306 |
| `bronze_collection_runs` | 59 | 59 | 59 |
| `bronze_source_changes` | 1,594 | 1,594 | 643 |

- Production content: **302 posts/videos** (251 YouTube, 51 Threads), **3 top-level YouTube comments and 1 reply**; **1,360 cumulative snapshots** across 306 content entities. Counts of 1,346 post observations and 14 comment observations are not unique content totals.
- Canonical venue projection: **349** (336 PROBABLE, 13 UNVERIFIED, 0 VERIFIED). First source inventory contains 337 PBPlus source IDs; social evidence adds candidates without fabricating PBPlus records. At `2026-10-12` the independently computed NATIONAL eligible supply is **336 venues, 2,617 known courts**, with known counts on **316 venues** and unknown counts on **20 venues**; coverage is **316/336 = 94.05%**. No unknown court count became one.
- Geography: **233 mention spans** on 144 content entities; **203 RESOLVED, 30 UNRESOLVED**. Explicit resolved mentions cover **134/302 posts = 44.37%**. **331/349 venues** have provable province-level projections. Codes/polygons/verified POI coordinates are not inferred from these province results.
- Reference population: 63 pre-July provinces + 34 post-July provinces + 3 current wards = 100 administrative rows; 63 province crosswalk relationships and 170 aliases. Identifiers are internal discovery keys, not official administrative codes.
- Temporal evidence: post publication timestamps span `2024-08-04T18:09:06.000000Z` to `2026-10-11T03:33:28.000000Z`; engagement observations span `2026-10-07T22:24:47.159178Z` to `2026-10-11T03:44:46.977446Z`. Old publication dates do not establish old engagement curves or continuous collection coverage.
- All 23 new datasets have **zero duplicate grain keys**. Every stored accepted row validated its contract. Full NULL-by-field counts, source contributions, temporal boundaries and output digests are included in the evidence JSON.
- Replay: UNCHANGED_INPUT_NO_WRITES, identical output versions and profiles. Money uses DECIMAL(20,2) in new Delta/ClickHouse tables; explicit migration preserves logical rows and Delta history.
- Bronze provenance scan: 59 runs (39 Threads, 14 YouTube, 6 PBPlus), no explicit fixture markers or fixture-named artifact roots found. 27 original artifact paths are missing; the authoritative stored observations remain usable. This marker scan is not independent manual validation of every record. Older legacy Silver is not used to populate the new intelligence tables.
- No actual production source tombstones or expired observations were present in this run. Delete/expiry/late-change behavior was verified using clearly synthetic unit and real Delta fixtures. All six Bronze row counts remained unchanged.
- Source-policy registry was not configured for these observations: **6,060 unreviewed-policy observations** across the six tables. No retention duration, permission review or physical erasure is claimed.

### DQ-01 through DQ-12

| Requirement | Executed evidence | Limit |
|---|---|---|
| DQ-01/02 | Contracts and grain uniqueness validate; duplicate keys = 0 for all outputs | Future inputs still must validate |
| DQ-03/04/05 | Coordinate pairs/provenance checks, explicit resolution states, version/reference validation and split/merge fixtures | Complete official geography and POIs unavailable |
| DQ-06/08 | Nonnegative raw counters, nullable measurements, known/unknown/conflicting supply partition, DECIMAL money; negative derived deltas retained | Source-reported missing metrics cannot be reconstructed |
| DQ-07 | UTC canonical instants and explicit Vietnam dates; day-crossing and alternate-timezone regressions passed | Requested relative times stay unresolved |
| DQ-09 | Bronze observation IDs in Silver/Gold; metric-definition IDs and validated publication digests | Deep classifier/source coverage remains partial |
| DQ-10 | Same-input unit replay and actual production replay; unchanged output versions; metric-only classifier avoidance and interrupted text/evidence cache recovery | Distributed/concurrent-cloud writer behavior not live-tested |
| DQ-11 | 885 coverage rows based on actual run/query evidence and explicit MISSING scopes | Query success is pipeline quality, not market coverage; plan/query allocation adapters incomplete |
| DQ-12 | Active new derivations and v1 consumers filter verified deletion/expiry; stale policy/cutoff blocks publication | Physical Bronze/archive/Kafka/backup deletion remains operator-reviewed work |

## D. Executed tests and runtime verification

Commands used the repository `.venv`, `PYTHONPATH=apps/collector/src:pipelines/spark/src`, Java 17 and `SPARK_LOCAL_IP=127.0.0.1` for real Spark tests. Local socket tests were rerun outside the sandbox after sandbox-only socket failures. Counts below are per command and overlap; they must not be summed as unique tests.

| Command / check | Passed | Failed | Skipped | Result |
|---|---:|---:|---:|---|
| `.venv/bin/python -m unittest discover -v -s pipelines/spark/test -p 'test_*.py'` | 159 | 0 | 0 | PASS; full existing Spark/Delta regression |
| `.venv/bin/python -m unittest discover -v -s pipelines/spark/test -p 'test_intelligence*.py'` | 28 | 0 | 0 | PASS; final 21 pure + 3 real Delta + 4 serving acceptance tests |
| `.venv/bin/python -m unittest discover -v -s pipelines/spark/test -p test_databricks_execution.py` | 15 | 0 | 0 | PASS; managed-session and configuration tests, no live Databricks deployment |
| `.venv/bin/python -m unittest discover -v -s apps/collector/test -p "test_*.py"` | 299 | 0 | 1 | PASS; local PBPlus sample read test skipped because the sample file was absent |
| `.venv/bin/python -m unittest discover -v -s tests/integration -p "test_*.py"` | 27 | 0 | 0 | PASS; final local orchestration controls |
| `docker compose --profile airflow run --rm --no-deps --entrypoint python airflow-scheduler -m unittest discover -v -s orchestration/airflow/test -p "test_*.py"` | 8 | 0 | 0 | PASS; actual pinned Airflow image and DAG failure gates/shared-lock runner |
| `go test -json ./...` in `apps/backend` | 68 | 0 | 0 | PASS; four tested packages, five packages have no tests |
| Production backfill → replay → ClickHouse staged typed comparison | 23 datasets / 10 served Gold tables | 0 | 0 | PASS, real historical observations |
| `make spark-intelligence` on completed inputs | 1 entrypoint | 0 | 0 | PASS; executed UNCHANGED_INPUT_NO_WRITES |
| Scoped Ruff, Black formatting and `git diff --check` | Final changed files | 0 | 0 | PASS |
| AT-06 live cloud independence with laptop off | 0 | 0 | 0 | NOT VERIFIED: only local infrastructure available |

AT-01/02/03/04/05/07 passed in automated fixtures. AT-03 split mappings in the test are explicitly synthetic relationships used to test ambiguity mechanics, not asserted Vietnamese administrative relationships. No live external cloud job or collector was deployed for AT-06.

One early ClickHouse comparison failed because JSON serialized whole-number Float64 values as integer literals. The loader now restores declared numeric types before comparing canonical digests; the regression test and subsequent publication passed. No mismatch was silently accepted.

## E. Compatibility and operation

- v1 `court_supply_by_location`, `social_activity_daily`, `location_market_signal_snapshot` remain on their existing paths and contracts. Observed row counts remain **8, 47, 8**. The default market-signal rolling window remains **30 days**; API/dashboard schemas were not changed. Existing aggregation, window and backend tests passed.
- Silver v3 uses separate paths under `silver/intelligence/`; no rewrite of legacy Silver was required to build the new outputs. Only new intelligence tables have an explicit previous-money-schema migration whitelist; unrelated schema differences fail.
- The local worker and finite pipeline build validated intelligence after successful Bronze/Silver v1 ingestion and before new serving. Airflow adds `build_silver_gold_intelligence` and `load_intelligence_clickhouse`, with all-success dependencies. Its finite mutation tasks reuse the existing local writer lock; a backend cycle and Airflow cannot write simultaneously. Interleaving entire generations or external direct writers still requires respecting single-writer operation.
- `make spark-intelligence` runs the batch; `make intelligence-test` runs its tests; `make intelligence-backfill` re-extracts archived Bronze envelopes and builds intelligence; `make load-intelligence-clickhouse` requires a completed current validated manifest. The actual backfill used the existing complete record-level Bronze tables. Original-envelope backfill target was dry-run checked, not needlessly rerun over all archives.
- The Java default now falls back to the actually installed system JDK when the historical bundled Java path is absent, while preserving explicit JAVA_HOME/DSP_JAVA_HOME overrides.
- The portable wheel packages all new modules/contracts and the geographic reference; `silver_gold_intelligence` reuses a managed Spark session. This adapter was tested offline; actual managed-cluster execution and wheel deployment were NOT VERIFIED.
- ClickHouse stages and exchanges each table atomically only after whole-generation validation. Ten exchanges are not one transaction: failure halfway through publication requires rerunning the loader to regain a coherent cross-table generation. Bronze/input and output version changes, schema mismatches, policy changes or passed retention cutoffs reject publication.

## F. Remaining blockers and implementation limits

1. **Open POI and venue verification data:** no observed OSM/Overture collector contribution or licensed bulk POI geocoder; zero production VERIFIED venues. Obtain supported source permissions/data and manual verification evidence rather than generating locations.
2. **Authoritative geography:** full official codes, polygons, district/ward validity history, complete aliases and real many-to-many subregion crosswalks are absent. The repository province reference is an immediately pre-July-2025 snapshot; earlier boundary validity cannot be asserted.
3. **Classification and extraction:** no measured evaluation set/model calibration. Rules are intentionally narrow. Broader multi-venue extraction, requested exact-time/budget-range extraction and price/service feedback classification remain implementation work; uncertain results and unmeasured confidence stay NULL.
4. **Engagement semantics:** a reviewed per-platform total/rate definition and high-engagement thresholds are absent; missing source metrics (including views/shares where unavailable) are not zero. Historical metric observations start in October 2026 despite earlier content publication dates.
5. **Collection/query coverage:** only four comment/reply entities are present; source-reported totals do not establish comment geography or full retrieval. Planned-query adapters/registry scheduling attribution and record-to-query allocation need stronger evidence. MISSING location/source/day rows do not measure absent market.
6. **Retention:** reviewed policies are missing for the production observations. Active projections enforce configured policy/expiry/tombstone evidence, but physical archive, Kafka, original artifact and backup erasure is not implemented by this reversible backfill. Legacy historical Delta versions remain an explicit retention dependency.
7. **Scale and incremental work:** the finite driver adapter is capped at 250,000 rows per dataset. No-op batches skip writes, text classification is cached, and Delta updates changed keys, but dirty batches still scan/recompute bounded aggregates. Distributed affected-entity/partition processing and cloud concurrency/recovery testing remain necessary for larger deployments.
8. **External infrastructure:** live laptop-independent cloud collection, deployment and live Databricks execution remain NOT VERIFIED. Local Docker tests are not substituted for those tests.

## G. Next priorities

1. Review source policies and execute an end-to-end retention plan across all physical copies; keep serving guards active.
2. Add a permitted open POI ingestion/verification path and authoritative geographic reference/crosswalk versions.
3. Build manually labeled relevance/intent/venue/geography evaluation sets; expand deterministic exact-time/budget/feedback extraction and configure measured platform definitions.
4. Expand collected comments/replies, engagement history and query-plan/record-attribution evidence before interpreting historical/national coverage.
5. Replace the bounded driver adapter with distributed affected-partition processing, then exercise scheduler independence, cloud recovery and publication consistency in an authorized deployed environment.
