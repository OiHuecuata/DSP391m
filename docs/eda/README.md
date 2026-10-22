# DSP391m executed lakehouse EDA

Completed read-only analysis of **32 requested datasets**: six Bronze, 13 Silver intelligence, ten new Gold and three legacy Gold. Three legacy Silver datasets and one generation manifest were also queried. Requested datasets contain 17,093 rows across their different grains; this sum is not a population count. No Bronze/Silver/Gold/ClickHouse production tables were changed. No collector, backfill, infrastructure installation or Superset dashboard was implemented.

The captured generation is `2026-10-12T05:30:05.213076Z` (**12:30:05 Asia/Ho_Chi_Minh**), based on source observations through `05:28:49.458043Z`. Detailed versions, paths, row counts and schemas are in [inventory.json](evidence/inventory.json). Current service data may advance after this capture; saved SQL is reusable, and its later execution timestamp/results are recorded separately.

| Layer | Successfully analyzed | Important captured sizes |
| --- | --- | --- |
| Bronze | 6/6 | Courts 1,687; posts 1,446; comments/replies 17; snapshots 1,463; runs 60; source changes 1,697 |
| Silver intelligence | 13/13 | Canonical venue records 350; posts/videos 356; comments/replies 7; geo spans 284; snapshots 1,463 |
| New Gold | 10/10 | Supply 22; engagement 167; intent 243; comments 6; activity 162; coverage 983; venue feedback 14; terms 94; discovery 24; monthly research 99 |
| Legacy Gold | 3/3 | Supply 8; activity 47; market snapshot 8 |

Nine findings deserve attention:

1. Observation counts overstate entities: 1,446 post observations represent 356 posts/videos; 17 comment observations represent seven comments/replies.
2. Eligible supply is **336 PROBABLE venues; zero VERIFIED**. Fourteen unverified canonical records remain separate. Known counts cover 316/336 (94.05%).
3. A literal **912-court source value contributes 34.85% of 2,617 known courts**. Review it before interpreting totals; sensitivity excluding that one record is 1,705, without changing stored data.
4. Hà Nội and Hồ Chí Minh hold 240/336 observed venues (71.43%). Eligible supply comes from one collected source, so concentration is sample-specific.
5. Resolved geography covers 245/284 mention spans (86.27%) but only 165/356 posts (46.35%); no comment has resolved own geography. All 350 venue records lack usable coordinates.
6. Only five comments and two replies are collected, with zero supported sentiment classifications and zero explicitly relevant comments. Reported video comments do not constitute a collected-comment census.
7. Snapshots cover October 7–12, 2026 only. YouTube view intervals include 21 negative changes; Threads counters repeat unchanged. Old publication dates cannot establish old engagement growth.
8. 303/356 posts (85.11%) are published in October 2026; collection-run evidence covers five local dates. Historical supply is missing on 77/99 integrated monthly groups.
9. Arithmetic is consistent: **1,954 new and 347 legacy independent checks pass**; ten new Gold tables exactly match captured ClickHouse values. Legacy supply/activity are absent from ClickHouse; the served legacy market snapshot ends October 7 despite being built October 12.

| Output | Contents |
| --- | --- |
| [bronze_eda.md](bronze_eda.md) | Source coverage, distinct entities, run cadence/outcomes, completeness, growth, text and snapshot availability |
| [silver_eda.md](silver_eda.md) | Venues, classification/intent, independent comments, geography, references, metrics and interval distributions |
| [gold_eda.md](gold_eda.md) | All 13 Gold tables, supply/social/feedback comparisons, legacy differences and consistency |
| [cross_layer_findings.md](cross_layer_findings.md) | Significant findings, exact counting/time rules, consistency evidence and analytical limits |
| [superset_dashboard_spec.md](superset_dashboard_spec.md) | Six dashboard groups, READY/PARTIAL/BLOCKED charts, formulas, dimensions, filters, grains and priorities |
| [queries/](queries/) | 14 executable, read-only ClickHouse queries; all returned populated results |
| [charts/](charts/) | Two static SVG charts: probable venue supply and known-court distribution |
| [profiles.json](evidence/profiles.json) | Complete field NULL/empty profiles, exact numeric statistics/quantiles, categories and temporal bounds |
| [findings.json](evidence/findings.json) | Computed findings, every independent check, serving comparisons and generation validation |
| [query_results.json](evidence/query_results.json) | Timestamped actual execution results for every saved SQL query |

## Commands and execution status

Run from the repository root using the existing environment:

```bash
PYTHONPATH=apps/collector/src:pipelines/spark/src SPARK_LOCAL_IP=127.0.0.1 \
  .venv/bin/python docs/eda/code/read_snapshot.py > /tmp/dsp-eda-read.log 2>&1
PYTHONPATH=apps/collector/src:pipelines/spark/src \
  .venv/bin/python docs/eda/code/analyze.py
.venv/bin/python docs/eda/code/write_queries.py
PYTHONPATH=apps/collector/src:pipelines/spark/src \
  .venv/bin/python docs/eda/code/verify_queries.py > /tmp/dsp-eda-queries.log 2>&1
PYTHONPATH=apps/collector/src:pipelines/spark/src \
  .venv/bin/python docs/eda/code/write_reports.py
.venv/bin/python docs/eda/code/verify_artifacts.py
git diff --check
```

Snapshot capture and SQL verification required approval outside the sandbox for Spark/local-service sockets. The initial capture failed on an absent legacy serving table; the successful reader records missing tables explicitly. An initial host-local naive timestamp serialization was corrected in the analysis copy using `.venv/bin/python docs/eda/code/repair_initial_capture_timezone.py`; current `read_snapshot.py` already handles conversion correctly. Corrected Bronze values match all six manifest digests. Raw input records remain only in ignored `.local/eda/snapshot.json`; report evidence contains aggregates and schema/lineage metadata.

Final results: **32/32 requested Delta datasets queried**, 17,093 contract-valid rows, zero duplicate declared grains; **29/29 manifest versions and digests match**; 1,954/1,954 new and 347/347 legacy independent checks pass; **10/10 supplementary production-formula replays match**. Serving comparison: ten new tables plus legacy market snapshot MATCH, two legacy serving tables MISSING. SQL execution: **14/14 populated queries**. Artifact verification and `git diff --check` pass; no unrelated application tests were needed for this analysis-only change.

## Top ten recommended charts, ranked by analytical value and readiness

1. Observed probable venues by current province — READY, P0.
2. Court-count completeness by province — READY, P0.
3. Collection evidence by province/source/day — READY, P0.
4. Relevant collected posts by platform — READY, P0.
5. Relevance classification coverage by publication date — READY, P0.
6. YouTube latest views by publication cohort — PARTIAL, P0.
7. Intent-label frequencies by platform — PARTIAL, P0.
8. Relevant social activity by current province — READY, P0.
9. Place-name usage within location/month/platform — READY, P1.
10. Venues first observed in the lake, by province — PARTIAL, P1.
