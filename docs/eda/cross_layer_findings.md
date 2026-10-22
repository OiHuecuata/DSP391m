# Cross-layer findings and analytical guardrails

Analysis of validated generation **2026-10-12T05:30:05.213076Z**, generated from source observations through **2026-10-12T05:28:49.458043Z**. All listed datasets were fully queried, with no sampling or source-table writes. Instants are UTC; new Gold publication/collection calendar dates use Asia/Ho_Chi_Minh.

## Findings supported by the captured population

1. Bronze's 1,446 post and 17 comment observations reduce to 356 unique posts/videos and seven comments/replies. Repeated collection substantially inflates observation counts.
2. Observed eligible supply is 336 PROBABLE venues; VERIFIED=0. Fourteen unverified canonical records remain separate. Court completeness is 316/336=94.05%.
3. The source-reported **912 courts at Pickleball 163 Trần Hòa** contributes 34.85% of the 2,617 known-court sum. Excluding only that record for sensitivity leaves 1,705; no production value was corrected. Median known courts per eligible venue=4, p95=12.
4. Hà Nội and Hồ Chí Minh hold 240/336 observed venues (71.43%). Source coverage is purposive and single-source for eligible venues, so this is sample concentration.
5. Resolved geographic spans=245/284 (86.27%), while resolved own geography covers only 165/356 posts (46.35%) and zero comments. Different grains give different coverage denominators. All 350 canonical venues lack usable coordinates; precise maps are blocked.
6. Only seven YouTube comments/replies are collected; sentiment is missing on all seven and relevance is never explicitly true. The 94 source-reported comments on relevant videos cannot be used as collected-comment geography or feedback evidence. One uncertain SEEK_COURT comment enters intent but is ineligible for relevance-only comment analytics.
7. There are 1,463 cumulative snapshots across 363 content entities, measured only October 7–12, 2026. YouTube's 906 valid view intervals include 21 decreases; Threads repeats static counters. Publication-cohort totals cannot establish historical engagement growth.
8. 303/356 posts (85.11%) are published in October 2026; the older 22-month publication inventory is sparse. Collection-run evidence covers five VN dates. Market seasonality and stable historical demand are unsupported.
9. Historical supply is absent on 77/99 integrated monthly groups. Supply/social comparisons are diagnostic raw counts only; seven of 13 sampled supply provinces have no observed social records.
10. All ten new ClickHouse Gold tables match captured Delta content; two legacy Gold tables are absent from serving, and the remaining legacy market snapshot ends October 7 even though rebuilt October 12.

## Executed consistency evidence

| Check | Executed result |
| --- | --- |
| Requested datasets read | 6 Bronze + 13 Silver intelligence + 10 new Gold + 3 legacy Gold = 32 |
| Supplementary reads | 3 legacy Silver + 1 generation manifest |
| Schema/row contracts | 17,093 rows, zero violations |
| Duplicate keys | Zero across all 32 requested grains |
| Independent new Silver → Gold | 1,954/1,954 |
| Independent legacy source / aggregate checks | 347/347 |
| Production-formula replay | 10/10 exact matches; supplementary, same formula implementation |
| Manifest input/output versions | 29/29 match |
| Manifest content digests | 29/29 match |
| ClickHouse typed content comparison | 10/10 new tables + legacy market snapshot match; v1 supply/activity missing |
| Version stability during capture | True |

Independent checks cover latest supply national/province counts, court sums/partitions, every activity group and record type, platform/date/location engagement post counts and nullable latest counters, and every intent group's distinct content. Legacy checks independently rebuild address-alias supply, latest Threads post/location/date counts/native counters and market-window contributions. Bronze source identities reconcile completely to Silver posts/comments and PBPlus source links. Checks validate arithmetic and lineage; they do not validate collector representativeness, source truth or classifier accuracy.

The first scan hit sandbox socket restrictions and then a missing legacy serving table; its incomplete results were not used. The reader was corrected to record absent serving tables. A naive Spark timestamp conversion was also corrected in the analysis capture only; all six Bronze manifest digests now match exactly. Existing collection advanced between attempts, so the final captured population is larger than the implementation report. The final scan itself was stable and matches one validated generation. Live SQL was verified later against the then-current service; query_results.json is timestamped separately and can have newer aggregate counts.

## Counting and temporal rules for every chart

- Choose **one** of NATIONAL, VN_ADMIN_POST_2025, VN_ADMIN_PRE_2025 or UNRESOLVED. For national metrics filter both admin_version and admin_unit_id to NATIONAL. Never add national and province rows or concatenate administrative versions.
- Province content is membership in a location; multi-location content can appear in several provinces. Do not derive unique national content or geographic resolution percentages by summing province groups.
- Supply counts canonical eligible venues; known courts sum only explicit counts. Verified/probable/unverified and unknown/conflicting counts remain labeled. Unknown operating status is visible.
- New social dates are VN publication dates; collection coverage uses VN run dates; engagement measurement uses observed_at/snapshot_as_of. v1 activity uses UTC publication dates and its market window is independent of build time.
- Choose one latest valid cumulative snapshot per content; use positive-time consecutive valid intervals for deltas. Negative deltas remain corrections, NULL is unavailable, and shares/rates/totals with no definitions stay blocked.
- Label counts overlap; evidence spans, unique content, venues, source entities, collected comments and reported video comments are different measures.
- Ratios use documented eligible denominators and weighted recomputation. No population/per-capita normalization, measured unmet demand, occupancy, actual player numbers or causal claim is supported.

## Next analysis and dashboard decisions

Review the 912-count source and repeated-name candidates; obtain supported venue verification/POI and comparable price evidence; expand permitted comments and true refreshed metric history; evaluate the rule labels; record query plans and complete official reference coverage. These are analytical dependencies, not jobs executed in this task. Build the READY chart specifications first; PARTIAL charts need visible caveats and BLOCKED charts need data/semantics first. No dashboards, pipelines, collector jobs or source mutations were implemented.

Full field NULL rates, empty-value counts, numeric quantiles, categorical frequencies and time bounds are in [profiles.json](evidence/profiles.json); exact findings and checks are in [findings.json](evidence/findings.json). Read logic: [read_snapshot.py](code/read_snapshot.py), [analyze.py](code/analyze.py).
