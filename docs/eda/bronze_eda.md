# Bronze EDA

Analysis of validated generation **2026-10-12T05:30:05.213076Z**, generated from source observations through **2026-10-12T05:28:49.458043Z**. All listed datasets were fully queried, with no sampling or source-table writes. Instants are UTC; new Gold publication/collection calendar dates use Asia/Ho_Chi_Minh.

## Coverage and source entities

| Dataset | Full rows | Delta version | Duplicate grain keys |
| --- | --- | --- | --- |
| `bronze_court_observations` | 1687 | 5 | 0 |
| `bronze_social_posts` | 1446 | 30 | 0 |
| `bronze_social_comments` | 17 | 9 | 0 |
| `bronze_engagement_snapshots` | 1463 | 30 | 0 |
| `bronze_collection_runs` | 60 | 115 | 0 |
| `bronze_source_changes` | 1697 | 31 | 0 |

| Dataset | Observations | Distinct platform / ID / type | Runs represented | Largest run share |
| --- | --- | --- | --- | --- |
| bronze_court_observations | 1687 | 337 | 6 | 19.98% |
| bronze_social_posts | 1446 | 356 | 30 | 6.92% |
| bronze_social_comments | 17 | 7 | 9 | 17.65% |
| bronze_engagement_snapshots | 1463 | 363 | 30 | 7.04% |
| bronze_collection_runs | 60 | 60 | 60 | 1.67% |
| bronze_source_changes | 1697 | 700 | 32 | 19.86% |

Distinct entity keys include platform and record type. The 700 entities in source-change events are event subjects, not new market entities. All six observation-ID grains and entity/run/measurement keys have zero duplicates. Raw payload keys vary by source; the actual schemas have zero unexpected/missing contract columns. Raw-payload key frequencies are saved as descriptive evidence, not interpreted as contract violations.

Court observations are entirely PBPlus. Post observations: 1,215 YouTube and 231 Threads, reducing to 305 videos and 51 posts. Comment observations are 17 YouTube records reducing to five top-level comments and two replies. No collected Threads comments, OSM, Overture or other social platform records are present. Zero observed contribution is not proof of absent market supply/activity.

## Collection frequency, growth and outcomes

| Platform | Outcome | Runs | Returned | Accepted | Rejected |
| --- | --- | --- | --- | --- | --- |
| pbplus | success | 6 | 1687 | 1687 | 0 |
| threads | partial | 34 | 216 | 216 | 0 |
| threads | success | 5 | 15 | 15 | 0 |
| youtube | partial | 4 | 121 | 121 | 0 |
| youtube | success | 11 | 1111 | 1111 | 0 |

There are 22 successful and 38 partial runs; no failed-status run is stored. Threads is partial in 34/39 runs. Source-returned/accepted counts describe observation retrieval, not distinct entities; snapshots/change events are additional records and must not be added to accepted content as independent source results. Some partial runs return zero records.

| VN run day | Platform | Runs | Accepted observations |
| --- | --- | --- | --- |
| 2026-10-05 | pbplus | 1 | 337 |
| 2026-10-08 | pbplus | 4 | 1013 |
| 2026-10-08 | threads | 14 | 221 |
| 2026-10-08 | youtube | 6 | 321 |
| 2026-10-10 | pbplus | 1 | 337 |
| 2026-10-10 | threads | 2 | 0 |
| 2026-10-10 | youtube | 4 | 406 |
| 2026-10-11 | threads | 23 | 10 |
| 2026-10-11 | youtube | 4 | 402 |
| 2026-10-12 | youtube | 1 | 103 |

Collection-run evidence covers five local dates from October 5–12, with no run evidence on October 6, 7 or 9. Observation timestamps and run completion timestamps are separate: observation-level UTC daily growth is saved in findings.json and must not be relabeled as run dates. Inter-run medians/maxima (hours): PBPlus 6.47/61.93, Threads 0.11/55.70, YouTube 1.00/49.38. These are observed cadence, not a planned schedule or uptime denominator.

The largest post run supplies 100/1,446 observations (6.92%); largest comment run supplies 3/17 (17.65%). Across Threads, 221/231 post observations (95.67%) came from local October 8 runs; only ten came from October 11. Platform/run concentration is saved independently of publication time.

## Completeness, numeric and text distributions

| Dataset / field | NULL / rows | NULL rate |
| --- | --- | --- |
| `bronze_court_observations.court_count_raw` | 100/1687 | 5.93% |
| `bronze_court_observations.address_raw` | 5/1687 | 0.30% |
| `bronze_court_observations.price_raw` | 1687/1687 | 100.00% |
| `bronze_court_observations.latitude_raw` | 1687/1687 | 100.00% |
| `bronze_social_posts.language_raw` | 231/1446 | 15.98% |
| `bronze_social_posts.text_raw` | 1215/1446 | 84.02% |
| `bronze_social_posts.description_raw` | 231/1446 | 15.98% |
| `bronze_collection_runs.started_at` | 27/60 | 45.00% |
| `bronze_engagement_snapshots.share_count` | 1463/1463 | 100.00% |

YouTube stores title/description while Threads stores text; NULL in `text_raw` alone is not missing YouTube content. There are no missing publication timestamps in posts/comments. Source language metadata is vi=1,092, en=71, id=35, ko=17, NULL=231 observations; these are source labels, not language detection accuracy.

| Measure | Available n | Min | Median | Mean | p95 | Max |
| --- | --- | --- | --- | --- | --- | --- |
| Court count, all parseable observations | 1587 | 1 | 4 | 8.27 | 12.00 | 912 |
| Court count, latest distinct PBPlus entity | 317 | 1 | 4 | 8.27 | 12.00 | 912 |
| bronze_court_observations.description_raw_length | 337 | 1877 | 2683 | 2,748.23 | 3,574.60 | 6255 |
| bronze_social_posts.text_raw_length | 231 | 33 | 144 | 197.01 | 484.00 | 487 |
| bronze_social_posts.description_raw_length | 1215 | 0 | 347 | 503.35 | 1,448.50 | 2792 |
| bronze_social_posts.title_raw_length | 1215 | 18 | 76 | 73.05 | 99.00 | 100 |
| bronze_social_comments.comment_text_raw_length | 17 | 22 | 33 | 45.76 | 105.60 | 192 |

The latest 337 court-source entities have 317 parseable counts and 20 missing counts. No raw price is available, so no comparable price distribution exists. The maximum count is a literal source value `912` for Pickleball 163 Trần Hòa, present in five observations; this is a review flag, not five separate 912-court venues.

## Source changes and deletion evidence

| Platform | Entity type | Change type | Events |
| --- | --- | --- | --- |
| pbplus | Venue | changed | 337 |
| pbplus | Venue | new | 337 |
| threads | Post | changed | 180 |
| threads | Post | new | 51 |
| youtube | Comment | new | 7 |
| youtube | Video | changed | 480 |
| youtube | Video | new | 305 |

There are 700 NEW and 997 CHANGED events, zero deletion/closure tombstones. All 337 PBPlus entities have changed payload hashes, but the changed raw key is `description`; venue name, address, court count, price and operating-status projections did not change. Hash changes cannot be read as court-supply changes. 27 recorded local source artifacts are missing; stored Delta observations remain queryable, but original-file reinspection is incomplete. Retention-policy IDs/expiry are NULL on all 6,370 Bronze rows; this is a source-review gap recorded in the existing implementation report.

## Publication and engagement observation history

Post publication spans 2024-08-04T18:09:06Z–2026-10-12T05:00:13Z. Median publication-to-collection age is 1.05 days, p95 308.31, maximum 794.58. No post/comment is published after its collection timestamp. The publication span does not establish continuous historical observation coverage.

| Platform | Type | Snapshots | Content IDs | Median snapshots / ID | Single snapshot IDs | Median interval h | Maximum interval h |
| --- | --- | --- | --- | --- | --- | --- | --- |
| threads | post | 231 | 51 | 4 | 14 | 0.39 | 75.26 |
| youtube | video | 1215 | 305 | 3 | 75 | 0.44 | 56.28 |
| youtube | comment | 17 | 7 | 3 | 3 | 0.14 | 6.74 |

All 1,463 cumulative snapshots were observed during 2026-10-07T22:24:47Z–2026-10-12T05:28:49Z. Use one latest valid snapshot per content for cumulative comparisons, or consecutive positive-time intervals for deltas. Never sum snapshots. Availability: YouTube videos have views on 1,215/1,215, likes on 1,210/1,215 and comments on 1,214/1,215 snapshots. Threads has likes/quotes/replies on all 231, reposts on 225, and no views/comments/shares. YouTube comments have only likes. Shares are absent everywhere. Repeated Threads measurements have unchanged counters and do not prove independently refreshed source metrics.

## Analytical implications

The source profile supports observation-versus-entity counts, collection outcome/coverage diagnostics and platform-specific metric availability. Historical growth rates, complete national collection coverage, price trends and a comment-population census are unsupported. Expand permitted observation history and retrieve richer comments before interpreting trends. Full field NULL rates, empty-value counts, numeric quantiles, categorical frequencies and time bounds are in [profiles.json](evidence/profiles.json); exact findings and checks are in [findings.json](evidence/findings.json). Read logic: [read_snapshot.py](code/read_snapshot.py), [analyze.py](code/analyze.py).
