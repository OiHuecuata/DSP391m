# Silver and Gold requirements v1.0 implementation contracts

The new schemas are in `contracts/data/{silver,gold}/*_*.schema.json`. The existing hyphenated v1 contracts retain their original meaning and paths. Silver v3 lives under `silver/intelligence/`; the dataset names in the manifest are the specification names. Internal `discovery:*` administrative identifiers come from the repository reference and are **not official codes**.

## Processing, temporal scope and lineage

`build_intelligence` reads all six authoritative Bronze observation tables, applies the reviewed source-policy registry, explicit source tombstones, expiry and configured run exclusions, then validates Silver and Gold before mutation. It reads persisted Silver back before writing Gold. Successful publication records the Bronze and output Delta versions in `quality/intelligence_manifest`. The ClickHouse loader checks these versions, all Gold contracts, grain uniqueness and content digests before any publication; changed policy/contracts or a passed retention cutoff reject stale generations. Each table is staged and exchanged atomically using the existing ClickHouse client. A group of ten table exchanges is not one database transaction: interruption requires rerunning publication before using a coherent cross-table generation.

`lineage_ids` contains the contributing Bronze observation IDs (reference dimensions contain their source references). Gold adds `metric_definition_id`, `source_platforms`, `processing_version`, `generated_at` and `completeness_status`. Resolve any observation ID in Bronze to its platform, source record ID, collection run, query evidence, source URL and original allowed payload. The metric-definition identifier points to the corresponding dataset below. Definitions describe every field, including nullable values.

UTC instants use explicit `Z` timestamps. Publication dates, weeks, months, metric-observation dates and snapshot dates use `Asia/Ho_Chi_Minh`. Engagement `activity_date` is publication date; `snapshot_as_of` is a measurement instant. Undated social content remains in Silver and is excluded from date aggregates. Late observations select by source collection time, with observation ID as deterministic tie-breaker. Two contradictory snapshots at the same content/instant are marked conflicting and excluded from aggregate selection.

`generated_at` is the latest contributing Bronze collection instant, allowing reproducible analytics for unchanged inputs; the manifest separately records execution `as_of`. It is not claimed to be wall-clock job completion time. Supply snapshots use execution `as_of` date. Historical supply dates already materialized are retained and recomputed from still-permitted observations up to each date, so deletions propagate without discarding unrelated historical contributions. The monthly research table never inserts today's supply into an earlier social month.

No-op fingerprints include processing/contracts/reference versions, policies, run exclusions, the Vietnam date and permitted input digests. They skip all data writes for unchanged input. Content text hashes reuse classification results and evidence spans when only metrics change. Cached evidence identities and processing versions are checked against their content so interrupted writes cannot reuse stale spans. Changed projections use keyed Delta reconciliation with explicit schemas and preserve Delta transaction history. The current finite adapter reads at most 250,000 rows per dataset into the driver; changed batches recompute bounded aggregates, while only changed rows are merged. It does **not** yet provide distributed affected-partition recomputation at national production scale.

## Silver interpretation

Canonical venue IDs are hashes of exact normalized name plus numbered address, or name plus provenance-tagged POI coordinate, when available. Otherwise they use source identity. Persisted source links take precedence on updates. Names alone never merge venues. Ambiguous matches remain candidates without an arbitrary canonical match. Candidate/evidence IDs derive from observations, preserving all conflicting source observations. Canonical attributes use agreement across the latest observations of each source entity; disagreements become NULL and appear in `conflicting_fields`. Known court counts must be nonnegative explicit counts (including an observed zero); missing counts never become one. PBPlus/OSM/Overture named addressed observations are PROBABLE; VERIFIED requires explicit source verification evidence. Social-only candidates are UNVERIFIED. CLOSED venues are excluded from current supply; unknown operating status is exposed and remains observed probable supply.

Classification uses conservative Vietnamese rules. `classification_confidence`, `relevance_confidence`, intent confidence and extraction probability remain NULL; no model accuracy has been measured. Explicit Pickleball mentions support relevance, clearly unrelated content is false, and incomplete court context is uncertain. Supply advertisements exclude court-seeking, booking and price-inquiry demand labels. Sentiment is limited to explicit facility/service praise or criticism, otherwise NULL. Exact text groups are retained; only duplicate source-permalink/text identities are flagged duplicates. Independent authors with the same short message are not silently discarded. Comments and replies have their own content IDs and extraction; parent IDs represent relationships, never inherited geography or intent.

Intent evidence preserves the actual matched text offsets, optional unique mention reference, explicit participant count, duration and budget. Ambiguous multiple mentions leave the intent-location reference NULL. Relative or incomplete requested times remain text with unresolved precision and no exact timestamp. No booking is asserted as confirmed.

Engagement snapshots retain platform-native counters and availability metadata; missing is NULL. Select the latest AVAILABLE snapshot at or before `as_of` for each content. `content_age_hours = (observed_at - published_at)/3600`; impossible negative ages mark INVALID_TIME. Deltas subtract the preceding valid chronological snapshot only where both inputs exist. `views_per_hour = delta_views/elapsed_hours` only for positive elapsed time. Negative deltas remain SOURCE_CORRECTION. Total/rate definitions and high-engagement thresholds default to unknown rather than fabricated cross-platform semantics.

The geography dimensions load the 63/34 province references, three current wards and their aliases from `threads_geography.v1.json`. Reference source URLs are retained; codes and geometries remain NULL when absent. The historical reference is immediately before July 2025, not an exhaustive earlier validity history. Province crosswalks use the reference's explicit current-name mapping. Explicit evidence-bearing custom crosswalks can express splits and partial mappings without allocating unknown overlap ratios. AS_WRITTEN retains raw text and contextual candidates; HISTORICAL uses publication-period evidence; HARMONIZED projects forward to the post-2025 system. Broad modern merged names never resolve backward to a specific old province. Multiple forward targets remain ambiguous. Absent POI geocoders produce an actual attempt with provider, address hash, candidate admin units and dependency status, with no invented coordinates. Administrative coordinates never become venue POIs.

## Shared Gold counting rules

Each grouping uses distinct canonical venue/content IDs. Label counts may overlap and must not be summed to obtain unique content. All location groups retain `admin_version`; `UNRESOLVED` explicitly represents unlocated evidence. Social tables also emit `NATIONAL` scope groups using distinct content across all locations, rather than summing province groups. PRE groups use provable raw historical geography; POST groups use forward harmonization. This mixed comparison basis is explicitly labeled and does not allocate modern observations backward.

Nullable sums use available inputs and remain NULL if none exist. A zero count means an observed empty eligible set within the documented sample; it never proves no market activity. Common source-platform arrays and lineage describe the sample, not national market completeness.

## `court_supply_by_admin_version_v2`

Grain: `snapshot_date × admin_version × admin_unit_id`. Source: Silver canonical venues and their source/evidence links, after excluding UNVERIFIED and CLOSED venues.

| Field | Formula / denominator / completeness |
|---|---|
| `venue_count` | Distinct eligible canonical venue IDs; one six-court venue contributes one |
| `known_court_count` | Sum nonnegative known court counts; NULL when no venue has a known count |
| `known_count_venue_count` | Eligible venues with KNOWN count |
| `unknown_count_venue_count`, `conflicting_count_venue_count` | Eligible venues in each count-status category; these and known partition venue_count |
| `court_count_coverage_rate` | known_count_venue_count / venue_count; NULL for empty denominator |
| `verified_venue_count`, `probable_venue_count` | Distinct eligible venues in each verification state |
| `price_min_vnd`, `price_median_vnd` | Min and median of venue minimum prices with VND_PER_COURT_HOUR; NULL with no comparable price |
| `priced_venue_count`, `price_unit` | Number with comparable prices; unit NULL if none |
| `venue_last_observed_at` | Maximum contributing venue last_seen_at |
| `unverified_candidate_count` | Unverified evidence candidates linked to eligible venues; not additional supply |
| `snapshot_date` | Actual materialized supply as_of date; never a daily social event |

## `social_engagement_daily_v1`

Grain: `activity_date × admin_version × admin_unit_id × platform`. Source: relevant, nonduplicate Silver posts/videos plus one valid cumulative snapshot per content, and geographic mentions. Comments/replies do not enter post metrics.

| Field | Formula / denominator / completeness |
|---|---|
| `post_count` | Distinct relevant post/video IDs published on the VN date |
| `total_views`, `total_likes`, `total_comments_reported`, `total_shares` | Nullable sum of corresponding latest selected counters, never sum across snapshots |
| `posts_with_views`, `posts_with_likes`, `posts_with_comments`, `posts_with_shares` | Distinct posts with the corresponding selected metric |
| `posts_with_metrics_count` | Distinct posts with any available metric in the selected snapshot |
| `avg_likes_per_post`, `avg_comments_per_post` | Available sum / posts with that metric; NULL with zero coverage |
| `median_engagement` | Median of confirmed per-content engagement totals; NULL while source definitions are absent |
| `high_engagement_post_count`, `high_engagement_threshold` | Count against a configured platform threshold only where total semantics and threshold exist; otherwise NULL |
| `snapshot_as_of`, `metric_window` | Latest contributing measurement instant; LatestValidAtOrBeforeAsOf |

## `social_intent_daily_v2`

Grain: `activity_date × admin_unit_id × admin_version × platform × intent_type`. Source: independent Silver content labels, geo mentions and matched intent evidence. Explicitly irrelevant content is excluded; uncertain topic rules remain visibly uncalibrated.

| Field | Formula / denominator / completeness |
|---|---|
| `post_count`, `comment_count`, `reply_count` | Distinct IDs with the label in each disjoint record category |
| `unique_content_count` | Distinct IDs across the three categories in this label group |
| `evidence_count` | Distinct evidence IDs for the contributing content/label |
| `resolved_content_count` | Distinct content with resolved geographic evidence in scope; national scope independently deduplicates |
| `high_confidence_count` | NULL until measured/calibrated confidence is available |

## `social_comment_analytics_daily_v1`

Grain: `activity_date × platform × admin_unit_id × admin_version`, using the comment's publication date and own geography. Source: Silver comments/replies and their independent classifiers.

| Field | Formula / denominator / completeness |
|---|---|
| `collected_comment_count`, `collected_reply_count` | Distinct top-level comment and reply IDs respectively |
| `reported_comment_count` | NULL: video-level totals cannot be assigned to a comment's different geography/date |
| `relevant_comment_count` | Distinct explicitly relevant comments plus replies; eligibility denominator for shares |
| `positive_comment_count`, `negative_comment_count`, `neutral_comment_count` | Counts among sentiment-classified relevant content; NULL if none classified, never treating unknown sentiment as neutral |
| `sentiment_classified_count` | Number with a supported sentiment result |
| `court_seeking_comment_count`, `booking_intent_comment_count`, `price_inquiry_comment_count`, `capacity_complaint_count` | Distinct relevant comment/reply IDs carrying each independent label |
| `comments_with_resolved_geo` | Distinct collected comment/reply IDs with resolved own mentions |
| `court_seeking_comment_share` | court_seeking_comment_count / relevant_comment_count; NULL for empty denominator |
| `source_collection_status` | Collected sample, source-reported geography unallocatable; never an inferred complete comment census |

## `social_activity_by_admin_version_daily`

Grain: `activity_date × admin_version × admin_unit_id`. Source: nonduplicate dated Silver posts/comments/replies and geo mentions. `post_count`, `comment_count` and `reply_count` count disjoint distinct categories; `unique_content_count` counts their union. `relevant_post_count` and `relevant_comment_count` count explicitly relevant records (the latter includes replies). `classified_content_count` counts known relevance decisions; `classification_coverage_rate = classified_content_count / unique_content_count`. `geographic_interpretation` records the projection basis. NATIONAL groups independently count the union across locations.

## `national_location_coverage_v1`

Grain: `admin_unit_id × admin_version × source_platform × period_start × period_end`. Periods are actual VN collection days observed in Bronze. Source: Bronze collection runs/query metadata and accepted Bronze observation IDs resolved through Silver. The table creates MISSING rows for known province/source/day combinations without target-query evidence.

| Field | Formula / denominator / completeness |
|---|---|
| `planned_queries` | Explicit recorded plan count only; NULL when absent; attempted query count is not substituted |
| `successful_queries`, `failed_queries` | Recorded query success/failure statuses; NULL without query logs; one run/query occurrence is one attempt |
| `records_returned`, `records_accepted` | Source run counts nationally; recorded per-query counts at target scope, which may include overlapping query results |
| `resolved_records` | National accepted observation IDs intersecting resolved Silver lineage in the same source/run; target attribution remains NULL without record/query allocation |
| `query_success_rate` | successful_queries / planned_queries, only when both available |
| `geo_resolution_rate` | resolved_records / records_accepted in the same eligible scope, only when available |
| `last_success_at` | Latest successful run ended_at (or actual collected_at if absent) |
| `collection_status` | MISSING without evidence, FAILED for failed-only runs, PARTIAL with missing plans/logs/failures, COMPLETE only with recorded sufficient query evidence |
| `query_evidence_status`, `collection_run_ids` | Recorded plan/log availability and exact contributing run IDs |

These ratios measure pipeline quality, **not the fraction of the Vietnamese market collected**. Resolution reflects the permitted current-content projection; historical text-state/query allocation requires additional evidence.

## `venue_community_feedback_weekly_v1`

Grain: `venue_id × week_start` (Monday in Vietnam time). Source: content that explicitly mentions a uniquely identifiable exact canonical venue name; ambiguous names remain unlinked. `mentioning_post_count` and `mentioning_comment_count` are distinct post and comment/reply IDs. `positive_feedback_count` and `negative_feedback_count` count supported sentiment among explicit venue feedback, NULL with no classifications. `facility_complaint_count` counts explicit negative facility feedback, NULL without feedback classifiers. `booking_difficulty_count` counts independently classified capacity complaints. `price_complaint_count` and `service_complaint_count` remain NULL pending supported issue extractors. `source_count` counts platforms, `classified_evidence_count` counts explicit feedback records and `sentiment_coverage_rate = supported sentiment count / all mentioning content`. This is a sample of opinions, not an objective venue rating.

## `location_term_usage_monthly_v1`

Grain: `month × location_term_raw_normalized × canonical_location_id × term_admin_version × platform`. Source: actual raw geographic mention spans, linked to content publication month. Canonical location uses one provable harmonized province (or historical/UNRESOLVED), not duplicated PRE and POST projections. `post_mention_count` and `comment_mention_count` count spans by content category; `mention_count` sums them. `unique_content_count` independently deduplicates IDs. `term_usage_share = this term's mention_count / all terms' mention_count for the same canonical location/platform/month`. `resolution_coverage = resolved mentions / all mentions in the term group`. `term_admin_version` records the contextual raw version or AMBIGUOUS.

## `venue_discovery_trends_v1`

Grain: `admin_unit_id × admin_version × month`, month of first lake observation. Source: canonical venues with stable IDs and first_seen_at. `newly_discovered_venue_count` counts distinct VERIFIED/PROBABLE IDs first seen that month. `verified_discovery_count` and `probable_discovery_count` partition those IDs; `unverified_candidate_count` counts distinct UNVERIFIED canonical candidates separately. `first_seen_min/max` are actual observation bounds. `discovery_basis` explicitly says FIRST_OBSERVED_IN_LAKE_NOT_OPENING_DATE. All current venues are never recounted as new discoveries each month.

## `location_market_research_monthly_v1`

Grain: `month × admin_unit_id × admin_version`. Source: distinct validated Silver content and latest snapshots, the supply snapshot dated at/before that month end, and validated discovery aggregates. `venue_count`, `known_court_count`, `court_count_coverage_rate` and `supply_as_of` copy the selected supply snapshot, NULL without one. `relevant_post_count` and `relevant_comment_count` count distinct explicitly relevant records; comments include replies. `total_views/likes/shares` sum one selected snapshot per relevant post across the month, retaining NULL when no inputs. `court_seeking_post_count`, `court_seeking_comment_count`, `booking_intent_content_count` and `capacity_complaint_count` count distinct independently labeled relevant records. `newly_discovered_venue_count` comes from that month's discovery table, NULL without evidence. `source_count` counts contributing social, supply and discovery platforms; `classification_coverage_rate` uses all collected monthly content as denominator. `social_coverage_status` and `supply_coverage_status` distinguish observed samples and absent historical snapshots. Engagement remains online attention; booking intent is not a transaction or measured player demand.

## Retention and compatibility boundaries

Verified source tombstones suppress only affected source evidence; surviving independent venue evidence remains usable. Expired/disallowed source observations cannot appear in new derivatives. This does not vacuum Bronze, erase archived Delta versions, Kafka, collector files or backups; those remain explicit operator-reviewed retention dependencies. v1 contracts/rolling 30-day behavior are preserved. Reference/model/provider availability and finite batch scaling limits are reported rather than hidden behind schema completeness.
