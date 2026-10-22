# Silver EDA

Analysis of validated generation **2026-10-12T05:30:05.213076Z**, generated from source observations through **2026-10-12T05:28:49.458043Z**. All listed datasets were fully queried, with no sampling or source-table writes. Instants are UTC; new Gold publication/collection calendar dates use Asia/Ho_Chi_Minh.

## All 13 intelligence datasets

| Dataset | Full rows | Delta version | Duplicate grain keys |
| --- | --- | --- | --- |
| `silver_venue_candidates` | 1743 | 5 | 0 |
| `silver_court_venues` | 350 | 5 | 0 |
| `silver_venue_evidence` | 1743 | 5 | 0 |
| `silver_venue_source_links` | 350 | 5 | 0 |
| `silver_social_posts` | 356 | 5 | 0 |
| `silver_social_comments` | 7 | 5 | 0 |
| `silver_social_engagement_snapshots` | 1463 | 5 | 0 |
| `silver_social_geo_mentions` | 284 | 5 | 0 |
| `silver_geocoding_attempts` | 1743 | 5 | 0 |
| `silver_social_intent_evidence` | 474 | 5 | 0 |
| `dim_admin_unit` | 100 | 5 | 0 |
| `bridge_admin_crosswalk` | 63 | 5 | 0 |
| `dim_location_alias` | 170 | 5 | 0 |

The 13 intelligence tables have zero missing/unexpected schema fields and zero duplicate declared grain keys. Legacy Silver was also read: courts=1,687 observations, social_posts=1,153 observations, social_comments=15 observations; these are supplementary source inputs for v1 checks, not substituted for intelligence entities.

## Canonical venues, candidates and evidence

There are 350 canonical records: **0 VERIFIED, 336 PROBABLE, 14 UNVERIFIED**, all with UNKNOWN operating status. Current analytical supply excludes the 14 unverified records. The 337 PBPlus source IDs are linked to 337 distinct canonical records (336 probable plus one unverified); 13 social-only records add unverified candidates. There are 350 source links: 337 PBPlus, three Threads, ten YouTube. All venues have `source_count=1`: no actual cross-source merge is demonstrated in this population.

The 1,743 candidates/evidence rows preserve repeated evidence: PBPlus=1,687, Threads=8, YouTube=48. Match statuses are NEW=338 and MATCHED=1,405; AMBIGUOUS=0. This does not establish absence of undetected duplicates. Two repeated-name groups need review: OCEAN PICKLEBALL (two IDs; same normalized address value, counts unknown), and Pickle Bounce Pickleball - 306/5 Vườn Lài (two address values, counts four and five). Names alone are insufficient for merging; no venue IDs were changed.

| Measure | Available n | Min | Median | Mean | p95 | Max |
| --- | --- | --- | --- | --- | --- | --- |
| Known courts, eligible probable venues | 316 | 1 | 4.00 | 8.28 | 12.00 | 912 |
| Known courts, all canonical records | 317 | 1 | 4 | 8.27 | 12.00 | 912 |

Known counts exist on 316/336 eligible venues (94.05%); 20 are unknown and none conflicting. Missing courts remain NULL. The literal 912-court record contributes 34.85% of 2,617 known courts. **Sensitivity only:** excluding that one record yields 1,705 courts across 315 known-count venues, mean 5.41, median 4.00; this is not a corrected official total. Review the source before prioritizing court-total comparisons. Price and amenities cannot be profiled from populated values.

| Dataset / field | NULL / rows | NULL rate |
| --- | --- | --- |
| `silver_court_venues.court_count` | 33/350 | 9.43% |
| `silver_court_venues.price_min_vnd` | 350/350 | 100.00% |
| `silver_court_venues.latitude` | 350/350 | 100.00% |
| `silver_court_venues.coordinate_source` | 350/350 | 100.00% |
| `silver_court_venues.is_indoor` | 350/350 | 100.00% |
| `silver_court_venues.has_lighting` | 350/350 | 100.00% |
| `silver_court_venues.has_parking` | 350/350 | 100.00% |

Of 350 canonical venues, 331 (94.57%) have at least one provable province projection: old=18, new=326, any=331 (versions overlap). Zero have usable coordinate pairs or POI precision. All 1,743 geocoding attempts use REPOSITORY_GAZETTEER with POI_GEOCODER_UNAVAILABLE: ADMIN_ONLY=1,687; UNRESOLVED=56. Province resolution cannot generate precise venue pins.

## Independent posts, comments and rule classification

| Platform | Posts/videos | Relevant | Irrelevant | Uncertain | Commercial |
| --- | --- | --- | --- | --- | --- |
| Threads | 51 | 51 | 0 | 0 | 1 |
| YouTube | 305 | 285 | 17 | 3 | 18 |

Overall relevance decision coverage is 353/356 posts (99.16%). Commercial posts=19, non-commercial=337. Every content has a rule label, but this is different from calibrated accuracy; all classification/relevance confidence fields are NULL. No post/comment is flagged duplicate. Exact label combinations, platform cross-tabs and topic counts are in findings.json.

| Intent | Threads posts | YouTube posts | YouTube comments/replies |
| --- | --- | --- | --- |
| EVENT_TOURNAMENT | 3 | 84 | 0 |
| GENERAL_DISCUSSION | 39 | 146 | 0 |
| IRRELEVANT | 0 | 17 | 6 |
| PRICE_INQUIRY | 1 | 61 | 0 |
| SEEK_COURT | 2 | 0 | 1 |
| SEEK_PLAYERS | 5 | 1 | 0 |
| SUPPLY_ADVERTISEMENT | 1 | 18 | 0 |
| VENUE_FEEDBACK | 0 | 1 | 0 |

There are 23 multi-label posts (6.46%): EVENT_TOURNAMENT + PRICE_INQUIRY=14, EVENT_TOURNAMENT + SUPPLY_ADVERTISEMENT=8, PRICE_INQUIRY + VENUE_FEEDBACK=1. Label counts must not be summed as unique content. BOOKING_INTENT and CAPACITY_COMPLAINT are absent in this observed population, not proven absent in the market. Intents have 474 evidence spans for 363 content IDs; only two participant counts, four durations, zero exact requested start times/budgets, and 127 resolved evidence-location links. No booking is confirmed.

Collected comments are five top-level comments plus two replies, all YouTube, published on three dates. Six are explicitly irrelevant; one has uncertain Pickleball relevance and a SEEK_COURT rule label. No comments are explicitly relevant, commercial or sentiment-classified. Their independent geography is unresolved for all seven, even though five parent posts have resolved province mentions. All seven parent relationships were found. Inheriting parent geography would invent local comment evidence.

Post text length median=402, mean=515.56, p95=1391.00, max=2883; comments median=38, max=192. Topic counts: PRICE=62, PLAYING_INTENT=8 posts + one comment, EVENT=87, SUPPLY=19, FACILITIES=1. Sentiment is present on only one post (NEGATIVE) and no comments; unknown is not neutral. Post language metadata exists on 305/356 records; Threads has none.

## Geographic intelligence and reference limits

There are 284 mention spans on 175 content IDs: 245 RESOLVED (86.27%) and 39 UNRESOLVED. All resolved mentions have PROVINCE precision. Resolved own mentions cover 165/356 posts (46.35%) and 0/7 comments. Fourteen current provinces and six historical provinces occur in social mention projections. Current mention concentrations: Hà Nội=131, Hồ Chí Minh=61, Đà Nẵng=18; these are spans, not distinct people or posts. Sixteen historical mention projections coexist with 245 forward projections and must not be concatenated as independent observations.

The reference has 100 units (63 old provinces, 34 current provinces, three current wards), 63 forward crosswalks (MERGED/UNCHANGED only) and 170 aliases over 105 raw alias texts; 65 alias texts reference multiple units, often versions of the same place. AS_WRITTEN mapping is ambiguous on zero current mention rows; unresolved and missing-reference results still remain. Common raw historical/colloquial terms include Sài Gòn (38 spans), Saigon (six), Bắc Giang (five), Hà Giang and Bình Thuận (one each). Codes and polygons are NULL on all 100 administrative units; alias validity ranges, overlap ratios and mapping confidences are unavailable. Full district/ward historical coverage is absent. The internal IDs and reference snapshots cannot serve as official geometries or exhaustive pre-2025 boundaries.

## Engagement distributions and valid changes

| Measure | Available n | Min | Median | Mean | p95 | Max |
| --- | --- | --- | --- | --- | --- | --- |
| youtube video latest views | 305 | 0 | 431 | 3,687.55 | 9,416.40 | 194506 |
| youtube video latest likes | 303 | 0 | 2 | 11.82 | 42.80 | 543 |
| youtube video latest comments | 304 | 0 | 0.00 | 0.32 | 2.00 | 36 |
| threads post latest likes | 51 | 0 | 10 | 185.29 | 829.50 | 4617 |
| threads post latest reposts | 50 | 0 | 0.00 | 12.14 | 67.75 | 230 |
| threads post latest quotes | 51 | 0 | 0 | 0.33 | 1.00 | 8 |
| threads post latest replies | 51 | 1 | 8 | 13.20 | 36.00 | 63 |
| youtube comment latest likes | 7 | 0 | 0 | 0.14 | 0.70 | 1 |

Distributions use one latest AVAILABLE snapshot per content, separated by platform and content type. They include relevant and irrelevant content; the Gold engagement population is narrower. YouTube-video view mean is much higher than its median, so counts/quantiles are more informative than a single average. Threads likes cannot be interpreted as a comparable exposure measure to YouTube views.

For YouTube videos, valid positive-time intervals=906; interval median=0.440 h, max=56.282 h. View deltas range -37 to +19,036; 21 are negative, 121 exceed sample Q3+3×IQR. Likes have four negative deltas (minimum -1, maximum +111). These are source corrections/review flags; irregular intervals and heavy tails prevent treating them as errors or uniform growth rates. Threads has 180 like intervals, all with zero delta despite 231 snapshots; repeated values do not establish fresh engagement growth. There are four non-AVAILABLE snapshots ({'AVAILABLE': 1459, 'INVALID_TIME': 4}); interval computations skip them. Detailed availability and delta statistics are saved by platform/type. Shares, engagement_total, engagement_rate, calibrated rate basis and cross-platform high-engagement definitions are absent.

## Implications

Use probable supply with explicit verification/unknown-count labels; prioritize post activity, classification completeness, place terms and platform-native engagement. Venue price/amenity charts, precise maps, sentiment scores, confirmed bookings, player counts and long-term engagement growth are unsupported. Full field NULL rates, empty-value counts, numeric quantiles, categorical frequencies and time bounds are in [profiles.json](evidence/profiles.json); exact findings and checks are in [findings.json](evidence/findings.json). Read logic: [read_snapshot.py](code/read_snapshot.py), [analyze.py](code/analyze.py).
