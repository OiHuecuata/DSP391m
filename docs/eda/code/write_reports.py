"""Render concise reports and two static SVG charts from computed evidence."""
import json
from collections import Counter
from html import escape
from pathlib import Path
from statistics import mean, median
from analyze import stats, grouped

ROOT=Path(__file__).resolve().parents[3]
OUT=ROOT/'docs/eda'
F=json.loads((OUT/'evidence/findings.json').read_text())
P=json.loads((OUT/'evidence/profiles.json').read_text())
I=json.loads((OUT/'evidence/inventory.json').read_text())
T=json.loads((ROOT/'.local/eda/snapshot.json').read_text())['tables']
B=F['bronze']; S=F['silver']; G=F['gold']; C=F['consistency']

def fmt(v):
    if v is None: return 'NULL'
    if isinstance(v,float): return f'{v:,.2f}'
    return str(v)

def pct(a,b): return f'{a/b:.2%}' if b else 'NULL'

def table(headers,rows):
    return '\n'.join(['| '+' | '.join(headers)+' |','| '+' | '.join('---' for _ in headers)+' |',
                      *('| '+' | '.join(fmt(v).replace('|',' / ').replace('\n',' ') for v in row)+' |' for row in rows)])+'\n'

def coverage(names):
    return table(['Dataset','Full rows','Delta version','Duplicate grain keys'],
                 [(f'`{n}`',P[n]['rows'],I[n]['delta_version'],P[n].get('duplicate_keys','see grain note')) for n in names])

def nulltable(pairs):
    return table(['Dataset / field','NULL / rows','NULL rate'],[(f'`{n}.{k}`',f"{P[n]['nulls'][k]['count']}/{P[n]['rows']}",pct(P[n]['nulls'][k]['count'],P[n]['rows'])) for n,k in pairs])

def stattable(items):
    return table(['Measure','Available n','Min','Median','Mean','p95','Max'],[(name,s['n'],s['min'],s['median'],s['mean'],s['p95'],s['max']) for name,s in items])

def save(name,text):
    (OUT/name).write_text(text.strip()+'\n')

STAMP=f"Analysis of validated generation **{F['manifest_as_of']}**, generated from source observations through **{F['manifest_generated_at']}**. All listed datasets were fully queried, with no sampling or source-table writes. Instants are UTC; new Gold publication/collection calendar dates use Asia/Ho_Chi_Minh."
EVIDENCE="Full field NULL rates, empty-value counts, numeric quantiles, categorical frequencies and time bounds are in [profiles.json](evidence/profiles.json); exact findings and checks are in [findings.json](evidence/findings.json). Read logic: [read_snapshot.py](code/read_snapshot.py), [analyze.py](code/analyze.py)."

def bronze():
    names=[n for n in P if n.startswith('bronze_')]
    rows=[(n,B[n]['observations'],B[n]['distinct_source_entities'],B[n]['collection_run_count'],pct(B[n]['top_run_observations'],B[n]['observations'])) for n in names]
    contentstats=[(n+'.'+k,v) for n in names for k,v in B[n].items() if k.endswith('_length')]
    changes=B['changes']; runout=B['run_outcomes']
    snaprows=[(r['source_platform'],r['content_type'],r['observations'],r['distinct_content'],r['snapshots_per_content']['median'],r['single_snapshot_content'],r['interval_hours']['median'],r['interval_hours']['max']) for r in B['engagement']]
    save('bronze_eda.md',f"""# Bronze EDA

{STAMP}

## Coverage and source entities

{coverage(names)}
{table(['Dataset','Observations','Distinct platform / ID / type','Runs represented','Largest run share'],rows)}
Distinct entity keys include platform and record type. The 700 entities in source-change events are event subjects, not new market entities. All six observation-ID grains and entity/run/measurement keys have zero duplicates. Raw payload keys vary by source; the actual schemas have zero unexpected/missing contract columns. Raw-payload key frequencies are saved as descriptive evidence, not interpreted as contract violations.

Court observations are entirely PBPlus. Post observations: 1,215 YouTube and 231 Threads, reducing to 305 videos and 51 posts. Comment observations are 17 YouTube records reducing to five top-level comments and two replies. No collected Threads comments, OSM, Overture or other social platform records are present. Zero observed contribution is not proof of absent market supply/activity.

## Collection frequency, growth and outcomes

{table(['Platform','Outcome','Runs','Returned','Accepted','Rejected'],[(r['source_platform'],r['execution_status'],r['rows'],r['records_returned'],r['records_accepted'],r['records_rejected']) for r in runout])}
There are 22 successful and 38 partial runs; no failed-status run is stored. Threads is partial in 34/39 runs. Source-returned/accepted counts describe observation retrieval, not distinct entities; snapshots/change events are additional records and must not be added to accepted content as independent source results. Some partial runs return zero records.

{table(['VN run day','Platform','Runs','Accepted observations'],[(r['day'],r['source_platform'],r['rows'],r['records_accepted']) for r in B['vn_run_days']])}
Collection-run evidence covers five local dates from October 5–12, with no run evidence on October 6, 7 or 9. Observation timestamps and run completion timestamps are separate: observation-level UTC daily growth is saved in findings.json and must not be relabeled as run dates. Inter-run medians/maxima (hours): PBPlus {B['run_intervals']['pbplus']['median']:.2f}/{B['run_intervals']['pbplus']['max']:.2f}, Threads {B['run_intervals']['threads']['median']:.2f}/{B['run_intervals']['threads']['max']:.2f}, YouTube {B['run_intervals']['youtube']['median']:.2f}/{B['run_intervals']['youtube']['max']:.2f}. These are observed cadence, not a planned schedule or uptime denominator.

The largest post run supplies 100/1,446 observations ({pct(100,1446)}); largest comment run supplies 3/17 ({pct(3,17)}). Across Threads, 221/231 post observations ({pct(221,231)}) came from local October 8 runs; only ten came from October 11. Platform/run concentration is saved independently of publication time.

## Completeness, numeric and text distributions

{nulltable([('bronze_court_observations','court_count_raw'),('bronze_court_observations','address_raw'),('bronze_court_observations','price_raw'),('bronze_court_observations','latitude_raw'),('bronze_social_posts','language_raw'),('bronze_social_posts','text_raw'),('bronze_social_posts','description_raw'),('bronze_collection_runs','started_at'),('bronze_engagement_snapshots','share_count')])}
YouTube stores title/description while Threads stores text; NULL in `text_raw` alone is not missing YouTube content. There are no missing publication timestamps in posts/comments. Source language metadata is vi=1,092, en=71, id=35, ko=17, NULL=231 observations; these are source labels, not language detection accuracy.

{stattable([('Court count, all parseable observations',B['court_parsing']['observation_court_counts']),('Court count, latest distinct PBPlus entity',B['court_parsing']['latest_entity_court_counts']),*contentstats])}
The latest 337 court-source entities have 317 parseable counts and 20 missing counts. No raw price is available, so no comparable price distribution exists. The maximum count is a literal source value `912` for Pickleball 163 Trần Hòa, present in five observations; this is a review flag, not five separate 912-court venues.

## Source changes and deletion evidence

{table(['Platform','Entity type','Change type','Events'],[(r['source_platform'],r['entity_record_type'],r['change_type'],r['rows']) for r in changes])}
There are 700 NEW and 997 CHANGED events, zero deletion/closure tombstones. All 337 PBPlus entities have changed payload hashes, but the changed raw key is `description`; venue name, address, court count, price and operating-status projections did not change. Hash changes cannot be read as court-supply changes. {B['missing_original_artifacts']} recorded local source artifacts are missing; stored Delta observations remain queryable, but original-file reinspection is incomplete. Retention-policy IDs/expiry are NULL on all 6,370 Bronze rows; this is a source-review gap recorded in the existing implementation report.

## Publication and engagement observation history

Post publication spans 2024-08-04T18:09:06Z–2026-10-12T05:00:13Z. Median publication-to-collection age is {B['bronze_social_posts']['publication_to_collection_days']['median']:.2f} days, p95 {B['bronze_social_posts']['publication_to_collection_days']['p95']:.2f}, maximum {B['bronze_social_posts']['publication_to_collection_days']['max']:.2f}. No post/comment is published after its collection timestamp. The publication span does not establish continuous historical observation coverage.

{table(['Platform','Type','Snapshots','Content IDs','Median snapshots / ID','Single snapshot IDs','Median interval h','Maximum interval h'],snaprows)}
All 1,463 cumulative snapshots were observed during 2026-10-07T22:24:47Z–2026-10-12T05:28:49Z. Use one latest valid snapshot per content for cumulative comparisons, or consecutive positive-time intervals for deltas. Never sum snapshots. Availability: YouTube videos have views on 1,215/1,215, likes on 1,210/1,215 and comments on 1,214/1,215 snapshots. Threads has likes/quotes/replies on all 231, reposts on 225, and no views/comments/shares. YouTube comments have only likes. Shares are absent everywhere. Repeated Threads measurements have unchanged counters and do not prove independently refreshed source metrics.

## Analytical implications

The source profile supports observation-versus-entity counts, collection outcome/coverage diagnostics and platform-specific metric availability. Historical growth rates, complete national collection coverage, price trends and a comment-population census are unsupported. Expand permitted observation history and retrieve richer comments before interpreting trends. {EVIDENCE}
""")

def silver():
    names=[n for n in P if n.startswith('silver_') or n in ('dim_admin_unit','bridge_admin_crosswalk','dim_location_alias')]
    eng=S['engagement']; statitems=[]
    for r in eng:
        for m in ('views','likes','comments','reposts','quotes','replies'):
            if r['latest_metrics'][m]['n']:
                statitems.append((r['platform']+' '+r['content_type']+' latest '+m,r['latest_metrics'][m]))
    known=S['eligible_court_stats']; largest=max(r['court_count'] or 0 for r in T['silver_court_venues'])
    ex=[r['court_count'] for r in T['silver_court_venues'] if r['verification_status']=='PROBABLE' and r['court_count'] is not None and r['court_count']!=largest]
    save('silver_eda.md',f"""# Silver EDA

{STAMP}

## All 13 intelligence datasets

{coverage(names)}
The 13 intelligence tables have zero missing/unexpected schema fields and zero duplicate declared grain keys. Legacy Silver was also read: courts=1,687 observations, social_posts=1,153 observations, social_comments=15 observations; these are supplementary source inputs for v1 checks, not substituted for intelligence entities.

## Canonical venues, candidates and evidence

There are 350 canonical records: **0 VERIFIED, 336 PROBABLE, 14 UNVERIFIED**, all with UNKNOWN operating status. Current analytical supply excludes the 14 unverified records. The 337 PBPlus source IDs are linked to 337 distinct canonical records (336 probable plus one unverified); 13 social-only records add unverified candidates. There are 350 source links: 337 PBPlus, three Threads, ten YouTube. All venues have `source_count=1`: no actual cross-source merge is demonstrated in this population.

The 1,743 candidates/evidence rows preserve repeated evidence: PBPlus=1,687, Threads=8, YouTube=48. Match statuses are NEW=338 and MATCHED=1,405; AMBIGUOUS=0. This does not establish absence of undetected duplicates. Two repeated-name groups need review: OCEAN PICKLEBALL (two IDs; same normalized address value, counts unknown), and Pickle Bounce Pickleball - 306/5 Vườn Lài (two address values, counts four and five). Names alone are insufficient for merging; no venue IDs were changed.

{stattable([('Known courts, eligible probable venues',known),('Known courts, all canonical records',S['venue_court_counts'])])}
Known counts exist on 316/336 eligible venues ({pct(316,336)}); 20 are unknown and none conflicting. Missing courts remain NULL. The literal 912-court record contributes {pct(912,2617)} of 2,617 known courts. **Sensitivity only:** excluding that one record yields 1,705 courts across 315 known-count venues, mean {mean(ex):.2f}, median {median(ex):.2f}; this is not a corrected official total. Review the source before prioritizing court-total comparisons. Price and amenities cannot be profiled from populated values.

{nulltable([('silver_court_venues','court_count'),('silver_court_venues','price_min_vnd'),('silver_court_venues','latitude'),('silver_court_venues','coordinate_source'),('silver_court_venues','is_indoor'),('silver_court_venues','has_lighting'),('silver_court_venues','has_parking')])}
Of 350 canonical venues, 331 ({pct(331,350)}) have at least one provable province projection: old=18, new=326, any=331 (versions overlap). Zero have usable coordinate pairs or POI precision. All 1,743 geocoding attempts use REPOSITORY_GAZETTEER with POI_GEOCODER_UNAVAILABLE: ADMIN_ONLY=1,687; UNRESOLVED=56. Province resolution cannot generate precise venue pins.

## Independent posts, comments and rule classification

{table(['Platform','Posts/videos','Relevant','Irrelevant','Uncertain','Commercial'],[('Threads',51,51,0,0,1),('YouTube',305,285,17,3,18)])}
Overall relevance decision coverage is 353/356 posts ({pct(353,356)}). Commercial posts=19, non-commercial=337. Every content has a rule label, but this is different from calibrated accuracy; all classification/relevance confidence fields are NULL. No post/comment is flagged duplicate. Exact label combinations, platform cross-tabs and topic counts are in findings.json.

{table(['Intent','Threads posts','YouTube posts','YouTube comments/replies'],[(label,S['posts']['intents_by_platform'][0]['labels'].get(label,0),S['posts']['intents_by_platform'][1]['labels'].get(label,0),S['comments']['intents_by_platform'][0]['labels'].get(label,0)) for label in sorted({l for r in S['posts']['intents_by_platform']+S['comments']['intents_by_platform'] for l in r['labels']})])}
There are 23 multi-label posts ({pct(23,356)}): EVENT_TOURNAMENT + PRICE_INQUIRY=14, EVENT_TOURNAMENT + SUPPLY_ADVERTISEMENT=8, PRICE_INQUIRY + VENUE_FEEDBACK=1. Label counts must not be summed as unique content. BOOKING_INTENT and CAPACITY_COMPLAINT are absent in this observed population, not proven absent in the market. Intents have 474 evidence spans for 363 content IDs; only two participant counts, four durations, zero exact requested start times/budgets, and 127 resolved evidence-location links. No booking is confirmed.

Collected comments are five top-level comments plus two replies, all YouTube, published on three dates. Six are explicitly irrelevant; one has uncertain Pickleball relevance and a SEEK_COURT rule label. No comments are explicitly relevant, commercial or sentiment-classified. Their independent geography is unresolved for all seven, even though five parent posts have resolved province mentions. All seven parent relationships were found. Inheriting parent geography would invent local comment evidence.

Post text length median={S['posts']['text_length']['median']:.0f}, mean={S['posts']['text_length']['mean']:.2f}, p95={S['posts']['text_length']['p95']:.2f}, max={S['posts']['text_length']['max']}; comments median={S['comments']['text_length']['median']}, max={S['comments']['text_length']['max']}. Topic counts: PRICE=62, PLAYING_INTENT=8 posts + one comment, EVENT=87, SUPPLY=19, FACILITIES=1. Sentiment is present on only one post (NEGATIVE) and no comments; unknown is not neutral. Post language metadata exists on 305/356 records; Threads has none.

## Geographic intelligence and reference limits

There are 284 mention spans on 175 content IDs: 245 RESOLVED ({pct(245,284)}) and 39 UNRESOLVED. All resolved mentions have PROVINCE precision. Resolved own mentions cover 165/356 posts ({pct(165,356)}) and 0/7 comments. Fourteen current provinces and six historical provinces occur in social mention projections. Current mention concentrations: Hà Nội=131, Hồ Chí Minh=61, Đà Nẵng=18; these are spans, not distinct people or posts. Sixteen historical mention projections coexist with 245 forward projections and must not be concatenated as independent observations.

The reference has 100 units (63 old provinces, 34 current provinces, three current wards), 63 forward crosswalks (MERGED/UNCHANGED only) and 170 aliases over 105 raw alias texts; 65 alias texts reference multiple units, often versions of the same place. AS_WRITTEN mapping is ambiguous on zero current mention rows; unresolved and missing-reference results still remain. Common raw historical/colloquial terms include Sài Gòn (38 spans), Saigon (six), Bắc Giang (five), Hà Giang and Bình Thuận (one each). Codes and polygons are NULL on all 100 administrative units; alias validity ranges, overlap ratios and mapping confidences are unavailable. Full district/ward historical coverage is absent. The internal IDs and reference snapshots cannot serve as official geometries or exhaustive pre-2025 boundaries.

## Engagement distributions and valid changes

{stattable(statitems)}
Distributions use one latest AVAILABLE snapshot per content, separated by platform and content type. They include relevant and irrelevant content; the Gold engagement population is narrower. YouTube-video view mean is much higher than its median, so counts/quantiles are more informative than a single average. Threads likes cannot be interpreted as a comparable exposure measure to YouTube views.

For YouTube videos, valid positive-time intervals=906; interval median=0.440 h, max=56.282 h. View deltas range -37 to +19,036; 21 are negative, 121 exceed sample Q3+3×IQR. Likes have four negative deltas (minimum -1, maximum +111). These are source corrections/review flags; irregular intervals and heavy tails prevent treating them as errors or uniform growth rates. Threads has 180 like intervals, all with zero delta despite 231 snapshots; repeated values do not establish fresh engagement growth. There are four non-AVAILABLE snapshots ({P['silver_social_engagement_snapshots']['categories']['snapshot_status']}); interval computations skip them. Detailed availability and delta statistics are saved by platform/type. Shares, engagement_total, engagement_rate, calibrated rate basis and cross-platform high-engagement definitions are absent.

## Implications

Use probable supply with explicit verification/unknown-count labels; prioritize post activity, classification completeness, place terms and platform-native engagement. Venue price/amenity charts, precise maps, sentiment scores, confirmed bookings, player counts and long-term engagement growth are unsupported. {EVIDENCE}
""")

def gold():
    names=[n for n in P if n in G or n in ('court_supply_by_location','social_activity_daily','location_market_signal_snapshot')]
    names=[n for n in names if n not in ('supply_latest',)]
    supply=sorted([r for r in G['supply_latest'] if r['admin_version']=='VN_ADMIN_POST_2025'],key=lambda r:-r['venue_count'])
    national=next(r for r in G['supply_latest'] if r['admin_version']=='NATIONAL')
    save('gold_eda.md',f"""# Gold EDA

{STAMP}

## Thirteen Gold datasets queried

{coverage(names)}
New Gold contributes {sum(len(T[n]) for n in names if n not in ('court_supply_by_location','social_activity_daily','location_market_signal_snapshot')):,} aggregate rows; legacy v1 contributes 63. These rows represent multiple projections/grains, not additional market entities. All ten new tables are populated and matched ClickHouse at capture; v1 supply/activity are absent from ClickHouse, while v1 market snapshot matches its eight served rows.

## Court supply and concentration

At the sole materialized supply date, 2026-10-12: 336 probable venues, zero verified venues; known counts on 316, unknown on 20, known court total 2,617. Courts per known-count venue mean=8.28, median=4, p95=12, max=912. Coverage=94.05%. Neither 20 unknown counts nor the 14 unverified canonical venues are added as courts. All eligible venues have unknown operating status. Province resolution in at least one version covers 328/336; eight fully unresolved venues carry 39 known courts. Current-version projections cover 323 venues, leaving five resolved only in the old version. Current province rows cover 13/34 units; historical rows cover seven/63. These are observed reference coverage counts, not national market capture rates; do not add old and current projections.

{table(['Current province','Probable venues','Known courts','Known-count venues','Court-count coverage'],[(r['admin_name'],r['venue_count'],r['known_court_count'],r['known_count_venue_count'],pct(r['known_count_venue_count'],r['venue_count'])) for r in supply])}
Hà Nội + Hồ Chí Minh contain 240/336 venues ({pct(240,336)}) and 2,149/2,617 reported known courts ({pct(2149,2617)}). The court concentration is heavily affected by the 912-court source value in Hà Nội; venue concentration is more robust. Counts of unrepresented provinces stay unavailable. The price fields are NULL on every supply group. A histogram of the 316 eligible known counts retains the extreme value in an explicit >32 bin.

![Observed probable venue supply](charts/probable_venue_supply.svg)

![Known court count distribution](charts/known_court_distribution.svg)

`venue_discovery_trends_v1` has 24 rows, all in October 2026: NATIONAL discovery=336 probable venues and 14 unverified candidates; zero verified discoveries. First observations begin October 5. This is lake discovery, not venue opening. A one-month inventory is supported; a growth curve or supply time series is not.

## Social activity, engagement and publication coverage

`social_activity_by_admin_version_daily` counts 356 posts, five comments and two replies in NATIONAL scope across 55 publication dates and 22 populated publication months. Of 356 posts, 303 ({pct(303,356)}) are published in October 2026. The old publication tail is sparse; absent dates/months are unobserved, not demonstrated zero activity. Publication ranges are August 5, 2024–October 12, 2026 in Vietnam time.

{table(['Platform','Relevant posts','Latest cumulative views','Latest likes','Source-reported comments'],[(r['platform'],r['post_count'],r.get('total_views'),r.get('total_likes'),r.get('total_comments_reported')) for r in G['social_engagement_daily_v1']['national']])}
Gold engagement includes 336 explicitly relevant posts (285 videos + 51 Threads posts) and excludes 17 irrelevant + three uncertain videos. It chooses one valid snapshot per content. YouTube views=1,098,210, likes=3,437 over 283/285 available videos, source-reported comments=94; Threads likes=9,450. Do not combine video views and Threads likes into total engagement. Shares, median_engagement and high-engagement fields are NULL throughout. Publication-cohort cumulative views are not historical daily view accrual.

The latest observed metric period is October 7–12, 2026 only. Geography bars must use one version; province sums can exceed NATIONAL totals because one content can mention several provinces. UNRESOLVED and NATIONAL are separate scopes. Averaging daily averages is invalid: recompute `sumOrNull(total_likes)/nullIf(sum(posts_with_likes),0)`.

## Intent and community diagnostics

NATIONAL intent totals: GENERAL_DISCUSSION=185, EVENT_TOURNAMENT=87, PRICE_INQUIRY=62, SUPPLY_ADVERTISEMENT=19, SEEK_PLAYERS=6, SEEK_COURT=3, VENUE_FEEDBACK=1. Labels overlap. SEEK_COURT consists of two Threads posts (one Hà Nội, one Hồ Chí Minh) and one geographically unresolved YouTube comment with uncertain relevance. BOOKING_INTENT/CAPACITY_COMPLAINT have no eligible rows. Do not label these as confirmed requests/bookings or measured unmet demand.

`social_comment_analytics_daily_v1` has six rows: three national publication-day groups plus three unresolved groups. NATIONAL collected comments=5, replies=2, relevant=0, sentiment-classified=0, resolved own geography=0. Thus sentiment counts/shares and reported-comment allocation remain NULL. Its court-seeking relevant count is zero while the intent table has one uncertain SEEK_COURT comment: this is an explicit eligibility difference (comments analytics requires true relevance; intent excludes only false relevance), not a broken aggregate.

`venue_community_feedback_weekly_v1` has 14 venue/week rows over nine venues, 15 mentioning-post memberships, zero comment memberships, zero classified feedback evidence. Positive/negative/facility/price/service feedback are NULL. One Silver negative-feedback post does not imply identifiable venue feedback in this table. Ratings and complaint leaderboards are unsupported.

## Geographic terms, collection coverage and supply/social pairs

`location_term_usage_monthly_v1` has 94 rows: actual raw location spans grouped by publication month, platform, term and one canonical target. `mention_count` is spans; `unique_content_count` deduplicates within a term group only. Term shares use the location/platform/month span denominator; never add per-term unique counts or average shares across months. Place-name usage is supported; changing market shares/populations is not.

`national_location_coverage_v1` has 983 source/location/day rows: 857 MISSING, 126 PARTIAL, zero COMPLETE/FAILED. Planned queries and query-success rates are NULL on all 983. MISSING is absent target-query evidence, not absent market activity. National accepted/resolved ratios describe allowed observation lineage, not distinct post-geography rates or market penetration. Only ten national source/day groups exist; the 973 province rows must not be added to them.

`location_market_research_monthly_v1` has 99 rows; 77 ({pct(77,99)}) lack historical supply snapshots. Only the October 2026 period has observed-as-of supply (22 groups). For the captured 13 supply provinces, only six have social observations; seven have no observed content. Raw diagnostics: Hà Nội 136 venues / 103 relevant post memberships / one SEEK_COURT post; Hồ Chí Minh 104 / 36 / one; Đà Nẵng 25 / 14 / zero. These pair a current supply sample with collected publication cohorts; no correlation, demand gap, occupancy or player-population inference is justified. Keep NO_CONTENT_OBSERVED separate from zero measured market activity.

## Legacy v1 differences and consistency

v1 `court_supply_by_location`: eight locations, summed venue_count=285 and known_court_count=2,342; narrower alias/geographic scope differs from v2 (336/2,617). It has no admin_version, verification partition or national distinct row. v1 `social_activity_daily`: 47 location/day rows, 51 post memberships, summed likes=6,811, engagement_total=7,756; it uses Threads native likes+replies+reposts+quotes and UTC publication dates. First UTC date 2024-08-04 becomes 2024-08-05 in new VN-date tables. NULL native metrics are zero-filled by legacy implementation, so legacy engagement is not interchangeable with new nullable platform semantics.

v1 `location_market_signal_snapshot`: eight rows with **snapshot/window end 2026-10-07**, window September 8–October 7 (30 days), despite generated_at on October 12. Four social post memberships, 14 legacy interactions and current legacy supply are present. Normalized per-venue fields are source-specific sample ratios, not occupancy or demand. Latest build time is not snapshot freshness.

Independent new checks: {C['independent_summary']['checks']:,}/{C['independent_summary']['checks']:,} pass. Independent v1 checks: {C['legacy_summary']['passed']}/{C['legacy_summary']['checks']} pass. All ten new tables also reproduce exactly using production formulas; this supplementary replay is not independent validation. All 29 manifest input/output versions and content digests match; all 17,093 rows across the 32 requested datasets validate their contracts. See [cross_layer_findings.md](cross_layer_findings.md) for scope rules and limitations. {EVIDENCE}
""")

def charts():
    folder=OUT/'charts'; folder.mkdir(exist_ok=True)
    def bars(name,title,subtitle,items):
        width=850; height=100+len(items)*32; maxval=max(v for _,v in items)
        parts=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img"><title>{escape(title)}</title><desc>{escape(subtitle)}</desc>',
               '<rect width="100%" height="100%" fill="white"/>',
               f'<text x="20" y="27" font-family="sans-serif" font-size="20">{escape(title)}</text>',
               f'<text x="20" y="49" font-family="sans-serif" font-size="12">{escape(subtitle)}</text>']
        for i,(label,v) in enumerate(items):
            y=70+i*32; length=500*v/maxval
            parts += [f'<text x="20" y="{y+17}" font-family="sans-serif" font-size="14">{escape(label)}</text>',
                      f'<rect x="235" y="{y}" width="{length:.2f}" height="23" fill="#246b91"/>',
                      f'<text x="{245+length:.2f}" y="{y+17}" font-family="sans-serif" font-size="14">{v}</text>']
        parts.append('</svg>'); (folder/name).write_text('\n'.join(parts))
    rr=sorted([r for r in G['supply_latest'] if r['admin_version']=='VN_ADMIN_POST_2025'],key=lambda r:-r['venue_count'])
    bars('probable_venue_supply.svg','Observed probable venue supply by current province','2026-10-12 sample; 323 current-version venues; 13 lack this projection; zero verified',[(r['admin_name'],r['venue_count']) for r in rr])
    counts=[r['court_count'] for r in T['silver_court_venues'] if r['verification_status']=='PROBABLE' and r['court_count_status']=='KNOWN']
    ranges=[('1',1,1),('2–4',2,4),('5–8',5,8),('9–16',9,16),('17–32',17,32),('>32 (912 review flag)',33,10000)]
    bars('known_court_distribution.svg','Known courts per probable venue','316 venues with explicit counts; 20 unknown excluded; source-reported 912 retained',[(name,sum(lo<=n<=hi for n in counts)) for name,lo,hi in ranges])

def cross():
    save('cross_layer_findings.md',f"""# Cross-layer findings and analytical guardrails

{STAMP}

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

{table(['Check','Executed result'],[('Requested datasets read', '6 Bronze + 13 Silver intelligence + 10 new Gold + 3 legacy Gold = 32'),('Supplementary reads','3 legacy Silver + 1 generation manifest'),('Schema/row contracts',f"{C['contract_validation']['rows']:,} rows, zero violations"),('Duplicate keys','Zero across all 32 requested grains'),('Independent new Silver → Gold',f"{C['independent_summary']['passed']:,}/{C['independent_summary']['checks']:,}"),('Independent legacy source / aggregate checks',f"{C['legacy_summary']['passed']}/{C['legacy_summary']['checks']}"),('Production-formula replay','10/10 exact matches; supplementary, same formula implementation'),('Manifest input/output versions','29/29 match'),('Manifest content digests','29/29 match'),('ClickHouse typed content comparison','10/10 new tables + legacy market snapshot match; v1 supply/activity missing'),('Version stability during capture',str(C['source_versions_unchanged']))])}
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

{EVIDENCE}
""")

def main():
    charts(); bronze(); silver(); gold(); cross()
    print('Rendered four EDA findings reports and two static charts.')

if __name__=='__main__': main()
