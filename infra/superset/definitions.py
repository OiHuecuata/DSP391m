"""Dashboard/chart requirements, derived from the verified EDA queries.

No data baselines are hardcoded. Warnings identify known analytical limits.
"""
from pathlib import Path
import json

PREFIX='DSP391m EDA v2 · '
MARKER='managed:dsp391m-eda-v2:'
GOLD={
 'court_supply_by_admin_version_v2':('snapshot_date','snapshot × version × administrative unit; eligible VERIFIED/PROBABLE, CLOSED excluded. Known courts are explicit counts; never combine versions/national/provinces.'),
 'social_engagement_daily_v1':('activity_date','VN publication date × version × unit × platform; relevant posts/videos only, one latest valid cumulative snapshot per content. Metric NULLs are unavailable.'),
 'social_intent_daily_v2':('activity_date','VN publication date × version × unit × platform × intent. Distinct within label; labels overlap and may include uncertain relevance. Rules uncalibrated.'),
 'social_comment_analytics_daily_v1':('activity_date','Comment VN publication date × platform × version × own unit. Collected comments/replies differ from source-reported video comments.'),
 'social_activity_by_admin_version_daily':('activity_date','VN publication date × version × unit. Distinct national content; per-province memberships overlap. Relevance decision coverage is not accuracy.'),
 'national_location_coverage_v1':('period_start','Unit × version × source × observed VN collection day. Missing query evidence is not no market. Planned-query ratios unavailable without plans.'),
 'venue_community_feedback_weekly_v1':('week_start','Canonical venue × publication week. Mention sample, not a venue rating; absent sentiment/complaint measurements remain NULL.'),
 'location_term_usage_monthly_v1':('month','Publication month × raw term × canonical target × raw term version × platform. Span counts differ from per-term distinct content; shares have same-target/platform/month denominators.'),
 'venue_discovery_trends_v1':('month','Province × version × first-observed month. First lake observation is not opening. Unverified candidates remain separate.'),
 'location_market_research_monthly_v1':('month','Publication month × version × unit. Supply is a real snapshot at/before month end and absent historical supply remains NULL. Social signals do not measure unmet demand.'),
}

def quote(v): return "'"+v.replace("'","''")+"'"

def sql_queries(db):
    result={}
    for p in sorted(Path('/app/eda/queries').glob('*.sql')):
        result[p.stem]='\n'.join(s for s in p.read_text().splitlines() if not s.startswith('--')).strip().rstrip(';').replace('dsp391m.',db+'.')
    if len(result)!=14: raise RuntimeError('Expected all 14 verified analytical queries')
    return result

def datasets(db):
    base=sql_queries(db)
    ref=json.loads(Path('/app/reference/threads_geography.v1.json').read_text())
    pairs={r['geo_id']:r['canonical_name'] for r in ref['locations']}
    def province(col):
        return 'CASE '+col+' '+' '.join(f'WHEN {quote(k)} THEN {quote(v)}' for k,v in sorted(pairs.items()))+' ELSE '+col+' END'
    result={}
    times={'01':'snapshot_date','02':'snapshot_date','03':'period_start','05':'activity_date','06':'activity_date','09':'month','10':'month','11':'month','13':'period_start','14':'publication_month'}
    for key,sql in base.items():
        prefix=key[:2]
        expr=[]
        if prefix in ('01','02','03','08','10','11'):
            expr.append(province('admin_unit_id')+' AS province')
        if prefix=='09':
            expr.append(province('canonical_location_id')+' AS province')
        sql='SELECT q.*'+(', '+', '.join(expr) if expr else '')+' FROM ('+sql+') AS q'
        result[key]={'sql':sql,'time':times.get(prefix),'description':f'Verified EDA SQL {key}. Native SQL grain and scope are fixed by the original query; NULL measurements stay unavailable. Province labels use the repository reference, not official boundaries.'}
    # Supplementary current Gold projections required by the user's dashboards.
    result['supply_detail']={'sql':f"SELECT *, {province('admin_unit_id')} AS province FROM {db}.court_supply_by_admin_version_v2 WHERE snapshot_date=(SELECT max(snapshot_date) FROM {db}.court_supply_by_admin_version_v2) AND admin_version IN ('VN_ADMIN_POST_2025','VN_ADMIN_PRE_2025')",'time':'snapshot_date','description':'Latest snapshot; select one administrative version. Known courts retain the reported outlier; national rows excluded.'}
    result['verification']={'sql':f"SELECT snapshot_date, 'PROBABLE' AS verification_status, probable_venue_count AS venues FROM {db}.court_supply_by_admin_version_v2 WHERE admin_version='NATIONAL' AND admin_unit_id='NATIONAL' AND snapshot_date=(SELECT max(snapshot_date) FROM {db}.court_supply_by_admin_version_v2) UNION ALL SELECT snapshot_date, 'VERIFIED', verified_venue_count FROM {db}.court_supply_by_admin_version_v2 WHERE admin_version='NATIONAL' AND admin_unit_id='NATIONAL' AND snapshot_date=(SELECT max(snapshot_date) FROM {db}.court_supply_by_admin_version_v2)",'time':'snapshot_date','description':'National latest eligible verification partition. Unverified candidates are not eligible supply.'}
    result['intent_geo']={'sql':f"SELECT activity_date, admin_version, admin_unit_id, {province('admin_unit_id')} AS province, platform, intent_type, post_count, comment_count, reply_count, unique_content_count FROM {db}.social_intent_daily_v2 WHERE admin_version IN ('VN_ADMIN_POST_2025','VN_ADMIN_PRE_2025')",'time':'activity_date','description':'One label/platform/publication-date/location group. Select one version; locality memberships and labels overlap.'}
    result['terms_all']={'sql':f"SELECT *, {province('canonical_location_id')} AS province, if(canonical_location_id='UNRESOLVED','UNRESOLVED','RESOLVED_TARGET') AS target_status FROM {db}.location_term_usage_monthly_v1",'time':'month','description':'Raw geographic spans grouped by publication month/term/target/platform. Target status is canonical-target assignment, not a distinct post geo-resolution rate.'}
    result['supply_social_coverage']={'sql':f"SELECT m.*, {province('m.admin_unit_id')} AS province, c.coverage_evidence_groups, c.missing_query_evidence_groups, c.partial_query_evidence_groups FROM {db}.location_market_research_monthly_v1 AS m LEFT JOIN (SELECT admin_version, admin_unit_id, toStartOfMonth(period_start) AS month, count() AS coverage_evidence_groups, countIf(collection_status='MISSING') AS missing_query_evidence_groups, countIf(collection_status='PARTIAL') AS partial_query_evidence_groups FROM {db}.national_location_coverage_v1 GROUP BY admin_version,admin_unit_id,month) AS c ON m.admin_version=c.admin_version AND m.admin_unit_id=c.admin_unit_id AND m.month=c.month WHERE m.admin_version IN ('VN_ADMIN_POST_2025','VN_ADMIN_PRE_2025') AND m.supply_coverage_status='OBSERVED_AS_OF_SNAPSHOT'",'time':'month','description':'Current observed monthly supply/social data with counts of source/province/day evidence groups, not market/exposure coverage. Fixed supply snapshot as-of; no historical substitution.'}
    # Preserve verified formulas at the daily source grain so native publication
    # filters apply before aggregation, including platform/province charts.
    result['posts_national_daily']={'sql':f"SELECT activity_date, platform, post_count AS relevant_posts, posts_with_views, posts_with_likes, posts_with_metrics_count FROM {db}.social_engagement_daily_v1 WHERE admin_version='NATIONAL' AND admin_unit_id='NATIONAL'",'time':'activity_date','description':'Daily source-grain form of verified query 04; publication filters apply before platform sums.'}
    result['intents_national_daily']={'sql':f"SELECT activity_date,platform,intent_type,post_count AS labeled_posts,comment_count AS labeled_comments,reply_count AS labeled_replies,unique_content_count AS content_with_label FROM {db}.social_intent_daily_v2 WHERE admin_version='NATIONAL' AND admin_unit_id='NATIONAL'",'time':'activity_date','description':'Daily source-grain form of verified query 07; label totals overlap.'}
    result['activity_geo_daily']={'sql':f"SELECT activity_date,admin_unit_id,{province('admin_unit_id')} AS province,relevant_post_count AS relevant_posts,relevant_comment_count AS relevant_comments_and_replies,unique_content_count AS dated_content_memberships FROM {db}.social_activity_by_admin_version_daily WHERE admin_version='VN_ADMIN_POST_2025'",'time':'activity_date','description':'Daily source-grain form of verified query 08; province memberships overlap nationally.'}
    return result

def chart(key,title,dataset,dimensions,metrics,viz='table',time=None,filters=None,status='READY',note=''):
    return dict(key=key,title=title,dataset=dataset,dimensions=dimensions,metrics=metrics,viz=viz,time=time,filters=filters or [],status=status,note=note)

def charts():
    return [
      chart('supply_venues','Probable venues by current province','01_supply_by_province',['province'],['probable_venue_count'],'echarts_timeseries_bar',note='Observed eligible probable supply; zero VERIFIED is possible. Fixed current version and latest snapshot.'),
      chart('supply_completeness','Known, unknown and conflicting court-count venues','02_court_count_completeness',['province'],['known_count_venue_count','unknown_count_venue_count','conflicting_count_venue_count'],'echarts_timeseries_bar',note='Counts partition venues. Missing court counts never become one.'),
      chart('supply_courts','Known courts by province — source outlier retained','supply_detail',['province'],['known_court_count'],'echarts_timeseries_bar',filters=[('admin_version','==','VN_ADMIN_POST_2025')],status='PARTIAL',note='A source-reported 912-count venue is retained. Review the source; sums do not certify court accuracy.'),
      chart('supply_verification','Eligible venue verification breakdown','verification',['verification_status'],['venues'],'pie',note='National latest VERIFIED/PROBABLE partition only. Unverified candidates excluded.'),
      chart('supply_table','Supply completeness and as-of evidence','supply_detail',['province','admin_version','snapshot_date'],['venue_count','known_court_count','court_count_coverage_rate'],filters=[('admin_version','==','VN_ADMIN_POST_2025')],status='PARTIAL',note='Coverage uses known-count venues / all eligible venues. One snapshot and one version.'),
      chart('dq_collection','Collection evidence by province, source and day','03_collection_evidence_matrix',['period_start','province','source_platform','collection_status','query_evidence_status'],[],note='MISSING means no target-query evidence, not zero market activity. Collection-day time.'),
      chart('dq_status','Collection evidence status by source','03_collection_evidence_matrix',['source_platform','collection_status'],['evidence_groups'],'echarts_timeseries_bar',note='Metric counts source/province/day groups, not distinct runs or sources.'),
      chart('dq_geo','Accepted observations and resolved current lineage','13_national_collection_geo_resolution',['period_start','source_platform'],['records_accepted','resolved_records','geo_resolution_rate'],status='PARTIAL',note='National observation lineage ratio, not market coverage or distinct-post resolution. Plans may be NULL.'),
      chart('dq_classification','Relevance classification coverage by publication date','05_classification_coverage',['activity_date'],['relevance_coverage'],'echarts_timeseries_line',time='activity_date',note='Ratio of classified relevance decisions / dated content; not accuracy or sentiment coverage.'),
      chart('social_platform','Relevant collected posts by platform','posts_national_daily',['platform'],['relevant_posts'],'echarts_timeseries_bar',note='Relevant posts/videos only. Native-platform units remain separate.'),
      chart('social_geo','Relevant social activity by current province','activity_geo_daily',['province'],['relevant_posts'],'echarts_timeseries_bar',note='Distinct within province; multi-location posts overlap across provinces. No national share from province sums.'),
      chart('social_dates','Relevant posts by VN publication date','05_classification_coverage',['activity_date'],['relevant_posts'],'echarts_timeseries_bar',time='activity_date',note='Collected publication inventory; absent dates are unobserved.'),
      chart('social_views','YouTube latest observed views by publication cohort','06_youtube_views_by_publication_cohort',['activity_date'],['latest_observed_views'],'echarts_timeseries_bar',time='activity_date',status='PARTIAL',note='One latest valid cumulative snapshot per video. Publication date is not measurement date; this is not daily view growth.'),
      chart('social_measurements','YouTube measurement provenance and availability','06_youtube_views_by_publication_cohort',['activity_date','oldest_group_measurement','newest_group_measurement'],['relevant_videos','videos_with_views','available_views_per_video'],status='PARTIAL',note='Cumulative views per available video; measurement timestamps are group-latest provenance, not historical curves.'),
      chart('intent_frequency','Intent labels by platform — counts overlap','intents_national_daily',['intent_type','platform'],['content_with_label'],'echarts_timeseries_bar',status='PARTIAL',note='Rule classifier accuracy uncalibrated. Includes uncertain relevance where not explicitly irrelevant; labels overlap.'),
      chart('intent_posts','Distinct posts within each intent label','intents_national_daily',['intent_type','platform'],['labeled_posts'],status='PARTIAL',note='Distinct within label. Summing labels does not give unique posts. No confirmed bookings.'),
      chart('intent_geo','Supported intent evidence by locality','intent_geo',['province','platform','intent_type'],['post_count','comment_count','reply_count'],filters=[('admin_version','==','VN_ADMIN_POST_2025')],status='PARTIAL',note='One administrative version; independently located comments only. Sparse court-seeking and no measured demand.'),
      chart('comment_availability','Collected comments, replies and classification availability','12_collected_comment_availability',['platform'],['collected_comments','collected_replies','relevant_comments_and_replies','sentiment_classified'],status='PARTIAL',note='EDA baseline has only seven comments/replies and no sentiment-classified comments; chart queries current counts. Not a sentiment-rating chart.'),
      chart('geo_terms','Place-name usage by month, platform and target','09_location_term_usage',['month','province','platform','location_term_raw_normalized','term_admin_version'],['mention_count','unique_content_count','term_usage_share'],status='READY',note='Native group shares use same location/platform/month span denominator. Term-level unique counts overlap.'),
      chart('geo_versions','Historical/current raw place-name references','terms_all',['term_admin_version','platform'],['mention_count'],'echarts_timeseries_bar',note='Raw term version, not separate PRE/POST copies of the same mention. Contextual version may be ambiguous.'),
      chart('geo_unresolved','Resolved targets versus unresolved geographic spans','terms_all',['target_status'],['mention_count'],'pie',status='PARTIAL',note='Publication-dated Gold geographic spans with/without canonical targets. This is not the 46.35% distinct-post resolution statistic.'),
      chart('geo_discovery','Venue first-observed month and province','10_first_observed_venues',['month','province'],['newly_discovered_venue_count','unverified_candidate_count'],status='PARTIAL',note='First lake observation, not opening. Only available months; unverified candidates are separate.'),
      chart('geo_publications','Collected publication-month inventory','14_publication_month_inventory',['publication_month'],['posts','comments','replies'],'echarts_timeseries_bar',time='publication_month',note='Publication inventory, not continuous historical collection or market seasonality.'),
      chart('market_pairs','Observed supply and relevant social signals — limited coverage','11_supply_social_pairs',['month','province','supply_as_of'],['venue_count','known_court_count','relevant_post_count','relevant_comment_count'],status='PARTIAL',note='Localities with both observed sample types only. No correlation, occupancy, causal or unmet-demand inference.'),
      chart('market_coverage','Province-level supply/social and query-evidence context','supply_social_coverage',['month','province','social_coverage_status','supply_coverage_status'],['venue_count','relevant_post_count','coverage_evidence_groups','missing_query_evidence_groups','partial_query_evidence_groups'],filters=[('admin_version','==','VN_ADMIN_POST_2025')],status='PARTIAL',note='MISSING evidence and NO_CONTENT_OBSERVED are distinct from no market. Source/day groups can overlap and are not an exposure denominator.'),
      chart('market_seek','Court-seeking evidence alongside observed supply','11_supply_social_pairs',['province','month'],['venue_count','known_court_count','court_seeking_post_count','court_seeking_comment_count'],status='PARTIAL',note='Tiny independently classified court-seeking sample; known courts retain source outlier. No demand gap or booking conversion.'),
    ]

DASHBOARDS=[
 ('court-supply','Court Supply EDA',['supply_venues','supply_completeness','supply_courts','supply_verification','supply_table'],'Observed eligible VERIFIED/PROBABLE supply; source-reported 912-court outlier is retained. Unknown court count is not one. Precise coordinates, comparable prices and source-level outlier subtraction are unavailable in serving.'),
 ('data-quality','Data Quality & Collection Coverage',['dq_collection','dq_status','dq_geo','dq_classification'],'Collection dates and publication dates differ. MISSING is no target-query evidence, not absent market. Resolved-record ratios are observation-lineage diagnostics; exact distinct-post resolution and query-plan success are unavailable in serving.'),
 ('social-activity','Social Activity & Engagement',['social_platform','social_geo','social_dates','social_views','social_measurements'],'Publication cohorts are separate from metric observation timestamps. Latest cumulative snapshots are already deduplicated per content. Source sampling is selective; do not combine views/likes across platforms or infer historical view accrual.'),
 ('social-intent','Social Intent & Community',['intent_frequency','intent_posts','intent_geo','comment_availability'],'Multi-label counts overlap; classifier accuracy is uncalibrated. Only seven comments/replies exist in the captured EDA baseline; current counts remain live. Reliable comment sentiment and venue ratings are unavailable.'),
 ('geo-temporal','Geographic & Temporal EDA',['geo_terms','geo_versions','geo_unresolved','geo_discovery','geo_publications'],'Province/term targets are not precise venue coordinates. Term spans are not distinct people/posts. Historical/current names retain their raw contextual version. Discovery is first observed in the lake, not opening.'),
 ('supply-social','Supply vs Social Signals',['market_pairs','market_coverage','market_seek'],'PARTIAL exploratory tables only. Historical supply is missing in most months. Social signals do not measure unmet demand, occupancy, booking conversion or player population. No correlation is presented.'),
]
