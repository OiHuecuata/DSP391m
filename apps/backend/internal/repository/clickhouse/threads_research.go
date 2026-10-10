package clickhouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/username/DSP391m/apps/backend/internal/config"
	"github.com/username/DSP391m/apps/backend/internal/domain"
)

var ErrResearchUnavailable = errors.New("research generation unavailable")

type ThreadsResearchRepository struct {
	settings config.ClickHouse
	http     *http.Client
}

func NewThreadsResearchRepository(settings config.ClickHouse) (*ThreadsResearchRepository, error) {
	if err := settings.Validate(); err != nil {
		return nil, err
	}
	return &ThreadsResearchRepository{settings: settings, http: &http.Client{Timeout: settings.Timeout}}, nil
}

func researchQuery[T any](ctx context.Context, r *ThreadsResearchRepository, sql string, params url.Values) ([]T, error) {
	if params == nil {
		params = url.Values{}
	}
	params.Set("wait_end_of_query", "1")
	params.Set("output_format_json_quote_64bit_integers", "0")
	params.Set("max_execution_time", "10")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.settings.URL()+"/?"+params.Encode(), strings.NewReader(sql+" FORMAT JSONEachRow"))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(r.settings.User, r.settings.Password)
	response, err := r.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("research query HTTP %d", response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 8<<20))
	decoder.DisallowUnknownFields()
	rows := make([]T, 0)
	for {
		var row T
		err := decoder.Decode(&row)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (r *ThreadsResearchRepository) Generation(ctx context.Context, id string) (domain.ResearchManifest, error) {
	condition := "status IN ('COMPLETE','INACTIVE_PARTIAL')"
	params := url.Values{}
	if id != "" {
		condition += " AND generation_id={generation:String}"
		params.Set("param_generation", id)
	}
	rows, err := researchQuery[struct {
		Manifest string `json:"manifest_json"`
	}](ctx, r, "SELECT manifest_json FROM "+r.settings.Database+".research_generations WHERE "+condition+" ORDER BY completed_at DESC, generation_id DESC LIMIT 1", params)
	if err != nil {
		return domain.ResearchManifest{}, err
	}
	if len(rows) != 1 {
		return domain.ResearchManifest{}, ErrResearchUnavailable
	}
	var manifest domain.ResearchManifest
	if err = json.Unmarshal([]byte(rows[0].Manifest), &manifest); err != nil {
		return manifest, err
	}
	if manifest.Scope != "THREADS_EXPLORATORY" && manifest.Status != "COMPLETE" {
		return manifest, ErrResearchUnavailable
	}
	if manifest.ValidUntil != "" {
		expires, err := time.Parse(time.RFC3339Nano, manifest.ValidUntil)
		if err != nil || !expires.After(time.Now()) {
			return manifest, ErrResearchUnavailable
		}
	}
	return manifest, nil
}

func (r *ThreadsResearchRepository) EDA(ctx context.Context, f domain.ThreadsFilters) (domain.ThreadsEDA, error) {
	manifest, err := r.Generation(ctx, f.Generation)
	if err != nil {
		return domain.ThreadsEDA{}, err
	}
	params := url.Values{"param_generation": {manifest.GenerationID}}
	conditions := []string{"generation_id={generation:String}"}
	add := func(field, value, expression string) {
		if value != "" {
			params.Set("param_"+field, value)
			conditions = append(conditions, expression)
		}
	}
	add("from", f.From, "published_date_vn>={from:String}")
	add("to", f.To, "published_date_vn<={to:String}")
	add("platform", f.Platform, "platform={platform:String}")
	add("locality", f.Locality, "(has(location_terms,{locality:String}) OR has(admin_unit_ids,{locality:String}))")
	add("resolution", f.Resolution, "geo_resolution={resolution:String}")
	add("intent", f.Intent, "has(intent_labels,{intent:String})")
	add("topic", f.Topic, "has(topic_labels,{topic:String})")
	add("origin", f.Origin, "origin={origin:String}")
	if f.CollectionStatus != "" {
		params.Set("param_coverage", f.CollectionStatus)
		conditions = append(conditions, "collection_run_id IN (SELECT collection_run_id FROM "+r.settings.Database+".research_threads_collection_eda_v1 WHERE generation_id={generation:String} AND collection_status={coverage:String})")
	}
	base := " FROM " + r.settings.Database + ".research_threads_content_eda_v1 WHERE " + strings.Join(conditions, " AND ")
	metric := f.Metric
	if metric == "" {
		metric = "likes"
	}
	// SQL identifiers come exclusively from this whitelist, never request strings.
	if !map[string]bool{"likes": true, "comments": true, "reposts": true, "quotes": true, "views": true, "shares": true}[metric] {
		return domain.ThreadsEDA{}, errors.New("invalid metric")
	}
	result := domain.ThreadsEDA{Generation: manifest, Metric: metric, Bucket: f.Bucket, ClassificationStatus: "EXPLORATORY_UNEVALUATED", CollectionScope: "GENERATION_ALL_RUNS"}
	rows, err := researchQuery[domain.ThreadsOverview](ctx, r, "SELECT countIf(content_type='Post') AS unique_posts, countIf(content_type='Comment') AS replies, sum(observation_count) AS observations, countIf(geo_resolution IN ('Resolved','Mixed')) AS resolved_content, countIf(notEmpty(location_terms)) AS content_with_mentions, countIf(isNotNull("+metric+")) AS content_with_metric, count() AS metric_denominator, maxOrNull(latest_observed_at) AS freshness"+base, params)
	if err != nil {
		return result, err
	}
	if len(rows) > 0 {
		result.Overview = rows[0]
	}
	period := "published_date_vn"
	switch f.Bucket {
	case "week":
		period = "week_start_vn"
	case "month":
		period = "month_vn"
	case "", "day":
	default:
		return result, errors.New("invalid bucket")
	}
	result.Temporal, err = researchQuery[domain.ThreadsTemporal](ctx, r, "SELECT "+period+" AS period, countIf(content_type='Post') AS posts, countIf(content_type='Comment') AS replies, sumOrNull(if(content_type='Post',"+metric+",NULL)) AS metric_total, countIf(content_type='Post' AND isNotNull("+metric+")) AS metric_denominator"+base+" AND "+period+" IS NOT NULL GROUP BY period ORDER BY period", params)
	if err != nil {
		return result, err
	}
	frequency := func(expression, extra string) ([]domain.ThreadsFrequency, error) {
		return researchQuery[domain.ThreadsFrequency](ctx, r, "SELECT "+expression+" AS label, uniqExact(content_id) AS content_count, uniqExactIf(content_id,content_type='Post') AS post_count, uniqExactIf(content_id,content_type='Comment') AS reply_count"+base+extra+" GROUP BY label ORDER BY content_count DESC,label LIMIT 100", params)
	}
	if result.PublicationHours, err = frequency("toString(publication_hour_vn)", " AND publication_hour_vn IS NOT NULL"); err != nil {
		return result, err
	}
	if result.Geography, err = frequency("arrayJoin(location_terms)", ""); err != nil {
		return result, err
	}
	if result.Intents, err = frequency("arrayJoin(intent_labels)", ""); err != nil {
		return result, err
	}
	if result.Topics, err = frequency("arrayJoin(topic_labels)", ""); err != nil {
		return result, err
	}
	if result.Hashtags, err = frequency("arrayJoin(hashtags)", ""); err != nil {
		return result, err
	}
	if result.Distribution, err = frequency("multiIf(isNull("+metric+"),'Unavailable',"+metric+"=0,'0',"+metric+"<10,'1–9',"+metric+"<50,'10–49',"+metric+"<100,'50–99','100+')", " AND content_type='Post'"); err != nil {
		return result, err
	}
	delta := "NULL"
	if map[string]bool{"likes": true, "comments": true, "reposts": true, "quotes": true}[metric] {
		delta = metric + "_delta"
	}
	result.TopContent, err = researchQuery[domain.ThreadsTopContent](ctx, r, "SELECT content_id,source_url AS url,"+metric+" AS metric,"+delta+" AS metric_delta,content_age_hours AS age_hours,snapshot_count AS snapshots,latest_observed_at AS observed_at"+base+" AND content_type='Post' AND "+metric+" IS NOT NULL ORDER BY "+metric+" DESC,content_id LIMIT 20", params)
	if err != nil {
		return result, err
	}
	result.Collections, err = researchQuery[domain.ThreadsCollection](ctx, r, "SELECT collection_run_id,origin,collection_status,historical_window_status,unique_content_count,planned_queries,successful_queries,failed_queries,requests_used,artifact_count,last_success_at,query_terms FROM "+r.settings.Database+".research_threads_collection_eda_v1 WHERE generation_id={generation:String} ORDER BY collection_run_id", params)
	return result, err
}
