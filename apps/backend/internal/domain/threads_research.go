package domain

type ResearchTable struct {
	RowCount int64  `json:"row_count"`
	SHA256   string `json:"sha256"`
}
type ResearchManifest struct {
	GenerationID  string                   `json:"generation_id"`
	Status        string                   `json:"status"`
	Scope         string                   `json:"scope"`
	AsOf          string                   `json:"as_of"`
	MetricWindow  string                   `json:"metric_window"`
	ValidUntil    string                   `json:"valid_until"`
	DatasetStatus map[string]string        `json:"dataset_status"`
	Tables        map[string]ResearchTable `json:"tables"`
	Dependencies  []string                 `json:"dependencies"`
}
type ThreadsFilters struct {
	Generation, From, To, Platform, Locality, Resolution, Topic, Intent, Metric, Origin, CollectionStatus, Bucket string
}
type ThreadsOverview struct {
	UniquePosts         int64   `json:"unique_posts"`
	Replies             int64   `json:"replies"`
	Observations        int64   `json:"observations"`
	ResolvedContent     int64   `json:"resolved_content"`
	ContentWithMentions int64   `json:"content_with_mentions"`
	ContentWithMetric   int64   `json:"content_with_metric"`
	MetricDenominator   int64   `json:"metric_denominator"`
	Freshness           *string `json:"freshness"`
}
type ThreadsTemporal struct {
	Period            string `json:"period"`
	Posts             int64  `json:"posts"`
	Replies           int64  `json:"replies"`
	MetricTotal       *int64 `json:"metric_total"`
	MetricDenominator int64  `json:"metric_denominator"`
}
type ThreadsFrequency struct {
	Label        string `json:"label"`
	ContentCount int64  `json:"content_count"`
	PostCount    int64  `json:"post_count"`
	ReplyCount   int64  `json:"reply_count"`
}
type ThreadsTopContent struct {
	ContentID   string   `json:"content_id"`
	URL         *string  `json:"url"`
	Metric      *int64   `json:"metric"`
	MetricDelta *int64   `json:"metric_delta"`
	AgeHours    *float64 `json:"age_hours"`
	Snapshots   int64    `json:"snapshots"`
	ObservedAt  *string  `json:"observed_at"`
}
type ThreadsCollection struct {
	RunID                  string   `json:"collection_run_id"`
	Origin                 string   `json:"origin"`
	Status                 string   `json:"collection_status"`
	HistoricalWindowStatus string   `json:"historical_window_status"`
	UniqueContentCount     int64    `json:"unique_content_count"`
	PlannedQueries         int64    `json:"planned_queries"`
	SuccessfulQueries      int64    `json:"successful_queries"`
	FailedQueries          int64    `json:"failed_queries"`
	RequestsUsed           int64    `json:"requests_used"`
	ArtifactCount          int64    `json:"artifact_count"`
	LastSuccess            *string  `json:"last_success_at"`
	QueryTerms             []string `json:"query_terms"`
}
type ThreadsEDA struct {
	Generation           ResearchManifest    `json:"generation"`
	Overview             ThreadsOverview     `json:"overview"`
	Temporal             []ThreadsTemporal   `json:"temporal"`
	PublicationHours     []ThreadsFrequency  `json:"publication_hours"`
	Geography            []ThreadsFrequency  `json:"geography"`
	Intents              []ThreadsFrequency  `json:"intents"`
	Topics               []ThreadsFrequency  `json:"topics"`
	Hashtags             []ThreadsFrequency  `json:"hashtags"`
	Distribution         []ThreadsFrequency  `json:"distribution"`
	TopContent           []ThreadsTopContent `json:"top_content"`
	Collections          []ThreadsCollection `json:"collections"`
	Metric               string              `json:"metric"`
	Bucket               string              `json:"bucket"`
	ClassificationStatus string              `json:"classification_status"`
	CollectionScope      string              `json:"collection_scope"`
}
