export type KnownComponentState =
  | "idle" | "running" | "success" | "healthy"
  | "failed" | "unavailable" | "not_configured";

export type ComponentState = KnownComponentState | (string & Record<never, never>);

export interface QueryStatus {
  query: string;
  kind?: string | null;
  geo_name?: string | null;
  status?: string | null;
  raw_count?: number | null;
  accepted_count?: number | null;
  total_collected?: number | null;
  error?: string | null;
}

// Optional fields also cover components with no runtime snapshot yet.
export interface CrawlerDetails extends Record<string, unknown> {
  seed_query?: string | null;
  sort?: string | null;
  target_count?: number | null;
  collected_count?: number | null;
  raw_count?: number | null;
  query_plan_count?: number | null;
  queries_processed?: number | null;
  queries_succeeded?: number | null;
  queries_failed?: number | null;
  current_query?: QueryStatus | null;
  last_query?: QueryStatus | null;
  started_at?: string | null;
  finished_at?: string | null;
  message?: string | null;
  error?: string | null;
}

export interface ComponentStatus<TDetails = Record<string, unknown>> {
  id: string;
  type: string;
  status: ComponentState;
  updated_at?: string | null;
  details?: TDetails | null;
}

export type CrawlerComponentStatus = ComponentStatus<CrawlerDetails> & { type: "crawler" };

export interface SystemStatus {
  components: ComponentStatus[];
}

export interface CrawlerSource {
  id: string;
  name: string;
}
