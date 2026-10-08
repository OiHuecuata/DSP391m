export type SourceName = "threads" | "youtube" | "pbplus";
export type Metrics = { accepted?: number; discovered?: number; new_identities?: number; existing_identities?: number; errors?: number; videos?: number; comments?: number; queries?: number; published?: number; acknowledged?: number; consumed?: number; stopping_reason?: string; requests?: { budget: number; used: number; by_endpoint: Record<string, number> } };
export type Source = { enabled: boolean; configured: boolean; interval_seconds: number; status: string; queued: boolean; last_run_id?: string; last_started_at?: string; last_completed_at?: string; next_run_at?: string; last_error?: string; metrics?: Metrics };
export type Run = { run_id: string; source: SourceName; status: string; started_at: string; completed_at?: string; duration_seconds: number; error?: string; metrics?: Metrics; stages?: Record<string, { seconds: number }> };
export type Collection = { continuous: boolean; status: string; sources: Record<SourceName, Source>; runs: Run[]; active_run_id?: string; progress?: { stage?: string; accepted?: number; discovered?: number; queries_processed?: number; query_plan_count?: number; requests_used?: number; videos?: number; comments?: number; query?: string; metrics?: Metrics } };
export type Table = { name: string; exists: boolean; rows: number | null; version: number | null; updated_at: string | null; error?: string };
export type Operations = {
  pipeline: { status: string; basis: string; refresh_interval_seconds: number };
  collection: Collection;
  lakehouse: { status: string; backend: string; uri: string; readable: boolean; writable: boolean; physical_bytes: number; table_count: number; updated_at: string | null; refreshed_at: string; tables: Table[] };
  warehouse: { status: string; backend: string; host: string; database: string; connected: boolean; exists: boolean; table: string; source_activity_rows?: number | null; rows: number | null; distinct_locations: number | null; latest_snapshot_date: string | null; gold_generated_at: string | null; last_successful_load_at: string | null; error?: string };
};
export type SourceConfig = Record<SourceName, { enabled: boolean; interval_seconds: number }>;
