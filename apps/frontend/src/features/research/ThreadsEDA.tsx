"use client";

import { FormEvent, useEffect, useRef, useState } from "react";

type Frequency = { label: string; content_count: number; post_count: number; reply_count: number };
type Research = {
  generation: { generation_id: string; status: string; scope: string; as_of: string; metric_window: string; dependencies: string[]; dataset_status: Record<string, string> };
  overview: { unique_posts: number; replies: number; observations: number; resolved_content: number; content_with_mentions: number; content_with_metric: number; metric_denominator: number; freshness: string | null };
  temporal: { period: string; posts: number; replies: number; metric_total: number | null; metric_denominator: number }[];
  publication_hours: Frequency[]; geography: Frequency[]; intents: Frequency[]; topics: Frequency[]; hashtags: Frequency[]; distribution: Frequency[];
  top_content: { content_id: string; url: string | null; metric: number | null; metric_delta: number | null; age_hours: number | null; snapshots: number; observed_at: string | null }[];
  collections: { collection_run_id: string; origin: string; collection_status: string; planned_queries: number; successful_queries: number; failed_queries: number; requests_used: number; last_success_at: string | null; query_terms: string[]; historical_window_status: string }[];
  metric: string; bucket: string; classification_status: string; collection_scope: string;
};

const intents = ["SEEK_COURT", "BOOKING_INTENT", "SEEK_PLAYERS", "SEEK_COACH", "PRICE_INQUIRY", "CAPACITY_COMPLAINT", "VENUE_FEEDBACK", "SUPPLY_ADVERTISEMENT", "SUPPLY_OPENING", "SUPPLY_CLOSURE", "EVENT_TOURNAMENT", "GENERAL_DISCUSSION", "IRRELEVANT"];
const display = (value: number | null | undefined) => value == null ? "Unavailable" : value.toLocaleString();

function Frequencies({ title, rows, note }: { title: string; rows: Frequency[]; note?: string }) {
  const maximum = Math.max(1, ...rows.map(row => row.content_count));
  return <section className="card eda-card"><h3>{title}</h3>{note && <p className="eda-note">{note}</p>}
    {!rows.length ? <p>No eligible evidence in this selection.</p> : <table><thead><tr><th scope="col">Category</th><th scope="col">Distinct content</th><th scope="col">Posts / replies</th></tr></thead>
      <tbody>{rows.map(row => <tr key={row.label}><th scope="row">{row.label.replaceAll("_", " ")}</th><td><span className="eda-bar" style={{ width: `${row.content_count / maximum * 100}%` }} />{display(row.content_count)}</td><td>{display(row.post_count)} / {display(row.reply_count)}</td></tr>)}</tbody></table>}
  </section>;
}

export function ThreadsEDA() {
  const [data, setData] = useState<Research | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [selection, setSelection] = useState("metric=likes&bucket=day&platform=threads");
  const [reload, setReload] = useState(0);
  const generation = useRef("");
  useEffect(() => {
    const controller = new AbortController();
    const params = new URLSearchParams(selection);
    if (generation.current) params.set("generation", generation.current);
    fetch(`/api/v2/research/threads/eda?${params}`, { signal: controller.signal, cache: "no-store" })
      .then(async response => {
        if (!response.ok) throw new Error(response.status === 404 ? "No validated Threads generation is available yet." : "The research service is unavailable. Please retry.");
        return response.json() as Promise<Research>;
      }).then(value => { generation.current = value.generation.generation_id; setData(value); setLoading(false); })
      .catch(reason => { if (!controller.signal.aborted) { setError(reason.message); setLoading(false); } });
    return () => controller.abort();
  }, [selection, reload]);
  function apply(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setLoading(true); setError(null);
    const params = new URLSearchParams();
    new FormData(event.currentTarget).forEach((value, key) => { if (String(value)) params.set(key, String(value)); });
    setSelection(params.toString());
  }
  const fixture = data?.collections.some(run => run.origin === "SYNTHETIC");
  return <div className="threads-eda" aria-busy={loading}>
    <div className="section-heading"><h2>Threads research</h2><p>Explore collection evidence, own-text signals, and native engagement snapshots.</p></div>
    <form className="card eda-filters" onSubmit={apply}>
      <label>From<input name="from" type="date" /></label><label>To<input name="to" type="date" /></label>
      <label>Platform<select name="platform"><option value="threads">Threads</option></select></label>
      <label>Province / city mention<input name="locality" placeholder="Exact mention or reviewed admin ID" maxLength={120} /></label>
      <label>Geographic resolution<select name="resolution"><option value="">All</option>{["Resolved", "Unresolved", "Mixed", "NoMention"].map(value => <option key={value}>{value}</option>)}</select></label>
      <label>Exploratory intent<select name="intent"><option value="">All</option>{intents.map(value => <option key={value} value={value}>{value.replaceAll("_", " ")}</option>)}</select></label>
      <label>Topic<select name="topic"><option value="">All</option>{intents.filter(value => !["GENERAL_DISCUSSION", "IRRELEVANT"].includes(value)).map(value => <option key={value} value={value.toLowerCase()}>{value.replaceAll("_", " ")}</option>)}</select></label>
      <label>Metric<select name="metric" aria-label="Metric" defaultValue="likes">{["likes", "comments", "reposts", "quotes", "views", "shares"].map(value => <option key={value} value={value}>{value === "comments" ? "Reported direct replies" : value}</option>)}</select></label>
      <label>Period<select name="bucket" defaultValue="day">{["day", "week", "month"].map(value => <option key={value}>{value}</option>)}</select></label>
      <label>Source evidence<select name="origin"><option value="">All</option><option value="PERMITTED_COLLECTION">Permitted collection</option><option value="SYNTHETIC">Synthetic fixture</option></select></label>
      <label>Collection coverage<select name="coverage"><option value="">All</option><option>PARTIAL</option><option>COMPLETE</option><option>FAILED</option></select></label>
      <button className="eda-button" type="submit">Apply filters</button>
      <button type="button" onClick={() => { setLoading(true); setError(null); generation.current = ""; setReload(value => value + 1); }}>Load latest generation</button>
    </form>
    {loading && <p role="status">Loading computed research data…</p>}{error && <p role="alert" className="card eda-notice">{error}</p>}
    {data && !error && <>
      <aside className="card eda-notice"><strong>{fixture ? "Synthetic fixture data" : "Threads collection sample"} · {data.generation.status === "COMPLETE" ? "Complete research generation" : "Partial, inactive research generation"}</strong>
        <p>Intent and relevance rules are exploratory and unevaluated. These samples describe social interest and collection coverage. They do not measure player demand.</p>
        <p>Administrative modes require reviewed versioned references. Unresolved place mentions have no verified physical coordinates.</p>
        <details><summary>Generation and evidence limits</summary><p className="eda-generation" data-testid="generation">{data.generation.generation_id}</p><p>Snapshots: {data.generation.metric_window} · as of {data.generation.as_of}</p><ul>{data.generation.dependencies.map(value => <li key={value}>{value.replaceAll("_", " ")}</li>)}</ul></details>
      </aside>
      <section className="eda-overview" aria-label="Collection overview">
        {[["Unique posts", data.overview.unique_posts], ["Replies collected", data.overview.replies], ["Observations", data.overview.observations], ["Content with place mentions", data.overview.content_with_mentions], ["Resolved content", data.overview.resolved_content], ["Content with selected metric", data.overview.content_with_metric]].map(([label, value]) => <div className="card eda-stat" key={String(label)}><span>{label}</span><strong data-testid={String(label).toLowerCase().replaceAll(" ", "-")}>{display(Number(value))}</strong></div>)}
      </section>
      <p className="eda-note">Metric availability: {display(data.overview.content_with_metric)} / {display(data.overview.metric_denominator)} selected posts and replies. Freshness: {data.overview.freshness ?? "Unavailable"}. Missing metrics remain unavailable.</p>
      <section className="card eda-card"><h3>Publication cohorts in Vietnam · {data.bucket || "day"}</h3><p className="eda-note">Post engagement uses one eligible snapshot per content; repeated cumulative counters are not added together. Replies have their own publication cohorts.</p>
        {!data.temporal.length ? <p>No dated content in this selection.</p> : <table><thead><tr><th>Period</th><th>Posts</th><th>Replies</th><th>{data.metric} on posts</th><th>Posts with metric</th></tr></thead><tbody>{data.temporal.map(row => <tr key={row.period}><th scope="row">{row.period}</th><td>{display(row.posts)}</td><td>{display(row.replies)}</td><td>{display(row.metric_total)}</td><td>{display(row.metric_denominator)}</td></tr>)}</tbody></table>}
      </section>
      <div className="eda-grid">
        <Frequencies title="Publication hour · Vietnam" rows={data.publication_hours} />
        <Frequencies title="Place mentions" rows={data.geography} note="Distinct content per raw place term. Content mentioning several places appears in several groups." />
        <Frequencies title="Exploratory intent" rows={data.intents} note="Multiple labels can apply to one content. Advertisements remain a separate category." />
        <Frequencies title="Topic evidence" rows={data.topics} />
        <Frequencies title="Hashtag frequencies" rows={data.hashtags} />
        <Frequencies title={`${data.metric} distribution on posts`} rows={data.distribution} note="Unavailable is separate from an observed zero." />
      </div>
      <section className="card eda-card"><h3>Top posts by {data.metric}</h3><p className="eda-note">Signed deltas compare consecutive recorded snapshots. A missing counter on either snapshot produces an unavailable delta.</p>
        <table><thead><tr><th>Content</th><th>Metric</th><th>Delta</th><th>Age (hours)</th><th>Snapshots</th><th>Last observed</th></tr></thead><tbody>{data.top_content.map(row => <tr key={row.content_id}><td>{row.url ? <a href={row.url} target="_blank" rel="noreferrer">View source post</a> : "Source URL unavailable"}</td><td>{display(row.metric)}</td><td>{display(row.metric_delta)}</td><td>{row.age_hours == null ? "Unavailable" : row.age_hours.toFixed(1)}</td><td>{display(row.snapshots)}</td><td>{row.observed_at ?? "Unavailable"}</td></tr>)}</tbody></table>
      </section>
      <section className="card eda-card"><h3>Acquisition runs</h3><p className="eda-note">Coverage below describes all runs in this pinned generation. Targets and public result limits do not establish exhaustive market coverage.</p>
        <table><thead><tr><th>Run / source evidence</th><th>Query success / planned</th><th>Failed</th><th>Requests</th><th>Status</th></tr></thead><tbody>{data.collections.map(run => <tr key={run.collection_run_id}><th scope="row">{run.collection_run_id}<small>{run.origin}</small></th><td>{display(run.successful_queries)} / {display(run.planned_queries)}</td><td>{display(run.failed_queries)}</td><td>{display(run.requests_used)}</td><td>{run.collection_status}</td></tr>)}</tbody></table>
        <details><summary>Queries and collection windows</summary>{data.collections.map(run => <div key={run.collection_run_id}><p>{run.query_terms.join(" · ")}</p><p>{run.historical_window_status.replaceAll("_", " ")}</p><p>Last successful fetch: {run.last_success_at ?? "Unavailable"}</p></div>)}</details>
      </section>
    </>}
  </div>;
}
