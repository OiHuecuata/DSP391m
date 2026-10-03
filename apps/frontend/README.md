# DSP391m tracker

Next.js frontend for the existing system status API. The default view is
**System → Crawler → Threads**. Analysis and API tabs are coming-soon placeholders.

## Run locally

Start the existing Go backend from `apps/backend`:

```bash
go run ./cmd/server
```

Then, from `apps/frontend`:

```bash
npm ci
npm run dev
```

Open http://localhost:3000. The frontend requests `/api/v1/system/status` on its
own origin. A Next.js rewrite forwards that exact route to the backend, which
defaults to `http://127.0.0.1:8080`. No backend CORS changes are needed.

Set `BACKEND_URL` when the Go backend runs elsewhere, for example:

```bash
BACKEND_URL=http://backend:8080 npm run dev
```

For production, set the same value **when building**, then run `npm run start`.
Rewrites are compiled into the build. The backend URL must be reachable from the
Next.js server. The starter's Geist fonts require network access during a fresh
production build.

## Structure and behavior

- `src/services/system.ts`: abortable, uncached API access with a 10-second
  request timeout and runtime validation of the response.
- `src/features/system/types.ts`: typed system envelope, crawler details,
  query snapshots, and extensible component states.
- `src/features/system/config/crawlerSources.ts`: crawler IDs and display names.
- `src/features/system/hooks/useSystemStatus.ts`: fetches immediately, then
  five seconds after each completed request. Requests never overlap; switching
  away from the crawler tracker aborts the active request and clears the timer.
  Polling continues for idle/completed runs so new runs are discovered.
- `src/features/system/components`: shared tabs, badge, placeholders, tracker,
  and crawler panel. Tabs support arrow keys, Home, and End.

Add a source entry to `crawlerSources` to add a navigation option. When the
backend returns a component with that source's ID and `type: "crawler"`, the
same panel displays its runtime fields. No source-specific page is needed.

Progress uses `collected_count / target_count`, clamped to 0–100%. Query counts
are secondary metrics. A missing or zero target has no percentage. Missing
metrics use a dash rather than invented zeroes. Absent crawler components show
**No runtime status available**. Unknown states receive a neutral badge.

Connection failures show an error and retry automatically. The last successful
snapshot remains visible with an explicit stale-data notice until recovery.
Collector errors and query errors remain visible.

## Validation

From `apps/frontend`:

```bash
npm run lint
npm run typecheck
npm run build
npx playwright install chromium
npm run test:e2e
```

Playwright starts the production frontend on port 3100, so build first.
Most tests intercept the API only inside the test browser. They cover polling,
progress semantics, failures/recovery, missing sources, optional metrics,
unknown states, placeholders, keyboard navigation, a new source configuration,
and the mobile layout. The application contains no simulated runtime data.

With the real backend running, also run:

```bash
RUN_REAL_BACKEND=1 npm run test:e2e
```

This includes a test of the actual backend response through the frontend
rewrite, compares the displayed Threads metrics, and saves a dashboard
screenshot in `test-results`. Controlled running/failure responses do not
alter the collector's real status file or collection behavior.

## Backend dependencies

Threads reads the existing collector snapshot at
`apps/collector/.local/threads/status.json`; the backend's default path expects
it to be started from `apps/backend`. A live collector run is needed for real
running updates. PickleballPlus (`pbplus`) and TikTok (`tiktok`) need to publish
runtime snapshots and be exposed by the backend before metrics can appear.
Data Lake and ClickHouse components are not displayed or modified here.
