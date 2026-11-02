# DSP391m tracker

Next.js frontend for the DSP391m data platform. The homepage at `/` follows the Figma home design and polls the existing Data Operations API every five seconds. It shows live system status, workspace navigation, and recent collection runs, with explicit loading, disconnected, and stale states.

The existing tracker is at `/collectors` (default view: **System → Crawler → Threads**), and collection controls remain at `/data-operations`. Analysis, Data Explorer, and Settings have no implemented homepage destination yet. The DSP391m brand links back to `/` on all three pages.

Homepage implementation: `src/features/home/HomePage.tsx` and `home.module.css`; original Figma icons: `public/figma/home/`. The mobile layout includes a collapsible navigation menu.

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

Open http://localhost:3000. The frontend requests `/api/v1/system/status` and `/api/v1/data-ops/status` on its
own origin. A Next.js rewrite forwards API routes to the backend, which
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

## Shared navigation and collectors

The root layout mounts `src/features/home/AppShell.tsx` so the two-row header and connection status persist across navigation. The navy header contains the Tracker home link, a read-only search preview, and horizontal navigation. Collectors is a click-triggered dropdown; mobile uses a collapsible menu. Collector pages are `/collectors/threads`, `/collectors/youtube`, and `/collectors/pickleballplus`; their top bar titles match the selected source. Threads reuses the crawler runtime panel, while YouTube and PickleballPlus show the existing Data Operations source metrics and run history. Collection controls remain on `/data-operations`.

## Homepage design and history
The homepage uses compact source rows, a 24-hour / 7-day stacked collection chart,
a compact infrastructure strip, and a run activity feed. The orange Tracker mark is
a local SVG. The chart uses only the latest 100 runs returned by the existing API:
it groups completed acquisition runs by completion timestamp, deduplicates run IDs,
excludes processing replay, and reports missing counts. Accepted observations are
not unique entities. Empty history renders an empty state, not sample activity.
Chart bars support keyboard focus and have a tabular data alternative. Progress
uses confirmed query coverage or current request-budget telemetry. Animation
respects reduced-motion preferences.

## Temporary PickleballPlus venue map

The collector page now reads `/api/pbplus/venues`, a read-only Next.js endpoint
that loads `apps/collector/.local/pbplus/pbplus_osm_geocoded.json` directly from
this checkout. It does not import the snapshot into Git or modify collector data.
Set `PBPLUS_GEO_FILE` to override the file path and `DSP_REPO_ROOT` to override
the repository root (default: two directories above the frontend working directory).
The file is reread on page load and **Refresh snapshot**. Missing/invalid files
return 503; the page shows an error with retry. Runtime status and collection
controls still use the existing Go backend through `/api/v1/*`.

The supplied snapshot has 337 venue listings, 303 mapped and 34 without coordinates.
Coordinates are geocoded estimates; repeated coordinates are clustered/spiderfied.
Cluster counts represent listings, not playing courts. Reported court counts may
contain outliers. Address-area summaries use the existing location reference and
are not verified administrative boundaries. Search, area/coordinate filters,
pagination, listing details and links remain available alongside the map.

Leaflet uses OpenStreetMap browser tiles with visible attribution; basemap tiles
need internet. `NEXT_PUBLIC_MAP_TILE_URL` may override the tile URL only for a
compatible OpenStreetMap-attributed provider. No geocoding requests are made.
This local-file adapter is temporary; a deployed frontend needs the file mounted
on its server or a future backend venue endpoint.

## Plain-language collection views
Threads and YouTube lead with an at-a-glance status and explained metric cards.
Threads search coverage is separate from accepted records; YouTube videos and
comments remain separate. Missing values are labeled “Not reported.” Technical
panels retain the underlying diagnostics. TikTok has an availability page only;
there is no simulated TikTok telemetry or new backend collection support.
Data Operations includes a three-step guide, a glossary, explained storage labels,
and expandable source diagnostics. Control endpoints and request bodies are unchanged.

### PickleballPlus layout
The page shows the full-width map first, a paginated court-data preview second,
and complete-snapshot CSV/JSON downloads last. Map/list filters and the area
summary sidebar have been removed. Marker selection still shows venue details.

### Court downloads and homepage slideshow
Court listings can be downloaded as spreadsheet-friendly UTF-8 CSV or structured
JSON, using the complete loaded snapshot (all pages).
Downloads include source collection time, reported hours/services and coordinates;
CSV cells escape formula-like source strings. Unknown fields remain empty/null.
The homepage has a white separator and five local photo slides, rotating every
six seconds. Rotation continues after hover and manual navigation; hidden browser tabs and
reduced-motion preferences suspend automatic rotation. Subtle arrows, dots and
horizontal touch gestures allow manual navigation. Photo credits and
sources are documented in `public/images/README.md`.
