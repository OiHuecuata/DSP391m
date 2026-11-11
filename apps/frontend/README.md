# Tracker frontend

Tracker displays court listings, Threads posts, YouTube videos and comments from a saved S3 Silver snapshot. Homepage totals, the header, collector pages, and Data Operations all read this snapshot. Data Operations also includes live Go backend collection controls, separate from the saved S3 metrics.

## Run locally

From `apps/frontend`:

```bash
npm install
npm run dev
```

Open http://localhost:3000. Configure `TRACKER_DATA_SOURCE=s3` in `.env.local` and prepare the cache using the [S3 setup instructions](../../scripts/website/README.md). AWS credentials stay outside the repository and browser. The website reads the exported cache; it does not need the Go backend or AWS credentials to display saved data.

From the repository root, refresh the cache:

```bash
./scripts/website/refresh_s3.sh
```

The shared status checks the saved cache every five seconds (three seconds on Data Operations). This does not fetch S3 or start a crawler. The header displays the actual cache sync time, not the time of the latest browser check. Refresh collector pages to reload their data.

## Pages and APIs

- `/`: automatic five-photo slideshow, daily S3 collection-history chart, saved-record totals and source links.
- `/collectors/threads`: aggregate post totals, reported likes/replies, and source collection history.
- `/collectors/youtube`: aggregate video/comment totals, reported likes/views, and source collection history.
- `/collectors/pickleballplus`: map, court table, CSV and JSON downloads.
- `/data-operations`: S3 location, sync time, dataset versions, counts and map coverage.
- `/api/s3/status`: summary only; no record bodies or credentials.
- `/api/collected/[source]`: paginated Threads/YouTube records.
- `/api/pbplus/venues`: court listings and coordinate provenance.

The export is a historical snapshot, not live run history. Court positions enriched from matching local geocoding are labelled separately. Missing coordinates are not fabricated. The existing Go proxy and runtime components remain in source for future integration, and the collection controller is used by Data Operations. Analytics remains unimplemented.

## Checks

```bash
npm run lint
npm run build
RUN_S3_WEBSITE=1 npm run test:e2e -- tests/s3-website.spec.ts
```

The S3 integration checks require the configured snapshot cache. Older runtime-dashboard tests describe the previous local-backend interface.

### Location mentions

Threads and YouTube overviews match saved post/video text against `configs/reference/vietnam_locations.v1.json`. Matching ignores accents and case, requires word boundaries, excludes URLs, and counts each saved item once per location. Multiple locations in one item count separately. YouTube comments are excluded. These are dictionary-based Vietnamese mentions, not verified court locations or exhaustive geographic extraction. Missing reference data displays unavailable rather than zero matches.

### Illustrated collector metrics

Collector overviews include six icon cards and sparklines where dated observations exist. Threads adds reposts, quotes and media counts; YouTube adds median reported ISO-8601 video duration and common tags. Card totals use the latest saved observation per item. The engagement timeline uses the latest observation per item per ICT collection day, then sums reported values for that day’s sample. It does not measure newly gained interactions or comparable fixed cohorts. Missing metrics stay null; no growth percentages, reach, saves, or follower counts are inferred. The cache exporter preserves these fields on future refreshes.

### Flow controls

Data Operations polls the live backend every three seconds while mounted. Start/stop, per-source runs, source settings, saved-data replay and supported Threads recovery actions use the existing `/api/v1/collection/*` endpoints. Backend outages disable actions without hiding the S3 snapshot sections. No crawler starts automatically when opening the page. The saved S3 export is refreshed separately.
