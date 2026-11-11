# S3 data for Tracker

The website uses a **read-only local snapshot cache** from:
`s3://dsp391m-data-lake/bootstrap/20261028T064025Z/lakehouse/`
Region: `ap-southeast-2`. This is the October 28 bootstrap snapshot, not a live
stream of later collector runs. No changes to the `model` branch are required.

## Local setup

Install AWS CLI v2.32+ and sign in locally with `aws login --profile tracker
--region ap-southeast-2`. On this computer the CLI is installed at
`/home/coara/Documents/Codex/tools/bin/aws`. Browser login uses temporary
credentials; never put credentials in the frontend or Git. The current development
login is root; use a separate read-only role/profile for ongoing or deployed work.

From the repository root:

```bash
python3 -m venv .local/website-s3-venv
.local/website-s3-venv/bin/pip install -r scripts/website/requirements.txt
./scripts/website/refresh_s3.sh
```

For other machines set `TRACKER_AWS_CLI` if `aws` is not on PATH. Optional
`TRACKER_AWS_PROFILE`, `AWS_REGION`, and `TRACKER_S3_BASE` override defaults.
Only immutable `/bootstrap/` namespaces are supported. Managed generations must
be resolved through their current/generation manifests before adding support;
do not read each managed table's latest version independently.

Set `TRACKER_DATA_SOURCE=s3` in ignored `apps/frontend/.env.local`, then start
backend and frontend normally. `TRACKER_S3_SNAPSHOT` optionally overrides the
snapshot JSON path; its default is `.local/website-data/snapshot.json` at repo root.
No AWS credentials are needed by the website server: it only reads that file.
UI refresh buttons reload the cache; run the shell command to fetch S3 again.
If the browser login expires, the refresh script prints the login command.
The last successfully exported cache remains usable until replaced.

## Data and integrity

The refresher only runs `sts get-caller-identity` and `s3 sync` **from S3 to local**.
It never writes to S3, starts crawlers, publishes to ClickHouse or runs the model.
Each refresh downloads into a new temporary directory, uses Delta transaction logs
to read active rows, limits each input to 100,000 rows, then publishes one atomic
JSON snapshot. Failed refreshes leave the prior file intact. Deduplication chooses
the latest collected observation for each source and source-record ID. All raw
transport metadata, local file paths and author identifiers are excluded from social
responses. APIs allow only Threads/YouTube and paginate 10 records at a time.

The current snapshot has court table v5, post table v33, and comment table v6.
After deduplication: 337 venues, 52 Threads posts, 575 YouTube videos, 75 comments.
Silver courts have no coordinates or services/opening-hours fields. Coordinates
are optionally enriched from the existing geocoded local file **only if the venue
ID and normalized, nonempty address both match**. Their local origin is shown on
the map and in JSON download metadata. Missing fields remain missing; no synthetic
records or zero values are substituted.

To update to a newer snapshot, set `TRACKER_S3_BASE` to the new approved immutable
bootstrap lakehouse URI and run the refresh script. Do not point at synthetic
staging data and present it as real collected content.

The `.local` data/venv directories and `.env.local` are ignored by Git.

## Minimal read-only access for a non-root identity

Grant `s3:ListBucket` on `arn:aws:s3:::dsp391m-data-lake` with an `s3:prefix`
condition restricted to `bootstrap/20261028T064025Z/lakehouse/silver/*`, and
`s3:GetObject` on `arn:aws:s3:::dsp391m-data-lake/bootstrap/20261028T064025Z/lakehouse/silver/*`.
KMS-encrypted objects also need the corresponding key's decrypt permission.
Console-based local login may require `SignInLocalDevelopmentAccess` on that identity.
This document does not create or modify any IAM permissions.

## Collection history

The export also aggregates active Silver observation rows by `collected_at` in Asia/Ho_Chi_Minh before latest-record deduplication. The homepage timeline includes repeated collections; YouTube combines videos and comments. Missing or timezone-less timestamps are excluded and counted. Days without observations are shown as zero within the available date range. Latest 7 days ends at the latest collection date, not today. These counts do not establish successful/failed run counts or new unique items.

Engagement history is exported separately from collection-volume history. It deduplicates each source item within each ICT day before summing likes/replies/reposts/quotes/views and carries reported-value coverage. The sampled content may differ daily; these are recorded totals, not engagement increments. Latest-item exports additionally allowlist reposts, quotes, media presence, duration and bounded video tags.

## Local demo automatic updates

Starting the frontend (`npm run dev` or `npm start` in `apps/frontend`) now
starts a server-side refresh immediately and every five minutes. Closing the
frontend stops its timer. The browser receives updated counts, maps and collector
views after the atomic snapshot changes. Refresh failures preserve saved data;
Data Operations shows the last refresh outcome. Credentials remain in the local
AWS CLI profile; an expired login must be renewed with `aws login --profile tracker
--region ap-southeast-2` before the next check succeeds.

By default, refresh selects the newest timestamped export under
`s3://dsp391m-data-lake/bootstrap/`. To follow a continuously published managed
lakehouse, set `TRACKER_S3_BASE=s3://bucket/prefix` in frontend `.env.local` to the
writer's actual root. That root must publish `control/current.json` referencing a
VALIDATED generation manifest. The reader pins all three Silver table versions
from the same manifest. Arbitrary staging files and partially built generations
are never treated as published website data. New raw crawler uploads require
Silver processing/publication before they can appear on the website.

Optional settings: `TRACKER_S3_AUTO_REFRESH=0` disables the timer;
`TRACKER_AWS_PROFILE` and `AWS_REGION` select the local credentials and region.
Concurrent refreshes are prevented by a filesystem lock. No S3 objects are written.

## Website accounts and roles

Account records are stored privately in
`s3://dsp391m-data-lake/website-auth/v1/accounts.json` (override with
`TRACKER_AUTH_S3_URI`). Passwords use salted scrypt hashes; account writes use
S3 ETag conditional updates and server-side AES256 encryption. No password hashes
or AWS credentials are returned to the browser. `aws login --profile tracker`
renews the computer's AWS access; website accounts are a separate identity system.

Create the first administrator with `node scripts/website/create_admin.mjs`.
The script verifies bucket public-access blocking, refuses to overwrite existing
accounts, and places the generated temporary password in ignored, owner-readable
`.local/auth/initial-admin.txt`. The first login must change that password before
opening any data or operational pages. The initial file may be deleted after the
password is changed. There are no hardcoded application passwords.

Administrators use `/admin` to create accounts, change roles, disable accounts,
and set temporary reset passwords. Users can browse datasets, use Dashboard,
download records, and change personal browser preferences. Data Operations,
manual S3 sync, automatic-sync settings, and account administration are admin-only.
Authorization is enforced on Next.js routes and direct Go API requests. Account
role/status is re-read from S3 during verification; AWS/storage outages fail closed.

Sessions use random opaque HttpOnly SameSite=Lax cookies, expire after eight hours,
and are stored as token hashes in owner-readable `.local/auth/sessions` files.
Password changes/reset invalidate earlier sessions using password revisions.
Set `AUTH_COOKIE_SECURE=1` when running behind HTTPS. This implementation restricts
account mutations to same-origin loopback requests for the existing local demo.
The Go backend verifies sessions through `AUTH_SERVICE_URL` (default
`http://127.0.0.1:3000/api/auth/me`) and listens only on loopback.
Preferences are kept separately for each signed-in user in this browser.

Isolated integration verification uses `auth_smoke.mjs setup`, a frontend server
started with `.local/auth-test/start.sh`, then `auth_smoke.mjs api`.
`auth_browser_smoke.mjs admin` and `auth_browser_smoke.mjs user` exercise the UI via
Playwright CLI. Finish with `auth_smoke.mjs cleanup`; only its allocated
`website-auth-test/<run>/accounts.json` object is removed. These checks never
change the primary administrator's password or the collected datasets.

### User interface and role checks

After `auth_smoke.mjs setup`, start its isolated server and open the Playwright CLI
browser on port 3100. Run `node scripts/website/user_interface_smoke.mjs` against
that fresh fixture. It checks the user's first password change, homepage, source
browsing, CSV export, mobile navigation, denied administrative pages and APIs,
and retained administrator access. It changes only the isolated fixture user's
password. Run the fixture cleanup after stopping the test server.

Regular users land on the browsing homepage after login. Their navigation exposes
Courts and Explore data; administrative controls and S3 settings are restricted
to administrators by server authorization as well as navigation visibility.

## Dashboard heatmap modes

The in-map Heatmap selector displays one measure at a time: social-share minus
venue-share (percentage points), social attention, average engagement, reported
playing-court supply, or activity by publication month in ICT. Social attention
uses Threads interactions or YouTube views separately. Average engagement uses
complete Threads interaction counts or complete YouTube likes/comment counts,
divided by the number of items reporting those measurements. Missing measurements
are hatched; valid zero values remain zero. Missing court counts are excluded,
with all-unknown provinces distinguished from reported zero court counts.

Monthly activity uses unique saved posts/videos and their publication dates, not
collection run dates or verified demand growth. Multi-province content can be
attributed to each mentioned province; viewer location is not inferred. Discussion
topics remains unavailable until classification is connected. Chart source
preferences in Settings affect the charts below the map independently.

## Temporary Cloudflare demo

Install the official `cloudflared` client (this laptop uses
`/home/coara/Documents/Codex/tools/bin/cloudflared`), then stop the existing local
frontend/backend and run from your own Terminal:

```bash
node scripts/website/share_demo.mjs
```

The launcher builds production Next.js and Go servers, creates a temporary HTTPS
`trycloudflare.com` address, and prints the verified public URL. Keep the Terminal
and laptop running; Ctrl+C stops its servers and tunnel. Use `--reuse-build` only
when the source has not changed since the last successful demo build. The public
URL changes on every launch. No Cloudflare account or domain is required.

Only the configured `TRACKER_PUBLIC_ORIGIN` may submit public mutations. Login,
password changes and admin controls keep their role checks and same-origin
protection; public sessions use Secure/HttpOnly cookies. The tunnel forwards only
the frontend. Go stays bound to loopback and validates sessions with the frontend.
S3 credentials remain in your local AWS profile and accounts stay in private S3.
Renew `aws login --profile tracker --region ap-southeast-2` when that AWS session
expires. Local sessions and the saved data cache remain on the laptop.
