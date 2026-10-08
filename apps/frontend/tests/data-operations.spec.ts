import { test, expect, type Page } from "@playwright/test";
import type { Operations } from "../src/features/data-ops/types";

function snapshot(status = "IDLE"): Operations {
  const source = { enabled: true, configured: true, interval_seconds: 43200, status, queued: false, metrics: { accepted: 5, discovered: 12, new_identities: 2, existing_identities: 3, errors: 0, videos: 2, comments: 3, requests: { used: 4, budget: 10, by_endpoint: { search: 1, videos: 1, commentThreads: 2 } } } };
  return { pipeline: { status, basis: "Verified complete cycle and storage health", refresh_interval_seconds: 15 }, collection: { continuous: status === "RUNNING", status, sources: { threads: { ...source }, youtube: { ...source, interval_seconds: 21600 }, pbplus: { ...source, enabled: false, interval_seconds: 86400 } }, runs: [] }, lakehouse: { status: "HEALTHY", backend: "Local Delta Lake", uri: "file:///fixture/lake", readable: true, writable: true, physical_bytes: 2000000, table_count: 10, updated_at: "2026-10-08T01:00:00Z", refreshed_at: "2026-10-08T01:00:00Z", tables: [{ name: "silver/social_posts", exists: true, rows: 100, version: 3, updated_at: "2026-10-08T01:00:00Z" }, { name: "silver/courts", exists: true, rows: 337, version: 2, updated_at: null }] }, warehouse: { status: "HEALTHY", backend: "ClickHouse", host: "localhost:8123", database: "dsp391m", connected: true, exists: true, table: "location_market_signal_snapshot", rows: 8, distinct_locations: 8, latest_snapshot_date: "2026-10-07", gold_generated_at: null, last_successful_load_at: null } };
}
async function show(page: Page, data = snapshot()) { await page.route("**/api/v1/data-ops/status", (route) => route.fulfill({ json: data })); await page.goto("/data-operations"); await expect(page.getByRole("heading", { name: "Collection", exact: true })).toBeVisible(); }

for (const [bytes, expected] of [
  [3_884_671, "0.0039 GB"],
  [500_000_000, "0.500 GB"],
  [1_500_000_000, "1.50 GB"],
] as const) {
  test(`Data Lake card displays ${bytes} bytes as ${expected}`, async ({ page }) => {
    const data = snapshot();
    data.lakehouse.physical_bytes = bytes;
    await show(page, data);
    const card = page.getByRole("region", { name: "Data Lake", exact: true });
    const label = card.getByText("Physical size", { exact: true });
    await expect(label).toBeVisible();
    await expect(label.locator("+ dd")).toHaveText(expected);
  });
}

test("Data Lake and Warehouse cards show actual response metrics", async ({ page }) => { await show(page); await expect(page.getByRole("region", { name: "Data Lake", exact: true })).toContainText("Local Delta Lake"); await expect(page.getByRole("region", { name: "Data Lake", exact: true })).toContainText("100"); await expect(page.getByRole("region", { name: "Data Warehouse", exact: true })).toContainText("2026-10-07"); await expect(page.getByRole("region", { name: "Data Warehouse", exact: true })).toContainText("dsp391m"); });
test("source cards show telemetry and server-only configuration flag", async ({ page }) => { await show(page, snapshot("PARTIAL")); const youtube = page.getByRole("article", { name: "YouTube collection" }); await expect(youtube).toContainText("API configured: Yes"); await expect(youtube).toContainText("4 / 10"); await expect(page.getByRole("article", { name: "Threads collection" })).toContainText("PARTIAL"); await expect(page.getByRole("article", { name: "PBPlus collection" })).toBeVisible(); expect(await page.content()).not.toContain("YOUTUBE_API_KEY"); expect(await page.content()).not.toContain("AIza"); });
test("Start sends selected sources and intervals and returns immediately", async ({ page }) => { await show(page); await page.getByLabel("Enable YouTube", { exact: true }).uncheck(); await page.getByLabel("Threads interval seconds", { exact: true }).fill("86400"); let body; await page.route("**/api/v1/collection/start", async (route) => { body = route.request().postDataJSON(); return route.fulfill({ status: 202, json: snapshot("RUNNING").collection }); }); await page.getByRole("button", { name: "Start continuous collection", exact: true }).click(); await expect(page.getByRole("status")).toContainText("You can close this browser"); expect(body).toMatchObject({ sources: { threads: { enabled: true, interval_seconds: 86400 }, youtube: { enabled: false }, pbplus: { enabled: false } } }); });
test("Stop renders STOPPING without interrupting current cycle", async ({ page }) => { await show(page, snapshot("RUNNING")); await page.route("**/api/v1/collection/stop", (route) => route.fulfill({ status: 202, json: { ...snapshot("STOPPING").collection, continuous: false, active_run_id: "current" } })); await page.getByRole("button", { name: "Stop after current run", exact: true }).click(); await expect(page.getByRole("region", { name: "Collection", exact: true })).toContainText("STOPPING"); await expect(page.getByRole("status")).toContainText("current cycle will finish safely"); });
test("Run Now selects one source and handles overlap rejection", async ({ page }) => { await show(page); let body; await page.route("**/api/v1/collection/run", async (route) => { body = route.request().postDataJSON(); return route.fulfill({ status: 409, json: { error: "source already running or queued" } }); }); await page.getByRole("button", { name: "Run YouTube now", exact: true }).click(); expect(body).toEqual({ sources: ["youtube"] }); await expect(page.getByRole("status")).toContainText("already running"); });
test("initial loading and backend failure are visible", async ({ page }) => { await page.route("**/api/v1/data-ops/status", () => {}); await page.goto("/data-operations"); await expect(page.getByRole("status")).toContainText("Loading data operations"); await page.unroute("**/api/v1/data-ops/status"); await page.route("**/api/v1/data-ops/status", (route) => route.fulfill({ status: 503 })); await page.reload(); await expect(page.getByRole("main").getByRole("alert")).toContainText("HTTP 503"); });
test("polling renders completed history and marks failed refresh stale", async ({ page }) => { let count = 0; const data = snapshot("SUCCESS"); data.collection.runs = [{ run_id: "fixture-run", source: "youtube", status: "SUCCESS", started_at: "2026-10-08T00:00:00Z", completed_at: "2026-10-08T00:01:00Z", duration_seconds: 60, metrics: { accepted: 10, new_identities: 5, errors: 0 } }]; await page.route("**/api/v1/data-ops/status", (route) => ++count === 1 ? route.fulfill({ json: snapshot("RUNNING") }) : count === 2 ? route.fulfill({ json: data }) : route.fulfill({ status: 503 })); await page.goto("/data-operations"); await expect(page.getByRole("region", { name: "Run history" })).toContainText("fixture-run"); await expect(page.getByRole("main").getByRole("alert")).toContainText("may be stale"); await expect(page.getByRole("region", { name: "Run history" })).toContainText("60.0s"); });
test("unconfigured YouTube controls are disabled", async ({ page }) => { const data = snapshot(); data.collection.sources.youtube.configured = false; data.collection.sources.youtube.enabled = false; await show(page, data); await expect(page.getByLabel("Enable YouTube", { exact: true })).toBeDisabled(); await expect(page.getByRole("button", { name: "Run YouTube now", exact: true })).toBeDisabled(); });
test("failed source and missing warehouse state remain visible", async ({ page }) => { const data = snapshot("FAILED"); data.warehouse.status = "FAILED"; data.warehouse.connected = false; data.warehouse.error = "ClickHouse connection/query failed"; data.collection.sources.threads.last_error = "Source cycle failed"; await show(page, data); await expect(page.getByRole("article", { name: "Threads collection" })).toContainText("Source cycle failed"); await expect(page.getByRole("region", { name: "Data Warehouse", exact: true })).toContainText("ClickHouse connection/query failed"); });
test("mobile operations fit viewport", async ({ page }) => { await page.setViewportSize({ width: 375, height: 812 }); await show(page); expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(375); });
test("real operations response renders local storage and source states", async ({ page, request }) => { test.skip(process.env.RUN_REAL_BACKEND !== "1", "Requires local Go backend"); const response = await request.get("/api/v1/data-ops/status"); expect(response.ok()).toBeTruthy(); const data = await response.json() as Operations; await page.goto("/data-operations"); await expect(page.getByRole("region", { name: "Data Lake", exact: true })).toContainText(data.lakehouse.backend); await expect(page.getByRole("region", { name: "Data Warehouse", exact: true })).toContainText(String(data.warehouse.rows ?? "—")); await expect(page.getByRole("article", { name: "YouTube collection" })).toContainText(data.collection.sources.youtube.configured ? "API configured: Yes" : "API configured: No"); });

test("Save settings uses PATCH without starting collection", async ({ page }) => {
  await show(page);let body;
  await page.route("**/api/v1/collection/config", (route) => { expect(route.request().method()).toBe("PATCH");body=route.request().postDataJSON();return route.fulfill({status:202,json:snapshot().collection}); });
  await page.getByRole("button",{name:"Save source settings",exact:true}).click();
  await expect(page.getByRole("status")).toContainText("Source settings saved");
  expect(body).toMatchObject({sources:{threads:{interval_seconds:43200},youtube:{interval_seconds:21600}}});
});
test("Retry saved run sends the original observation ID", async ({ page }) => {
  const data=snapshot("FAILED");data.collection.sources.threads.last_run_id="saved-failed-run";
  await show(page,data);let body;
  await page.route("**/api/v1/collection/retry", (route) => {body=route.request().postDataJSON();return route.fulfill({status:202,json:snapshot("RUNNING").collection});});
  await page.getByRole("button",{name:"Retry saved Threads run",exact:true}).click();
  await expect(page.getByRole("status")).toContainText("source data will not be fetched again");
  expect(body).toEqual({run_id:"saved-failed-run"});
});

test("active source metrics come from current structured progress", async ({ page }) => {
 const data=snapshot("RUNNING");data.collection.active_run_id="current-run";data.collection.sources.threads.last_run_id="current-run";data.collection.sources.threads.metrics={accepted:999};
 data.collection.progress={stage:"spark_ingestion",metrics:{accepted:3,acknowledged:3,consumed:3}};
 await show(page,data);
 await expect(page.getByRole("article",{name:"Threads collection"})).not.toContainText("999");
 await expect(page.getByRole("status")).toContainText("Kafka acknowledged 3");
 await expect(page.getByRole("status")).toContainText("Spark consumed 3");
});
