/** @jsxImportSource react */
import { test, expect, type Page } from "@playwright/test";
import { renderToStaticMarkup } from "react-dom/server";
import { CrawlerSourceTabs } from "../src/features/system/components/CrawlerSourceTabs";
import { crawlerSources } from "../src/features/system/config/crawlerSources";
import type { CrawlerComponentStatus, SystemStatus } from "../src/features/system/types";

const endpoint = "**/api/v1/system/status";
// Fixtures belong only to tests; the application always uses the real endpoint.
function snapshot(status = "success", collected = 10): { components: CrawlerComponentStatus[] } {
  return { components: [{
    id: "threads", type: "crawler", status, updated_at: "2026-09-30T08:30:44Z",
    details: {
      collected_count: collected, target_count: 10, raw_count: 74,
      queries_processed: 8, query_plan_count: 17, queries_succeeded: 8, queries_failed: 0,
      seed_query: "pickleball", sort: "recent", current_query: status === "running"
        ? { query: "pickleball Hà Nội", kind: "city", status: "running" } : null,
      last_query: { query: "pickleball Đà Nẵng", raw_count: 17, accepted_count: 1, status: "ok" },
      started_at: "2026-09-30T08:00:00Z", finished_at: status === "running" ? null : "2026-09-30T08:30:44Z",
      error: status === "failed" ? "No posts matched the configured Vietnam relevance policy" : null,
    },
  }] };
}

async function showSnapshot(page: Page, data: SystemStatus) {
  await page.route(endpoint, (route) => route.fulfill({ json: data }));
  await page.goto("/");
}

test("successful early stop is 100% even with unprocessed queries", async ({ page }) => {
  await showSnapshot(page, snapshot());
  await expect(page.locator(".status-badge")).toHaveText("Success");
  await expect(page.getByRole("progressbar")).toHaveAttribute("aria-valuenow", "100");
  await expect(page.locator(".metric-grid")).toContainText("8 / 17");
  await expect(page.getByText("17 raw → 1 accepted")).toBeVisible();
  await expect(page.locator(".run-details")).toContainText("pickleball");
});

test("running crawler updates through five-second polling", async ({ page }) => {
  const requests: number[] = [];
  await page.route(endpoint, (route) => {
    requests.push(Date.now());
    return route.fulfill({ json: snapshot("running", requests.length === 1 ? 6 : 8) });
  });
  await page.goto("/");
  await expect(page.getByRole("progressbar")).toHaveAttribute("aria-valuenow", "60");
  await expect(page.getByText("pickleball Hà Nội", { exact: true })).toBeVisible();
  await expect(page.getByRole("progressbar")).toHaveAttribute("aria-valuenow", "80");
  expect(requests[1] - requests[0]).toBeGreaterThanOrEqual(4_800);
  expect(requests[1] - requests[0]).toBeLessThan(8_000);
});

test("failed crawler exposes its backend error", async ({ page }) => {
  await showSnapshot(page, snapshot("failed", 0));
  await expect(page.locator(".status-badge")).toHaveText("Failed");
  await expect(page.getByRole("main").getByRole("alert")).toContainText("No posts matched the configured Vietnam relevance policy");
});

for (const [status, label] of [
  ["idle", "Idle"], ["healthy", "Healthy"], ["unavailable", "Unavailable"],
  ["not_configured", "Not configured"], ["paused_future", "paused future"],
]) {
  test(`status ${status} is safe without metrics`, async ({ page }) => {
    await showSnapshot(page, { components: [{ id: "threads", type: "crawler", status }] });
    await expect(page.locator(".status-badge")).toHaveText(label);
    await expect(page.getByText("Collection progress is not available.")).toBeVisible();
    await expect(page.getByRole("progressbar")).toHaveCount(0);
  });
}

for (const name of ["PickleballPlus", "TikTok"]) {
  test(`${name} has an empty state when absent`, async ({ page }) => {
    await showSnapshot(page, snapshot());
    await page.getByRole("tab", { name, exact: true }).click();
    await expect(page.getByText("No runtime status available", { exact: true })).toBeVisible();
    await expect(page.getByRole("progressbar")).toHaveCount(0);
    await expect(page.locator(".status-badge")).toHaveCount(0);
  });
}

test("lookup works for any configured source and ignores other component types", async ({ page }) => {
  const crawler = snapshot("running", 6).components[0];
  await showSnapshot(page, { components: [
    { id: "tiktok", type: "data_lake", status: "healthy" },
    { ...crawler, id: "tiktok" },
  ] });
  await page.getByRole("tab", { name: "TikTok", exact: true }).click();
  await expect(page.locator(".status-badge")).toHaveText("Running");
  await expect(page.getByRole("progressbar")).toHaveAttribute("aria-valuenow", "60");
});

test("backend failure retains navigation and recovers automatically", async ({ page }) => {
  let requests = 0;
  await page.route(endpoint, (route) => {
    requests++;
    return requests === 1 ? route.fulfill({ status: 503, body: "Unavailable" })
      : route.fulfill({ json: snapshot() });
  });
  await page.goto("/");
  await expect(page.getByRole("main").getByRole("alert")).toContainText("Backend unavailable");
  await expect(page.getByRole("tab", { name: "TikTok" })).toBeVisible();
  await expect(page.locator(".status-badge")).toHaveText("Success");
  await expect(page.getByRole("main").getByRole("alert")).toHaveCount(0);
});

test("network errors show backend unavailable", async ({ page }) => {
  await page.route(endpoint, (route) => route.abort("connectionrefused"));
  await page.goto("/");
  await expect(page.getByRole("main").getByRole("alert")).toContainText("Backend unavailable");
  await expect(page.getByRole("heading", { name: "Tracker", exact: true })).toBeVisible();
});

test("a failed refresh marks the previous snapshot as stale", async ({ page }) => {
  let requests = 0;
  await page.route(endpoint, (route) => ++requests === 1
    ? route.fulfill({ json: snapshot() }) : route.fulfill({ status: 503 }));
  await page.goto("/");
  await expect(page.locator(".status-badge")).toHaveText("Success");
  await expect(page.getByRole("main").getByRole("alert")).toContainText("Showing the last received snapshot; it may be out of date.");
  await expect(page.getByRole("progressbar")).toHaveAttribute("aria-valuenow", "100");
});

test("loading is visible while the first request is pending", async ({ page }) => {
  await page.route(endpoint, () => {});
  await page.goto("/");
  await expect(page.getByRole("status")).toContainText("Loading crawler status");
});

test("invalid response is handled without a page crash", async ({ page }) => {
  await page.route(endpoint, (route) => route.fulfill({ json: { components: [{ id: "threads", type: "crawler", status: "running", details: { current_query: { query: {} } } }] } }));
  await page.goto("/");
  await expect(page.getByRole("main").getByRole("alert")).toContainText("invalid system status response");
});

test("zero target does not produce an invalid progress value", async ({ page }) => {
  const data = snapshot("idle", 0);
  data.components[0].details!.target_count = 0;
  await showSnapshot(page, data);
  await expect(page.getByText("Collection progress is not available.")).toBeVisible();
  await expect(page.getByRole("progressbar")).toHaveCount(0);
});

test("placeholder tabs stop polling and support keyboard navigation", async ({ page }) => {
  let requests = 0;
  await page.route(endpoint, (route) => { requests++; return route.fulfill({ json: snapshot() }); });
  await page.goto("/");
  await expect(page.locator(".status-badge")).toBeVisible();
  await page.getByRole("tab", { name: "Threads", exact: true }).focus();
  await page.keyboard.press("ArrowRight");
  await expect(page.getByRole("tab", { name: "PickleballPlus" })).toBeFocused();
  await page.getByRole("tab", { name: "API", exact: true }).click();
  await expect(page.getByRole("heading", { name: "API tracker" })).toBeVisible();
  const previous = requests;
  await page.waitForTimeout(5_200);
  expect(requests).toBe(previous);
  await page.getByRole("tab", { name: "Analysis", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Analysis", exact: true })).toBeVisible();
  await expect(page.getByText("Coming soon", { exact: true })).toBeVisible();
});

test("a new configuration entry automatically adds a navigation option", () => {
  const sources = [...crawlerSources, { id: "youtube", name: "YouTube" }];
  const markup = renderToStaticMarkup(<CrawlerSourceTabs sources={sources} selected="youtube" onSelect={() => {}} />);
  expect(markup).toContain('id="crawler-tab-youtube"');
  expect(markup).toContain('aria-selected="true"');
  expect(markup).toContain("YouTube");
});

test("mobile dashboard fits the viewport", async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 812 });
  await showSnapshot(page, snapshot());
  await expect(page.locator(".status-badge")).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(375);
});

test("real endpoint matches the crawler snapshot", async ({ page, request }, testInfo) => {
  test.skip(process.env.RUN_REAL_BACKEND !== "1", "Set RUN_REAL_BACKEND=1 with the existing Go backend running.");
  const response = await request.get("/api/v1/system/status");
  expect(response.ok()).toBeTruthy();
  const data = await response.json() as SystemStatus;
  const crawler = data.components.find((item): item is CrawlerComponentStatus => item.type === "crawler" && item.id === "threads");
  expect(crawler).toBeDefined();
  const pendingResponse = page.waitForResponse((response) => response.url().endsWith("/api/v1/system/status"));
  await page.goto("/");
  const actualResponse = await pendingResponse;
  expect(actualResponse.ok()).toBeTruthy();
  await expect(page.locator(".status-badge")).toContainText(crawler!.status.replaceAll("_", " "), { ignoreCase: true });
  if (crawler?.details?.target_count && crawler.details.collected_count != null) {
    const progress = Math.min(100, crawler.details.collected_count / crawler.details.target_count * 100);
    await expect(page.getByRole("progressbar")).toHaveAttribute("aria-valuenow", String(progress));
    await expect(page.locator(".metric-grid")).toContainText(String(crawler.details.raw_count));
  }
  await page.screenshot({ path: testInfo.outputPath("real-dashboard.png"), fullPage: true });
});
