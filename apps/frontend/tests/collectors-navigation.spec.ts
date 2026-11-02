import { test, expect } from "@playwright/test";
import type { Operations } from "../src/features/data-ops/types";

function snapshot(status = "IDLE"): Operations {
  const source = { enabled: true, configured: true, interval_seconds: 43200, status, queued: false, metrics: { accepted: 5, discovered: 12, new_identities: 2, existing_identities: 3, errors: 0, videos: 2, comments: 3, requests: { used: 4, budget: 10, by_endpoint: { search: 1, videos: 1, commentThreads: 2 } } } };
  return { pipeline: { status, basis: "Verified complete cycle and storage health", refresh_interval_seconds: 15 }, collection: { continuous: status === "RUNNING", status, sources: { threads: { ...source }, youtube: { ...source, interval_seconds: 21600 }, pbplus: { ...source, enabled: false, interval_seconds: 86400 } }, runs: [] }, lakehouse: { status: "HEALTHY", backend: "Local Delta Lake", uri: "file:///fixture/lake", readable: true, writable: true, physical_bytes: 2000000, table_count: 10, updated_at: "2026-10-08T01:00:00Z", refreshed_at: "2026-10-08T01:00:00Z", tables: [{ name: "silver/social_posts", exists: true, rows: 100, version: 3, updated_at: "2026-10-08T01:00:00Z" }, { name: "silver/courts", exists: true, rows: 337, version: 2, updated_at: null }] }, warehouse: { status: "HEALTHY", backend: "ClickHouse", host: "localhost:8123", database: "dsp391m", connected: true, exists: true, table: "location_market_signal_snapshot", rows: 8, distinct_locations: 8, latest_snapshot_date: "2026-10-07", gold_generated_at: null, last_successful_load_at: null } };
}

test.beforeEach(async ({ page }) => {
  await page.route("**/api/v1/data-ops/status", route => route.fulfill({ json: snapshot() }));
  await page.route("**/api/v1/system/status", route => route.fulfill({ json: {
    components: [{ id: "threads", type: "crawler", status: "idle", details: { collected_count: 5, target_count: 10 } }]
  } }));
});

test("collector pages keep the same header navigation and show the correct title", async ({ page }) => {
  await page.goto("/");
  const sidebar = page.getByRole("navigation", { name: "Main navigation" });
  await page.getByRole("button", { name: "Collectors", exact: true }).click();
  const originalNavigation = (await sidebar.textContent()) ?? "";
  for (const [name, route] of [["Threads", "threads"], ["YouTube", "youtube"], ["PickleballPlus", "pickleballplus"]]) {
    if (await page.getByRole("button", { name: "Collectors", exact: true }).getAttribute("aria-expanded") !== "true") await page.getByRole("button", { name: "Collectors", exact: true }).click();
    await sidebar.getByRole("link", { name, exact: true }).click();
    await expect(page).toHaveURL(new RegExp("/collectors/" + route + "$"));
    await expect(page.getByTestId("page-title")).toHaveText(name);
    await expect(sidebar).toHaveText(originalNavigation);
    await expect(sidebar.locator(`a[href="/collectors/${route}"]`)).toHaveAttribute("aria-current", "page");
    await expect(sidebar.getByRole("link", { name: "Homepage", exact: true })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "Collectors", exact: true })).toHaveAttribute("aria-expanded", "false");
    if (route === "pickleballplus") await expect(page.getByRole("heading", { name: "Court listings · CSV preview", exact: false })).toBeVisible();
    else await expect(page.getByRole("heading", { name: "Recent " + name + " runs" })).toBeVisible();
    await page.screenshot({ path: "/home/coara/Documents/Codex/2026-10-26/i-j/outputs/collector-" + route + ".png", fullPage: true });
  }
  await page.getByRole("link", { name: "Tracker home" }).click();
  await expect(page.getByTestId("page-title")).toHaveText("Homepage");
});

test("collector direct links render inside the same layout", async ({ page }) => {
  for (const [name, route] of [["Threads", "threads"], ["YouTube", "youtube"], ["PickleballPlus", "pickleballplus"]]) {
    await page.goto("/collectors/" + route);
    await expect(page.getByTestId("page-title")).toHaveText(name);
    await expect(page.getByRole("navigation", { name: "Main navigation" })).toBeVisible();
    await expect(page.getByRole("link", { name: "Manage collection" })).toHaveAttribute("href", "/data-operations");
  }
});

test("mobile collector navigation closes the drawer and stays within the viewport", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");
  await page.getByRole("button", { name: "Menu", exact: true }).click();
  await page.getByRole("button", { name: "Collectors", exact: true }).click();
  await page.getByRole("navigation", { name: "Main navigation" }).getByRole("link", { name: "YouTube", exact: true }).click();
  await expect(page.getByTestId("page-title")).toHaveText("YouTube");
  await expect(page.getByRole("navigation", { name: "Main navigation" })).toBeHidden();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page.getByRole("button", { name: "Menu", exact: true }).click();
  await page.getByRole("button", { name: "Collectors", exact: true }).click();
  await page.getByRole("navigation", { name: "Main navigation" }).getByRole("link", { name: "PickleballPlus", exact: true }).click();
  await expect(page.getByTestId("page-title")).toHaveText("PickleballPlus");
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});
