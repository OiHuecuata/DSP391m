import { test, expect } from "@playwright/test";
import type { Operations } from "../src/features/data-ops/types";
import { collectionHistory } from "../src/features/home/history";

test("homepage renders honest disconnected state and preserves navigation", async ({ page }) => {
  await page.route("**/api/v1/data-ops/status", r => r.fulfill({ status: 503, json: { error: "offline" } }));
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "System status", exact: true })).toBeVisible();
  await expect(page.getByRole("main").getByRole("alert")).toContainText("Unable to load system status");
  await expect(page.getByText("Disconnected", { exact: true })).toBeVisible();
  await expect(page.getByRole("link", { name: "Open Threads collector" })).toHaveAttribute("href", "/collectors/threads");
  await expect(page.getByRole("link", { name: "Open operations" })).toHaveAttribute("href", "/data-operations");
  await page.getByRole("link", { name: "Open Threads collector" }).click();
  await expect(page).toHaveURL(/\/collectors\/threads$/);
  await page.getByRole("link", { name: "Tracker home" }).click();
  await expect(page.getByRole("heading", { name: "System status", exact: true })).toBeVisible();
});

test("mobile menu opens and homepage has no horizontal overflow", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.route("**/api/v1/data-ops/status", r => r.fulfill({ status: 503, json: {} }));
  await page.goto("/");
  await expect(page.getByRole("navigation", { name: "Main navigation" })).toBeHidden();
  await page.getByRole("button", { name: "Menu", exact: true }).click();
  await expect(page.getByRole("navigation", { name: "Main navigation" })).toBeVisible();
  await page.getByRole("button", { name: "Close menu" }).click();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

test("live homepage loads status and local visual assets", async ({ page }) => {
  test.skip(process.env.RUN_REAL_BACKEND !== "1");
  await page.setViewportSize({ width: 1440, height: 1106 });
  await page.goto("/");
  await expect(page.getByText("Connected", { exact: true })).toBeVisible();
  await expect(page.getByRole("main").getByRole("alert")).toHaveCount(0);
  await expect(page.getByText("Loading recent activity…")).toHaveCount(0);
  const images = await page.locator("main img:visible, aside img:visible, header img:visible").evaluateAll(images => images.map(image => {
    const img = image as HTMLImageElement;
    return { source: img.getAttribute("src"), loaded: img.complete && img.naturalWidth > 0, width: img.getBoundingClientRect().width, height: img.getBoundingClientRect().height };
  }));
  expect(images.length).toBeGreaterThan(8);
  expect(images.every(image => image.loaded && image.width > 0 && image.height > 0)).toBe(true);
  await page.screenshot({ path: "/home/coara/Documents/Codex/2026-10-26/i-j/outputs/homepage-desktop.png", fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({ path: "/home/coara/Documents/Codex/2026-10-26/i-j/outputs/homepage-mobile.png", fullPage: true });
});

test("Collectors opens on hover and supports keyboard and touch", async ({ page }) => {
  await page.route("**/api/v1/data-ops/status", r => r.fulfill({ status: 503, json: {} }));
  await page.goto("/");
  const toggle = page.getByRole("button", { name: "Collectors", exact: true });
  const options = page.locator("#collector-options");
  await expect(options).toBeHidden();
  await toggle.hover();
  await expect(options).toBeVisible();
  await expect(toggle).toHaveAttribute("aria-expanded", "true");
  for (const name of ["Threads", "YouTube", "PickleballPlus", "TikTok"]) {
    await expect(options.getByText(name, { exact: true })).toBeVisible();
  }
  for (const name of ["threads.svg", "youtube.png", "tiktok.png", "pickleballplus.png"]) {
    await expect(options.locator(`img[src="/brands/${name}"]`)).toBeVisible();
  }
  await page.screenshot({ path: "/home/coara/Documents/Codex/2026-10-26/i-j/outputs/tracker-collectors-menu.png", fullPage: true });
  await options.getByText("Threads", { exact: true }).hover();
  await expect(options).toBeVisible();
  await page.getByRole("link", { name: "Tracker home" }).hover();
  await expect(options).toBeHidden();
  await toggle.focus();
  await toggle.press("Escape");
  await expect(options).toBeHidden();
  await expect(toggle).toBeFocused();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole("button", { name: "Menu", exact: true }).click();
  await toggle.focus();
  await expect(options).toBeVisible();
  await toggle.press("Escape");
  await expect(options).toBeHidden();
});

for (const mobile of [false, true]) {
  test(`Data Operations navigates on the first click with Collectors open (${mobile ? "mobile" : "desktop"})`, async ({ page }) => {
    await page.route("**/api/v1/data-ops/status", r => r.fulfill({ status: 503, json: {} }));
    if (mobile) await page.setViewportSize({ width: 390, height: 844 });
    await page.goto("/");
    if (mobile) await page.getByRole("button", { name: "Menu", exact: true }).click();
    await page.getByRole("button", { name: "Collectors", exact: true }).click();
    await expect(page.locator("#collector-options")).toBeVisible();
    await page.getByRole("navigation", { name: "Main navigation" }).getByRole("link", { name: "Data Operations", exact: true }).click();
    await expect(page).toHaveURL(/\/data-operations$/);
    await expect(page.getByRole("heading", { name: "Data Operations", exact: true })).toBeVisible();
  });
}

function snapshot(status = "IDLE"): Operations {
  const source = { enabled: true, configured: true, interval_seconds: 43200, status, queued: false, metrics: { accepted: 5, discovered: 12, new_identities: 2, existing_identities: 3, errors: 0, videos: 2, comments: 3, requests: { used: 4, budget: 10, by_endpoint: { search: 1, videos: 1, commentThreads: 2 } } } };
  return { pipeline: { status, basis: "Verified complete cycle and storage health", refresh_interval_seconds: 15 }, collection: { continuous: status === "RUNNING", status, sources: { threads: { ...source }, youtube: { ...source, interval_seconds: 21600 }, pbplus: { ...source, enabled: false, interval_seconds: 86400 } }, runs: [] }, lakehouse: { status: "HEALTHY", backend: "Local Delta Lake", uri: "file:///fixture/lake", readable: true, writable: true, physical_bytes: 2000000, table_count: 10, updated_at: "2026-10-08T01:00:00Z", refreshed_at: "2026-10-08T01:00:00Z", tables: [{ name: "silver/social_posts", exists: true, rows: 100, version: 3, updated_at: "2026-10-08T01:00:00Z" }, { name: "silver/courts", exists: true, rows: 337, version: 2, updated_at: null }] }, warehouse: { status: "HEALTHY", backend: "ClickHouse", host: "localhost:8123", database: "dsp391m", connected: true, exists: true, table: "location_market_signal_snapshot", rows: 8, distinct_locations: 8, latest_snapshot_date: "2026-10-07", gold_generated_at: null, last_successful_load_at: null } };
}


test("homepage separates collectors and infrastructure with source-specific latest-run values", async ({ page }) => {
  const data = snapshot();
  data.collection.sources.threads.metrics = { accepted: 999 };
  data.collection.sources.threads.last_run_id = "threads-latest";
  data.collection.sources.threads.last_completed_at = "2026-10-28T01:05:00Z";
  data.collection.sources.youtube.configured = false;
  data.collection.sources.youtube.metrics = {};
  data.collection.sources.pbplus.metrics = { accepted: 37 };
  data.collection.runs = [{
    run_id: "threads-latest", source: "threads", status: "SUCCESS",
    started_at: "2026-10-28T01:00:00Z", completed_at: "2026-10-28T01:05:00Z",
    duration_seconds: 300, metrics: { accepted: 123 },
  }];
  await page.route("**/api/v1/data-ops/status", route => route.fulfill({ json: data }));
  await page.goto("/");
  const threads = page.getByRole("link", { name: "Open Threads collector" });
  await expect(threads).toContainText("123");
  await expect(threads).not.toContainText("999");
  await expect(threads).toContainText("28 Oct");
  const youtube = page.getByRole("link", { name: "Open YouTube collector" });
  await expect(youtube).toContainText("not configured");
  await expect(youtube).toContainText("No runtime data yet");
  const pbplus = page.getByRole("link", { name: "Open PickleballPlus collector" });
  await expect(pbplus).toContainText("37");
  await expect(pbplus).toHaveAttribute("href", "/collectors/pickleballplus");
  await expect(page.getByRole("heading", { name: "Infrastructure", exact: true })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Scheduler", exact: true })).toBeVisible();
  await expect(page.getByRole("link", { name: "Open collectors", exact: true })).toHaveCount(0);
  await expect(page.getByRole("main").getByText("TikTok Coming soon", { exact: true })).toBeVisible();
});

test("chart totals exclude replay, duplicate, future, old, and missing-count runs", () => {
  const now = Date.parse("2026-10-28T06:00:00Z");
  const at = (hours: number) => new Date(now - hours * 3_600_000).toISOString();
  const run = (id: string, source: "threads" | "youtube" | "pbplus", hours: number, accepted?: number) => ({
    run_id: id, source, status: "SUCCESS", started_at: at(hours + .1), completed_at: at(hours),
    duration_seconds: 360, metrics: { accepted },
  });
  const runs = [
    run("threads-one", "threads", 1, 100), run("threads-one", "threads", 1.5, 100),
    run("youtube-one", "youtube", 2, 250), run("court-one", "pbplus", 72, 40),
    run("too-old", "pbplus", 192, 20), run("future", "youtube", -1, 600),
    { ...run("replay", "threads", .5, 900), operation: "processing_replay" },
    run("missing", "youtube", 4),
  ];
  expect(collectionHistory(runs, "24h", now)).toMatchObject({ total: 350, countedRuns: 2, missingCounts: 1 });
  expect(collectionHistory(runs, "7d", now)).toMatchObject({ total: 390, countedRuns: 3, missingCounts: 1 });
});

test("time controls change real chart totals and expose accessible chart data", async ({ page }) => {
  const data = snapshot("RUNNING");
  const now = Date.now();
  const at = (hours: number) => new Date(now - hours * 3_600_000).toISOString();
  data.collection.runs = [
    { run_id: "threads-one", source: "threads", status: "SUCCESS", started_at: at(1.1), completed_at: at(1), duration_seconds: 360, metrics: { accepted: 100 } },
    { run_id: "youtube-one", source: "youtube", status: "SUCCESS", started_at: at(2.1), completed_at: at(2), duration_seconds: 360, metrics: { accepted: 250 } },
    { run_id: "court-one", source: "pbplus", status: "SUCCESS", started_at: at(72.1), completed_at: at(72), duration_seconds: 360, metrics: { accepted: 40 } },
    { run_id: "replay", source: "threads", status: "SUCCESS", operation: "processing_replay", started_at: at(.6), completed_at: at(.5), duration_seconds: 360, metrics: { accepted: 900 } },
  ];
  data.collection.sources.threads.survey = { completed_queries: 20, candidate_queries: 40 };
  await page.route("**/api/v1/data-ops/status", route => route.fulfill({ json: data }));
  await page.goto("/");
  const chart = page.getByRole("region", { name: "Collection volume" });
  await expect(chart.getByText("350", { exact: true })).toBeVisible();
  await expect(page.getByRole("progressbar", { name: "Threads query progress" })).toHaveAttribute("aria-valuenow", "50");
  await chart.getByRole("button", { name: "7 days", exact: true }).click();
  await expect(chart.getByText("390", { exact: true })).toBeVisible();
  await chart.getByText("View chart data", { exact: true }).click();
  await expect(chart.getByRole("table")).toBeVisible();
  await chart.getByRole("button", { name: "24 hours", exact: true }).click();
  await expect(chart.getByText("350", { exact: true })).toBeVisible();
  await chart.getByText("View chart data", { exact: true }).click();
  await page.screenshot({ path: "/home/coara/Documents/Codex/2026-10-26/i-j/outputs/tracker-populated-preview.png", fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page.screenshot({ path: "/home/coara/Documents/Codex/2026-10-26/i-j/outputs/tracker-populated-mobile.png", fullPage: true });
});

test("empty activity offers a useful operation shortcut without fake chart data", async ({ page }) => {
  const data = snapshot();
  await page.route("**/api/v1/data-ops/status", route => route.fulfill({ json: data }));
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Ready for your first collection" })).toBeVisible();
  await expect(page.getByRole("link", { name: "Open Data Operations", exact: true })).toHaveAttribute("href", "/data-operations");
  await expect(page.getByText("A little quiet. Ready when you are.", { exact: true })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Your workspace", exact: true })).toHaveCount(0);
});
