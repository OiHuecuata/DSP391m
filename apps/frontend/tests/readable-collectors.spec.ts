import { test, expect } from "@playwright/test";
import type { Operations } from "../src/features/data-ops/types";
function snapshot(status = "IDLE"): Operations {
  const source = { enabled: true, configured: true, interval_seconds: 43200, status, queued: false, metrics: { accepted: 5, discovered: 12, new_identities: 2, existing_identities: 3, errors: 0, videos: 2, comments: 3, requests: { used: 4, budget: 10, by_endpoint: { search: 1, videos: 1, commentThreads: 2 } } } };
  return { pipeline: { status, basis: "Verified complete cycle and storage health", refresh_interval_seconds: 15 }, collection: { continuous: status === "RUNNING", status, sources: { threads: { ...source }, youtube: { ...source, interval_seconds: 21600 }, pbplus: { ...source, enabled: false, interval_seconds: 86400 } }, runs: [] }, lakehouse: { status: "HEALTHY", backend: "Local Delta Lake", uri: "file:///fixture/lake", readable: true, writable: true, physical_bytes: 2000000, table_count: 10, updated_at: "2026-10-08T01:00:00Z", refreshed_at: "2026-10-08T01:00:00Z", tables: [{ name: "silver/social_posts", exists: true, rows: 100, version: 3, updated_at: "2026-10-08T01:00:00Z" }, { name: "silver/courts", exists: true, rows: 337, version: 2, updated_at: null }] }, warehouse: { status: "HEALTHY", backend: "ClickHouse", host: "localhost:8123", database: "dsp391m", connected: true, exists: true, table: "location_market_signal_snapshot", rows: 8, distinct_locations: 8, latest_snapshot_date: "2026-10-07", gold_generated_at: null, last_successful_load_at: null } };
}

test.beforeEach(async ({ page }) => {
 await page.route("**/api/v1/data-ops/status",r=>r.fulfill({json:snapshot()}));
 await page.route("**/api/v1/system/status",r=>r.fulfill({json:{components:[{id:"threads",type:"crawler",status:"running",details:{collected_count:12,raw_count:50,queries_processed:2,query_plan_count:8,queries_failed:0}}]}}));
});
test("Threads explains its numbers and keeps diagnostics accessible",async({page})=>{
 await page.goto("/collectors/threads");
 const overview=page.getByRole("region",{name:"Threads at a glance"});
 await expect(overview.getByRole("heading")).toHaveText("Collecting now");
 await expect(overview.getByText("Items accepted",{exact:true})).toHaveAttribute("title", /not a count of unique people/);
 await expect(overview.getByRole("progressbar",{name:"Search coverage"})).toHaveAttribute("aria-valuenow","25");
 const details=page.locator("details").filter({has:page.locator("summary",{hasText:"Technical details"})});
 await expect(details).not.toHaveAttribute("open", "");
 await details.locator("summary").first().click();
 await expect(details.getByRole("article")).toContainText("Raw scanned");
 await details.locator("summary").first().click();
 await page.screenshot({path:"/home/coara/Documents/Codex/2026-10-26/i-j/outputs/threads-readable.png",fullPage:true});
});
test("YouTube separates videos and comments and does not turn missing data into zero",async({page})=>{
 const value=snapshot();delete value.collection.sources.youtube.metrics!.comments;
 await page.route("**/api/v1/data-ops/status",r=>r.fulfill({json:value}));
 await page.goto("/collectors/youtube");
 const overview=page.getByRole("region",{name:"YouTube at a glance"});
 await expect(overview).toContainText("Videos collected");
 await expect(overview.getByRole("article").filter({hasText:"Comments collected"})).toContainText("Not reported");
 await expect(overview.getByText("Items accepted",{exact:true})).toBeVisible();
 await page.screenshot({path:"/home/coara/Documents/Codex/2026-10-26/i-j/outputs/youtube-readable.png",fullPage:true});
 await page.setViewportSize({width:390,height:844});
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
});
test("TikTok clearly explains that it is not connected",async({page})=>{
 await page.goto("/collectors/tiktok");
 await expect(page.getByRole("heading",{name:"TikTok collection is not available yet."})).toBeVisible();
 await expect(page.getByRole("link",{name:"View available collectors"})).toHaveAttribute("href","/data-operations");
});
test("Data Operations explains collection and preserves run and save controls",async({page})=>{
 await page.goto("/data-operations");
 await expect(page.getByRole("region",{name:"How data collection works"})).toBeVisible();
 await page.getByText("What do these numbers and actions mean?",{exact:true}).click();
 await expect(page.getByText("These are content identifiers, not user identities.",{exact:false})).toBeVisible();
 let body:unknown;
 await page.route("**/api/v1/collection/run",r=>{body=r.request().postDataJSON();return r.fulfill({json:snapshot().collection})});
 await page.getByRole("button",{name:"Run YouTube now",exact:true}).click();
 await expect(page.getByRole("status")).toContainText("Run queued");
 expect(body).toEqual({sources:["youtube"]});
 await page.getByLabel("Threads interval seconds").fill("86400");
 await page.route("**/api/v1/collection/config",r=>{body=r.request().postDataJSON();return r.fulfill({json:snapshot().collection})});
 await page.getByRole("button",{name:"Save source settings",exact:true}).click();
 await expect(page.getByRole("status")).toContainText("settings saved");
 expect(body).toMatchObject({sources:{threads:{interval_seconds:86400}}});
 await page.screenshot({path:"/home/coara/Documents/Codex/2026-10-26/i-j/outputs/data-operations-readable.png",fullPage:true});
 await page.setViewportSize({width:390,height:844});
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
});
