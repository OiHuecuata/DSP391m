import type { CrawlerDetails, QueryStatus, SystemStatus } from "@/features/system/types";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function optionalFields(value: Record<string, unknown>, strings: readonly string[], numbers: readonly string[]): boolean {
  return strings.every((key) => value[key] == null || typeof value[key] === "string") &&
    numbers.every((key) => value[key] == null ||
      (typeof value[key] === "number" && Number.isFinite(value[key]) && value[key] >= 0));
}

function isQuery(value: unknown): value is QueryStatus {
  return isRecord(value) && typeof value.query === "string" &&
    optionalFields(value, ["kind", "geo_name", "status", "error"], ["raw_count", "accepted_count", "total_collected"]);
}

function isCrawlerDetails(value: unknown): value is CrawlerDetails {
  return isRecord(value) && optionalFields(value,
    ["seed_query", "sort", "started_at", "finished_at", "message", "error"],
    ["target_count", "collected_count", "raw_count", "query_plan_count", "queries_processed", "queries_succeeded", "queries_failed"]) &&
    ["current_query", "last_query"].every((key) => value[key] == null || isQuery(value[key]));
}

function isSystemStatus(value: unknown): value is SystemStatus {
  return isRecord(value) && Array.isArray(value.components) &&
    value.components.every((component: unknown) => isRecord(component) &&
      typeof component.id === "string" && typeof component.type === "string" &&
      typeof component.status === "string" && optionalFields(component, ["updated_at"], []) &&
      (component.details == null || (component.type === "crawler"
        ? isCrawlerDetails(component.details) : isRecord(component.details))));
}

export async function getSystemStatus(signal: AbortSignal): Promise<SystemStatus> {
  const response = await fetch("/api/v1/system/status", {
    cache: "no-store",
    headers: { Accept: "application/json" },
    signal: AbortSignal.any([signal, AbortSignal.timeout(10_000)]),
  });
  if (!response.ok) throw new Error(`Status request failed (HTTP ${response.status}).`);
  const data: unknown = await response.json();
  if (!isSystemStatus(data)) throw new Error("The backend returned an invalid system status response.");
  return data;
}
