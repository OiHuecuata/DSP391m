import type { Collection, Operations, SourceConfig, SourceName } from "@/features/data-ops/types";

export async function getOperations(signal: AbortSignal): Promise<Operations> {
  const response = await fetch("/api/v1/data-ops/status", { cache: "no-store", signal: AbortSignal.any([signal, AbortSignal.timeout(10_000)]) });
  if (!response.ok) throw new Error(`Operations request failed (HTTP ${response.status}).`);
  const value = await response.json() as Operations;
  if (!value?.pipeline || !value.collection?.sources || !Array.isArray(value.collection.runs) || !Array.isArray(value.lakehouse?.tables) || !value.warehouse || ["threads", "youtube", "pbplus"].some((s) => !value.collection.sources[s as SourceName])) throw new Error("Invalid operations response.");
  return value;
}

export async function controlCollection(action: "start" | "stop" | "run" | "config" | "retry", body?: { sources: SourceConfig | SourceName[] } | { run_id: string }): Promise<Collection> {
  const response = await fetch(`/api/v1/collection/${action}`, { method: action === "config" ? "PATCH" : "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body ?? {}), signal: AbortSignal.timeout(10_000) });
  if (!response.ok) {
    const value = await response.json().catch(() => null);
    throw new Error(value?.error ?? `Collection control failed (HTTP ${response.status}).`);
  }
  return response.json();
}
