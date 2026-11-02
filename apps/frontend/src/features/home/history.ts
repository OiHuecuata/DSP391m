import type { Run, SourceName } from "@/features/data-ops/types";

export type HistoryRange = "24h" | "7d";
export type HistoryBin = { start: number; end: number; counts: Record<SourceName, number>; runs: number };
export function collectionHistory(runs: Run[], range: HistoryRange, now: number) {
  const count = range === "24h" ? 24 : 7;
  const step = range === "24h" ? 3_600_000 : 86_400_000;
  const start = now - count * step;
  const bins: HistoryBin[] = Array.from({ length: count }, (_, i) => ({
    start: start + i * step, end: start + (i + 1) * step,
    counts: { threads: 0, youtube: 0, pbplus: 0 }, runs: 0,
  }));
  const seen = new Set<string>();
  let total = 0, countedRuns = 0, missingCounts = 0;
  const completed = runs.filter(run => run.completed_at && run.operation !== "processing_replay")
    .sort((a, b) => Date.parse(b.completed_at!) - Date.parse(a.completed_at!));
  for (const run of completed) {
    const key = run.source + ":" + run.run_id;
    if (seen.has(key)) continue;
    seen.add(key);
    const timestamp = Date.parse(run.completed_at!);
    if (!Number.isFinite(timestamp) || timestamp < start || timestamp > now) continue;
    const value = run.metrics?.accepted;
    if (value == null || !Number.isFinite(value) || value < 0) { missingCounts++; continue; }
    const index = Math.min(count - 1, Math.floor((timestamp - start) / step));
    bins[index].counts[run.source] += value;
    bins[index].runs++;
    countedRuns++;
    total += value;
  }
  return { bins, total, countedRuns, missingCounts };
}
