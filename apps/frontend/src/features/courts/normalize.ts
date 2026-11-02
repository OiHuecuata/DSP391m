import type { CourtSnapshot, CourtVenue } from "./types";

export type LocationReference = { canonical_name: string; aliases: string[] };
const clean = (value: unknown, limit = 500) => typeof value === "string" ? value.trim().slice(0, limit) : "";
export function normalizeSearch(value: string) {
  return value.normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/đ/g, "d").replace(/Đ/g, "D").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}
function addressArea(address: string, references: LocationReference[]) {
  const text = " " + normalizeSearch(address) + " ";
  let label = "Unresolved address area", lastIndex = -1, longest = 0;
  for (const reference of references) for (const alias of reference.aliases) {
    const normalized = normalizeSearch(alias);
    const index = text.lastIndexOf(" " + normalized + " ");
    if (normalized && (index > lastIndex || index >= 0 && index === lastIndex && normalized.length > longest)) {
      label = reference.canonical_name; lastIndex = index; longest = normalized.length;
    }
  }
  return label;
}
function sourceUrl(value: unknown) {
  try { const url = new URL(clean(value, 2000)); return ["pickleballplus.vn", "www.pickleballplus.vn"].includes(url.hostname) && ["https:", "http:"].includes(url.protocol) ? url.href : null; } catch { return null; }
}
export function normalizeSnapshot(raw: unknown, references: LocationReference[], fileUpdatedAt: string, filename: string): CourtSnapshot {
  if (!raw || typeof raw !== "object" || !("posts" in raw) || !Array.isArray(raw.posts)) throw new Error("Invalid court snapshot.");
  const root = raw as Record<string, unknown>;
  const unique = new Map<string, CourtVenue>();
  for (const [index, item] of raw.posts.entries()) {
    if (!item || typeof item !== "object") continue;
    const value = item as Record<string, unknown>;
    const address = clean(value.address, 1500);
    const id = clean(value.id, 2000) || clean(value.code, 100) || "listing-" + index;
    if (unique.has(id)) continue;
    const valid = typeof value.latitude === "number" && Number.isFinite(value.latitude) && Math.abs(value.latitude) <= 90 &&
      typeof value.longitude === "number" && Number.isFinite(value.longitude) && Math.abs(value.longitude) <= 180;
    unique.set(id, {
      id, name: clean(value.court_name, 250) || "Unnamed venue", address,
      area: addressArea(address, references), url: sourceUrl(value.url),
      courts: typeof value.number_of_courts === "number" && Number.isFinite(value.number_of_courts) && value.number_of_courts >= 0 ? value.number_of_courts : null,
      services: Array.isArray(value.services) ? value.services.filter((v): v is string => typeof v === "string").slice(0, 20).map(v => v.slice(0, 150)) : [],
      openingHours: Array.isArray(value.opening_hours) ? value.opening_hours.filter((v): v is string => typeof v === "string").slice(0, 30).map(v => v.trim().slice(0, 250)) : [],
      latitude: valid ? value.latitude as number : null, longitude: valid ? value.longitude as number : null,
    });
  }
  const venues = [...unique.values()];
  const areas = new Map<string, { name: string; total: number; mapped: number }>();
  const points = new Set<string>();
  let mapped = 0;
  for (const venue of venues) {
    const area = areas.get(venue.area) ?? { name: venue.area, total: 0, mapped: 0 };
    area.total++;
    if (venue.latitude != null && venue.longitude != null) {
      mapped++; area.mapped++; points.add(venue.latitude + "," + venue.longitude);
    }
    areas.set(venue.area, area);
  }
  const collected = clean(root.collected_at, 100);
  return {
    source: "pbplus", mode: "local_snapshot", filename,
    collectedAt: collected && Number.isFinite(Date.parse(collected)) ? collected : null,
    fileUpdatedAt, loadedAt: new Date().toISOString(),
    total: venues.length, mapped, unmapped: venues.length - mapped, duplicateCoordinates: mapped - points.size,
    areas: [...areas.values()].sort((a, b) => b.total - a.total || a.name.localeCompare(b.name)), venues,
  };
}
