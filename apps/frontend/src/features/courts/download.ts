import type { CourtSnapshot, CourtVenue } from "./types";

function csvCell(value: string | number | null) {
  let text = value == null ? "" : String(value);
  // Keep spreadsheet apps from interpreting source-provided text as formulas.
  if (typeof value === "string" && (/^\s*[=+@-]/.test(text) || /^[\t\r\n]/.test(text))) text = "'" + text;
  return '"' + text.replaceAll('"', '""') + '"';
}
export function courtDownload(snapshot: CourtSnapshot, venues: CourtVenue[], format: string, scope: string) {
  const collected = snapshot.collectedAt?.slice(0,10) ?? "undated";
  const name = `pickleball-courts-${scope === "all" ? "all" : "filtered"}-${collected}`;
  if (format === "json") return {
    filename: name + ".json", mime: "application/json;charset=utf-8",
    content: JSON.stringify({ source: snapshot.source, source_file: snapshot.filename, collected_at: snapshot.collectedAt,
      file_updated_at: snapshot.fileUpdatedAt, exported_at: new Date().toISOString(), count: venues.length,
      note: "Source-reported venue listings; coordinates are geocoded estimates. Hours and facilities are not live availability.", venues }, null, 2),
  };
  const header = ["Venue", "Address", "Address area", "Courts (reported)", "Services (reported)", "Opening hours (ICT, reported)", "Latitude (estimated)", "Longitude (estimated)", "Source URL", "Collected at"];
  const rows = venues.map(v => [v.name, v.address, v.area, v.courts, v.services.join("; "), (v.openingHours ?? []).join("; "), v.latitude, v.longitude, v.url, snapshot.collectedAt].map(csvCell).join(","));
  return { filename: name + ".csv", mime: "text/csv;charset=utf-8", content: "\uFEFF" + header.map(csvCell).join(",") + "\r\n" + rows.join("\r\n") };
}
