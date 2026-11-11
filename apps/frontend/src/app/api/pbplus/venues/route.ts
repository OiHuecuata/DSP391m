import { readS3Snapshot } from "@/features/s3/snapshot";
import { readFile, stat } from "node:fs/promises";
import path from "node:path";
import { normalizeSnapshot, type LocationReference } from "@/features/courts/normalize";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET() {
  const root = process.env.DSP_REPO_ROOT ?? path.resolve(process.cwd(), "../..");
  const filename = process.env.PBPLUS_GEO_FILE ?? path.join(root, "apps/collector/.local/pbplus/pbplus_osm_geocoded.json");
  try {
    const s3 = process.env.TRACKER_DATA_SOURCE === "s3" ? await readS3Snapshot() : null;
    const info = s3 ? null : await stat(/* turbopackIgnore: true */ filename);
    if (info && (!info.isFile() || info.size > 10 * 1024 * 1024)) throw new Error("Unsupported snapshot.");
    const raw: unknown = s3 ?? JSON.parse(await readFile(/* turbopackIgnore: true */ filename, "utf8"));
    let references: LocationReference[] = [];
    try {
      const catalog = JSON.parse(await readFile(/* turbopackIgnore: true */ path.join(root, "configs/reference/vietnam_locations.v1.json"), "utf8"));
      if (Array.isArray(catalog.locations)) references = catalog.locations.filter((item: LocationReference) =>
        typeof item?.canonical_name === "string" && Array.isArray(item.aliases) && item.aliases.every(alias => typeof alias === "string"));
    } catch { /* Coordinates remain usable if optional address labels are unavailable. */ }
    const snapshot = normalizeSnapshot(raw, references, s3?.synced_at ?? info!.mtime.toISOString(), s3 ? `S3 Silver courts · version ${s3.s3.versions.courts}` : path.basename(filename));
    return Response.json(s3 ? { ...snapshot, mode:"s3_snapshot", s3:s3.s3 } : snapshot, { headers: { "Cache-Control": "no-store" } });
  } catch {
    return Response.json({ error: "Court data is unavailable. Check the configured server snapshot and refresh the cache." }, { status: 503, headers: { "Cache-Control": "no-store" } });
  }
}
