import { readFile, stat } from "node:fs/promises";
import path from "node:path";
import { normalizeSnapshot, type LocationReference } from "@/features/courts/normalize";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET() {
  const root = process.env.DSP_REPO_ROOT ?? path.resolve(process.cwd(), "../..");
  const filename = process.env.PBPLUS_GEO_FILE ?? path.join(root, "apps/collector/.local/pbplus/pbplus_osm_geocoded.json");
  try {
    const info = await stat(/* turbopackIgnore: true */ filename);
    if (!info.isFile() || info.size > 10 * 1024 * 1024) throw new Error("Unsupported snapshot.");
    const raw: unknown = JSON.parse(await readFile(/* turbopackIgnore: true */ filename, "utf8"));
    let references: LocationReference[] = [];
    try {
      const catalog = JSON.parse(await readFile(/* turbopackIgnore: true */ path.join(root, "configs/reference/vietnam_locations.v1.json"), "utf8"));
      if (Array.isArray(catalog.locations)) references = catalog.locations.filter((item: LocationReference) =>
        typeof item?.canonical_name === "string" && Array.isArray(item.aliases) && item.aliases.every(alias => typeof alias === "string"));
    } catch { /* Coordinates remain usable if optional address labels are unavailable. */ }
    return Response.json(normalizeSnapshot(raw, references, info.mtime.toISOString(), path.basename(filename)), { headers: { "Cache-Control": "no-store" } });
  } catch {
    return Response.json({ error: "The local PickleballPlus snapshot is missing, unreadable, or invalid. Check PBPLUS_GEO_FILE on the server." }, { status: 503, headers: { "Cache-Control": "no-store" } });
  }
}
