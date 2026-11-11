import { snapshotOverview } from "@/features/s3/overview";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { readS3Snapshot } from "@/features/s3/snapshot";
import { normalizeSnapshot } from "@/features/courts/normalize";
export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export async function GET() {
  try {
    if (process.env.TRACKER_DATA_SOURCE !== "s3") throw new Error("S3 not configured");
    const s = await readS3Snapshot();
    let sync = null;
    try { sync = JSON.parse(await readFile(/* turbopackIgnore: true */ path.join(process.env.DSP_REPO_ROOT ?? path.resolve(process.cwd(),"../.."),".local/website-data/sync-status.json"),"utf8")); } catch {}
    const courts = normalizeSnapshot(s, [], s.synced_at, "S3 Silver courts");
    return Response.json({ ...await snapshotOverview(s), sync, history: s.history, syncedAt: s.synced_at, collectedAt: s.collected_at, ...s.s3,
      counts: { courts: courts.venues.length, mapped: courts.venues.filter(v => v.latitude != null && v.longitude != null).length, threads: s.social.threads.length, youtube: s.social.youtube.length, comments: s.social.youtube_comments.length }
    }, { headers: { "Cache-Control": "no-store" } });
  } catch { return Response.json({ error: "S3 snapshot unavailable. Refresh the S3 export and try again." }, { status: 503 }); }
}
