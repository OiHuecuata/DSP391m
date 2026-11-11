import { readFile, stat } from "node:fs/promises";
import path from "node:path";
import type { CollectionHistory } from "./status";

export type EngagementPoint = { date: string; items: number; likes: number|null; replies: number|null; reposts: number|null; quotes: number|null; views: number|null; likesReported: number; repliesReported: number; repostsReported: number; quotesReported: number; viewsReported: number };
export type SocialRecord = { parentRecordId?:string|null; id: string; source: string; url: string | null; text: string; publishedAt: string | null; collectedAt: string | null; likes: number | null; views: number | null; replies: number | null; reposts?: number|null; quotes?: number|null; hasMedia?: boolean|null; videoDuration?: string|null; tags?: string[] };
export type SnapshotChanges = { comparedAt:string; sources: {id:string;added:number;updated:number;removed:number}[] };
export type WebsiteSnapshot = {
  changes?: SnapshotChanges|null;
  engagement?: {threads: EngagementPoint[]; youtube: EngagementPoint[]}; history?: CollectionHistory; mode: "s3_snapshot"; collected_at: string | null; synced_at: string;
  s3: { uri: string; region: string; versions: Record<string, number>; localCoordinateMatches: number };
  posts: unknown[]; social: { threads: SocialRecord[]; youtube: SocialRecord[]; youtube_comments: SocialRecord[] };
};
export async function readS3Snapshot(): Promise<WebsiteSnapshot> {
  const root = process.env.DSP_REPO_ROOT ?? path.resolve(process.cwd(), "../..");
  const file = process.env.TRACKER_S3_SNAPSHOT ?? path.join(root,".local/website-data/snapshot.json");
  const info = await stat(/* turbopackIgnore: true */ file);
  if (!info.isFile() || info.size > 20 * 1024 * 1024) throw new Error("Invalid website snapshot");
  const value = JSON.parse(await readFile(/* turbopackIgnore: true */ file,"utf8"));
  if (value.mode !== "s3_snapshot" || !Array.isArray(value.posts) || !value.s3?.uri?.startsWith("s3://") || !Array.isArray(value.social?.threads) || !Array.isArray(value.social?.youtube) || !Array.isArray(value.social?.youtube_comments)) throw new Error("Invalid website snapshot");
  return value;
}
