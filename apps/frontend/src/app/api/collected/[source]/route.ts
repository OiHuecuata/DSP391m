import { readS3Snapshot } from "@/features/s3/snapshot";
export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export async function GET(request: Request, { params }: { params: Promise<{source:string}> }) {
  const { source } = await params;
  if (!["threads","youtube"].includes(source)) return Response.json({error:"Unknown source"},{status:404});
  if (process.env.TRACKER_DATA_SOURCE !== "s3") return Response.json({error:"S3 data is not configured."},{status:503});
  const url = new URL(request.url), kind = url.searchParams.get("kind") ?? "posts";
  const page = Number(url.searchParams.get("page") ?? "1");
  if (!Number.isSafeInteger(page) || page < 1 || page > 100000 || !["posts","comments"].includes(kind) || (source === "threads" && kind === "comments")) return Response.json({error:"Invalid data request"},{status:400});
  try {
    const data = await readS3Snapshot();
    const posts = source === "threads" ? data.social.threads : data.social.youtube;
    const rows = kind === "comments" ? data.social.youtube_comments : posts;
    return Response.json({ mode:data.mode, s3:data.s3, syncedAt:data.synced_at, collectedAt:data.collected_at,
      total:rows.length, counts:{posts:posts.length,comments:source==="youtube"?data.social.youtube_comments.length:0},
      page,pageSize:10,records:rows.slice((page-1)*10,page*10) },{headers:{"Cache-Control":"no-store"}});
  } catch { return Response.json({error:"S3 snapshot is unavailable. Refresh the server's website cache."},{status:503}); }
}
