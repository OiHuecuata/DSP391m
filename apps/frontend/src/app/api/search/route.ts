import {requireAccount,AuthError} from '@/features/auth/server';
import { searchRecords, recordsCsv } from "@/features/search/query";
export const runtime="nodejs";
export const dynamic="force-dynamic";
export async function GET(request:Request){try{
 const params=new URL(request.url).searchParams,user=await requireAccount();
 if(user.role!=='admin')throw new AuthError('Data browsing is for administrators.',403);
 const result=await searchRecords(params);
 const download=params.get("download"),scope=params.get("scope")??"all";
 if(!["all","page"].includes(scope)||download&&!["csv","json"].includes(download))throw new RangeError("Invalid download options");
 const records=scope==="page"?result.records:result.all;
 if(download==="csv")return new Response(recordsCsv(records),{headers:{"Content-Type":"text/csv; charset=utf-8","Content-Disposition":'attachment; filename="pickleballdb-records.csv"',"Cache-Control":"no-store"}});
 if(download==="json")return Response.json({count:records.length,total:result.total,syncedAt:result.syncedAt,records},{headers:{"Content-Disposition":'attachment; filename="pickleballdb-records.json"',"Cache-Control":"no-store"}});
 const {all,...response}=result;void all;return Response.json(response,{headers:{"Cache-Control":"no-store"}});
}catch(error){return Response.json({error:error instanceof AuthError||error instanceof RangeError?error.message:"Search data unavailable. Try again after the S3 sync."},{status:error instanceof AuthError?error.status:error instanceof RangeError?400:503});}}
