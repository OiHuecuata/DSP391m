import {NextRequest,NextResponse} from 'next/server';
import {localMutation} from '@/features/settings/server';
import {currentAccount,AuthError} from '@/features/auth/server';
export async function proxy(request:NextRequest){
 const path=request.nextUrl.pathname;
 if(path==='/login'||path.startsWith('/api/auth/'))return NextResponse.next();
 const api=path.startsWith('/api/');
 if(api&&!['GET','HEAD','OPTIONS'].includes(request.method)&&!localMutation(request))return NextResponse.json({error:'Invalid request origin.'},{status:403});
 try{const user=await currentAccount(request.cookies.get('pickleballdb_session')?.value??null);
 if(!user){if(api)return NextResponse.json({error:'Please sign in.'},{status:401});const url=new URL('/login',request.url);url.searchParams.set('next',path+request.nextUrl.search);return NextResponse.redirect(url);}
 if(user.mustChangePassword&&path!=='/account'){if(api)return NextResponse.json({error:'Change your temporary password first.'},{status:428});return NextResponse.redirect(new URL('/account',request.url));}
 if(user.role==='admin'&&(path==='/collectors'||path.startsWith('/collectors/'))){const source=({threads:'threads',youtube:'youtube',pickleballplus:'pbplus',tiktok:'tiktok'} as Record<string,string>)[path.split('/')[2]];const target=new URL('/data-operations',request.url);if(source)target.searchParams.set('source',source);return NextResponse.redirect(target);}
 const admin=path.startsWith('/admin')||path.startsWith('/data-operations')||path.startsWith('/api/admin')||path.startsWith('/api/settings')||path.startsWith('/api/s3/actions')||path.startsWith('/api/v1/collection')||path.startsWith('/api/v1/data-ops');
 if(admin&&user.role!=='admin'){if(api)return NextResponse.json({error:'Administrator access required.'},{status:403});return NextResponse.redirect(new URL('/dashboard',request.url));}
 return NextResponse.next();
 }catch(e){if(api)return NextResponse.json({error:e instanceof AuthError?e.message:'Authentication unavailable.'},{status:503});return NextResponse.redirect(new URL('/login?error=storage',request.url));}
}
export const config={matcher:['/','/dashboard/:path*','/analytics/:path*','/collectors/:path*','/data-operations/:path*','/settings/:path*','/search/:path*','/account/:path*','/admin/:path*','/api/:path*']};
