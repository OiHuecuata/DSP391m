import Image from 'next/image';
import Link from 'next/link';
import styles from './footer.module.css';
export function SiteFooter({exploreHref,admin=false}:{exploreHref:string;admin?:boolean}){
 return <footer className={styles.footer}>
  <div className={styles.inner}>
   <div className={styles.columns} data-results-only={!admin}>
    <div className={styles.about}><Link href='/' className={styles.brand} aria-label='PickleballDB homepage'><Image src='/tracker-mark.svg' alt='' width={38} height={38} unoptimized/><strong>PickleballDB</strong></Link><p>Exploring Vietnam’s pickleball supply and social interest.</p></div>
    <nav className={styles.links} aria-label='Footer explore'><h2>Explore</h2>{admin&&<Link href={exploreHref}>Explore data</Link>}<Link href='/dashboard'>Dashboard</Link><Link href='/analytics'>Analytics</Link></nav>
    {admin&&<nav className={styles.links} aria-label='Footer data sources'><h2>Data sources</h2><Link href='/collectors/pickleballplus'>PickleballPlus</Link><Link href='/data-operations?source=threads'>Threads</Link><Link href='/data-operations?source=youtube'>YouTube</Link></nav>}
   </div>
   <div className={styles.bottom}><span>DSP391m · Group 5</span><a href='#page-top'>Back to top <span aria-hidden='true'>↑</span></a></div>
  </div>
 </footer>;
}
