import Image from "next/image";
import Link from "next/link";
import styles from "@/features/home/home.module.css";
import presentation from "@/features/collectors/collectors.module.css";
export default function Page() {
  return <main className={styles.main}><div className={styles.collectorHeading}><div><h1><Image src="/brands/tiktok.png" alt="" width={28} height={28} unoptimized />TikTok</h1></div></div><section className={presentation.setup}><span className={presentation.eyebrow}>NOT CONNECTED YET</span><h2>TikTok collection is not available yet.</h2><p>This workspace does not currently receive TikTok collection data. There are no verified video counts, comments, or collection results to show here.</p><p>You can collect from the supported sources in Data Operations. TikTok needs a backend collector connection before this page can display live results.</p><Link href="/data-operations">View available collectors →</Link></section></main>;
}
