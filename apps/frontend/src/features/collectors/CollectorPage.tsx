"use client";
import {useAuth} from "@/features/auth/AuthProvider";
import Image from "next/image";
import Link from "next/link";
import { VenueSnapshot } from "@/features/courts/VenueSnapshot";
import type { SourceName } from "@/features/data-ops/types";
import { RecordBrowser } from "@/features/search/RecordBrowser";
import { CollectedData } from "./CollectedData";
import styles from "@/features/home/home.module.css";
const collectors = { threads: { name: "Threads", logo: "threads.svg" }, youtube: { name: "YouTube", logo: "youtube.png" }, pbplus: { name: "PickleballPlus", logo: "pickleballplus.png" } };
export function CollectorPage({ source }: { source: SourceName }) {
 const {user}=useAuth();
 const collector = collectors[source];
 return <main className={styles.main}><div className={styles.collectorHeading}><h1><Image src={`/brands/${collector.logo}`} alt="" width={28} height={28} className={styles.collectorLogo} unoptimized />{collector.name}</h1>{user?.role==="admin"&&<Link className={styles.manageLink} href="/data-operations">Data source</Link>}</div>{source === "pbplus" ? <VenueSnapshot /> : <><CollectedData key={source} source={source} /><details className={styles.recordDisclosure}><summary>{source==="threads"?"Browse posts":"Browse videos"}</summary><RecordBrowser key={`records-${source}`} source={source}/></details></>}</main>;
}
