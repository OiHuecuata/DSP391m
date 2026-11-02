"use client";

import Image from "next/image";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { createContext, useContext, useEffect, useRef, useState, type Dispatch, type SetStateAction, type ReactNode } from "react";
import { getOperations } from "@/services/data-ops";
import type { Operations } from "@/features/data-ops/types";
import styles from "./header.module.css";
import { retainSurvey } from "@/features/data-ops/progress";

const iconSizes: Record<string, number[]> = {"imgHouse": [16.0, 16.0], "imgRefreshCw": [16.0, 16.0], "imgIcon5": [17.6, 17.6], "imgStatusIndicator": [5.0, 5.0], "imgIcon": [6.4, 6.4], "imgConnectionIndicator": [6.0, 6.0], "imgIcon12": [15.6001, 16.2122], "imgLayers2": [16.0, 16.0], "imgSearch1": [18.0, 18.0], "imgIcon9": [18.0, 18.0], "imgIcon1": [16.0, 16.0], "imgSearch": [16.0, 16.0], "imgIcon6": [5.35932, 4.80862], "imgIcon2": [16.0, 16.0], "imgStatusIndicator2": [5.0, 5.0], "imgIcon11": [15.6, 15.6], "imgStatusIndicator1": [5.0, 5.0], "imgBrandMark": [26.0, 26.0], "imgIcon7": [14.0, 13.9985], "imgLayers3": [18.0, 18.0], "imgIcon8": [16.0, 16.0], "imgIcon10": [19.6, 19.6], "imgIcon3": [8.53333, 6.4], "imgIcon13": [14.0, 14.0], "imgIcon4": [16.0, 16.0]};
export function Icon({ name, size = 16 }: { name: string; size?: number }) {
  const [width, height] = iconSizes[name] ?? [size, size];
  return <span style={{ width: size, height: size, display: "inline-flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
    <Image src={`/figma/home/${name}.svg`} alt="" width={width} height={height} style={{ width, height, maxWidth: "none" }} unoptimized />
  </span>;
}

export function time(value?: string | null) {
  return value ? new Date(value).toLocaleTimeString("en-GB", { timeZone: "Asia/Ho_Chi_Minh" }) : "—";
}
type WorkspaceStatus = { data: Operations | null; error: string; updated?: string; setData: Dispatch<SetStateAction<Operations | null>> };
const WorkspaceContext = createContext<WorkspaceStatus>({ data: null, error: "", setData: () => {} });
export function useWorkspaceStatus() { return useContext(WorkspaceContext); }

export function AppShell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const title = ({
    "/": "Homepage",
    "/collectors": "Collectors",
    "/collectors/threads": "Threads",
    "/collectors/youtube": "YouTube",
    "/collectors/tiktok": "TikTok",
    "/collectors/pickleballplus": "PickleballPlus",
    "/data-operations": "Data Operations",
  } as Record<string, string>)[pathname] ?? "Tracker";
  const [data, setData] = useState<Operations | null>(null);
  const [error, setError] = useState("");
  const [updated, setUpdated] = useState<string>();
  const [menu, setMenu] = useState(false);
  const [collectorsOpen, setCollectorsOpen] = useState(false);
  const collectorsButton = useRef<HTMLButtonElement>(null);
  const collectorNav = useRef<HTMLDivElement>(null);
  useEffect(() => {
    function dismiss(event: MouseEvent) {
      if (collectorNav.current && !collectorNav.current.contains(event.target as Node)) setCollectorsOpen(false);
    }
    document.addEventListener("click", dismiss);
    return () => document.removeEventListener("click", dismiss);
  }, []);
  const pollInterval = pathname === "/data-operations" ? 3000 : 5000;
  useEffect(() => {
    const abort = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function poll() {
      try {
        const result = await getOperations(abort.signal);
        if (!abort.signal.aborted) { setData(old => ({ ...result, collection: retainSurvey(result.collection, old?.collection) })); setUpdated(new Date().toISOString()); setError(""); }
      } catch (e) {
        if (!abort.signal.aborted) setError(e instanceof Error ? e.message : "Connection unavailable.");
      } finally {
        if (!abort.signal.aborted) timer = setTimeout(poll, pollInterval);
      }
    }
    void poll();
    return () => { abort.abort(); clearTimeout(timer); };
  }, [pollInterval]);
  function nav(label: string, icon: string, href?: string, active = href === pathname) {
    const platformLogos: Record<string, string> = { Threads: "threads.svg", YouTube: "youtube.png", TikTok: "tiktok.png", PickleballPlus: "pickleballplus.png" };
    const content = <>{platformLogos[label] ? <Image className={styles.platformLogo} src={`/brands/${platformLogos[label]}`} alt="" width={18} height={18} unoptimized /> : <Icon name={icon} />}<span>{label}</span></>;
    return href ? <Link className={`${styles.navItem} ${active ? styles.active : ""}`} href={href} aria-current={active ? "page" : undefined} onClick={() => { setMenu(false); setCollectorsOpen(false); }}>{content}</Link> : <div className={styles.navItem} aria-disabled="true">{content}</div>;
  }
  return <div className={styles.shell}>
    <header className={styles.siteHeader}><div className={styles.headerInner}><div className={styles.headerTop}>
      <Link className={styles.brand} href="/" aria-label="Tracker home"><Image src="/tracker-mark.svg" alt="" width={32} height={32} unoptimized /><strong>Tracker<span className={styles.brandPeriod}>.</span></strong></Link>
      <div className={styles.searchPreview}><svg width="21" height="21" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" aria-hidden="true"><circle cx="10.5" cy="10.5" r="7.5"/><path d="m16 16 5 5"/></svg><input aria-label="Search Tracker (preview)" placeholder="Search Tracker…" readOnly aria-disabled="true" tabIndex={-1}/></div>
      <div className={styles.headerStatus}><span className={error ? styles.disconnected : styles.connection}>{error ? "Disconnected" : data ? "Connected" : "Connecting"}</span><small>Local workspace</small></div>
      <button className={styles.menuToggle} aria-expanded={menu} aria-controls="home-navigation" onClick={() => setMenu(!menu)}>{menu ? "Close menu" : "Menu"}</button>
      </div><nav id="home-navigation" className={`${styles.navigation} ${menu ? styles.open : ""}`} aria-label="Main navigation">
        <div ref={collectorNav} className={styles.collectorNav}
          onPointerEnter={event => { if (event.pointerType === "mouse") setCollectorsOpen(true); }}
          onPointerLeave={event => { if (event.pointerType === "mouse") setCollectorsOpen(false); }}
          onFocus={event => { if (event.target.matches(":focus-visible")) setCollectorsOpen(true); }}
          onBlur={event => { if (!event.currentTarget.contains(event.relatedTarget as Node | null)) setCollectorsOpen(false); }}
          onKeyDown={event => {
            if (event.key === "Escape") { collectorsButton.current?.focus(); setCollectorsOpen(false); }
          }}>
          <button ref={collectorsButton} className={`${styles.navItem} ${styles.collectorsToggle}`}
            aria-expanded={collectorsOpen} aria-controls="collector-options"
            onClick={() => { if (window.matchMedia("(hover: none)").matches) setCollectorsOpen(open => !open); }}>
            <Icon name="imgLayers2" /><span>Collectors</span><span className={`${styles.chevron} ${collectorsOpen ? styles.chevronOpen : ""}`} aria-hidden="true">⌄</span>
          </button>
          <div id="collector-options" className={styles.collectorOptions} data-open={collectorsOpen} aria-hidden={!collectorsOpen} inert={!collectorsOpen}>
            {nav("Threads", "imgIcon", "/collectors/threads")}
            {nav("YouTube", "imgIcon1", "/collectors/youtube")}
            {nav("PickleballPlus", "imgIcon2", "/collectors/pickleballplus")}
            {nav("TikTok", "imgIcon3", "/collectors/tiktok")}
          </div>
        </div>
        {nav("Data Operations", "imgIcon4", "/data-operations")}
        {nav("Analytics", "imgIcon5")}
        {nav("Settings", "imgIcon6")}
      </nav>
      </div></header>
    <div className={styles.workspaceBody}>
      {pathname !== "/" && <header className={styles.topbar}>
        <nav className={styles.breadcrumb} aria-label="Breadcrumb"><Link href="/">Homepage</Link><span aria-hidden="true">›</span><span data-testid="page-title" aria-current="page">{title}</span></nav>
        <div className={styles.freshness}><span>Updated {time(updated)} ICT</span><span title={`Status refreshes every ${pollInterval / 1000} seconds`}><Icon name="imgIcon8" /></span></div>
      </header>}
      <WorkspaceContext.Provider value={{ data, error, updated, setData }}>
        {children}
      </WorkspaceContext.Provider>
    </div>
  </div>;
}
