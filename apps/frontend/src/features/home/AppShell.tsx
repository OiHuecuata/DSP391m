"use client";

import Image from "next/image";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { createContext, useContext, useEffect, useState, type Dispatch, type SetStateAction, type ReactNode } from "react";
import {AuthProvider,useAuth} from "@/features/auth/AuthProvider";
import {SiteFooter} from "./SiteFooter";
import {UserMenu} from "@/features/auth/UserMenu";
import { PreferencesProvider, usePreferences, vietnamese } from "@/features/settings/PreferencesProvider";
import { S3Context, type S3Status } from "@/features/s3/status";
import type { Operations } from "@/features/data-ops/types";
import styles from "./header.module.css";


const iconSizes: Record<string, number[]> = {"imgHouse": [16.0, 16.0], "imgRefreshCw": [16.0, 16.0], "imgIcon5": [17.6, 17.6], "imgStatusIndicator": [5.0, 5.0], "imgIcon": [6.4, 6.4], "imgConnectionIndicator": [6.0, 6.0], "imgIcon12": [15.6001, 16.2122], "imgLayers2": [16.0, 16.0], "imgSearch1": [18.0, 18.0], "imgIcon9": [18.0, 18.0], "imgIcon1": [16.0, 16.0], "imgSearch": [16.0, 16.0], "imgIcon6": [5.35932, 4.80862], "imgIcon2": [16.0, 16.0], "imgStatusIndicator2": [5.0, 5.0], "imgIcon11": [15.6, 15.6], "imgStatusIndicator1": [5.0, 5.0], "imgBrandMark": [26.0, 26.0], "imgIcon7": [14.0, 13.9985], "imgLayers3": [18.0, 18.0], "imgIcon8": [16.0, 16.0], "imgIcon10": [19.6, 19.6], "imgIcon3": [8.53333, 6.4], "imgIcon13": [14.0, 14.0], "imgIcon4": [16.0, 16.0]};
export function Icon({ name, size = 16 }: { name: string; size?: number }) {
  if(name === "dashboard") return <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" aria-hidden="true"><rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/></svg>;
  if(name === "imgIcon6") return <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="m9 3-.6 2.5-2.1 1.2-2.4-.7L1.9 9.5l1.8 1.8v2.4l-1.8 1.8L3.9 19l2.4-.7 2.1 1.2L9 22h4l.6-2.5 2.1-1.2 2.4.7 2-3.5-1.8-1.8v-2.4l1.8-1.8-2-3.5-2.4.7-2.1-1.2L13 3Z"/><circle cx="11" cy="12.5" r="3.2"/></svg>;
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

export function AppShell({children}:{children:ReactNode}){return <AuthProvider><PreferencesProvider><WorkspaceShell>{children}</WorkspaceShell></PreferencesProvider></AuthProvider>;}
function WorkspaceShell({ children }: { children: ReactNode }) {
  const {user}=useAuth();
  const {preferences}=usePreferences();
  const t=(s:string)=>preferences.language==="vi"?vietnamese[s]??s:s;
  const pathname = usePathname();
  const title = ({
    "/": "Homepage",
    "/collectors": "Explore data",
    "/collectors/threads": "Threads",
    "/collectors/youtube": "YouTube",
    "/collectors/tiktok": "TikTok",
    "/collectors/pickleballplus": "PickleballPlus",
    "/data-operations": "Data Operations",
    "/analytics": "Analytics",
    "/dashboard": "Dashboard",
    "/settings": "Settings",
    "/admin": "User Management",
    "/account": "My account",
    "/search": "Search",
  } as Record<string, string>)[pathname] ?? "PickleballDB";
  const [data, setData] = useState<Operations | null>(null);
  const [snapshotState, setSnapshot] = useState<{owner:string;data:S3Status}|null>(null);
  const snapshot=snapshotState?.owner===user?.id?snapshotState?.data??null:null;
  const [error, setError] = useState("");
  const [updated, setUpdated] = useState<string>();
  const [menu, setMenu] = useState(false);
  const isLogin=pathname==="/login", userId=user?.id??"", canBrowse=!!user&&!user.mustChangePassword;
  const pollInterval = pathname === "/data-operations" ? 5000 : 30000;
  useEffect(() => {
    if(isLogin||!canBrowse||!userId)return;
    const abort = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function poll() {
      try {
        const response = await fetch("/api/s3/status", { signal: abort.signal, cache: "no-store" });
        if (!response.ok) throw new Error("S3 snapshot unavailable");
        const result: S3Status = await response.json();
        if (!abort.signal.aborted) { setSnapshot({owner:userId,data:result}); setUpdated(result.syncedAt); setError(""); }
      } catch (e) {
        if (!abort.signal.aborted) setError(e instanceof Error ? e.message : "Connection unavailable.");
      } finally {
        if (!abort.signal.aborted) timer = setTimeout(poll, pollInterval);
      }
    }
    void poll();
    return () => { abort.abort(); clearTimeout(timer); };
  }, [pollInterval,isLogin,userId,canBrowse]);
  function nav(label: string, icon: string, href?: string, active = href === pathname) {
    const platformLogos: Record<string, string> = { Threads: "threads.svg", YouTube: "youtube.png", TikTok: "tiktok.png", PickleballPlus: "pickleballplus.png" };
    const content = <>{platformLogos[label] ? <Image className={styles.platformLogo} src={`/brands/${platformLogos[label]}`} alt="" width={18} height={18} unoptimized /> : <Icon name={icon} />}<span>{t(label)}</span></>;
    return href ? <Link className={`${styles.navItem} ${active ? styles.active : ""}`} href={href} aria-current={active ? "page" : undefined} onClick={() => { setMenu(false); }}>{content}</Link> : <div className={styles.navItem} aria-disabled="true">{content}</div>;
  }
  if(pathname === "/login")return <div className={styles.shell}>{children}</div>;
  return <div className={styles.shell}>
    <header id="page-top" tabIndex={-1} className={styles.siteHeader}><div className={styles.headerInner}><div className={styles.headerTop}>
      <Link className={styles.brand} href="/" aria-label="PickleballDB home"><Image src="/tracker-mark.svg" alt="" width={32} height={32} unoptimized /><strong>PickleballDB</strong></Link>

      <div className={styles.accountSlot}><UserMenu/></div>
      <button className={styles.menuToggle} aria-expanded={menu} aria-controls="home-navigation" onClick={() => setMenu(!menu)}>{menu ? "Close menu" : "Menu"}</button>
      </div><nav id="home-navigation" className={`${styles.navigation} ${menu ? styles.open : ""}`} aria-label="Main navigation">
        {user?.role === "admin" && nav("Data Operations", "imgIcon4", "/data-operations")}
        {nav("Dashboard", "dashboard", "/dashboard")}
        {nav("Analytics", "imgIcon5", "/analytics")}
        {nav("Settings", "imgIcon6", "/settings")}
      </nav>
      </div></header>
    <div className={styles.workspaceBody}>
      {pathname !== "/" && <header className={styles.topbar}>
        <nav className={styles.breadcrumb} aria-label="Breadcrumb"><Link href="/">{t("Homepage")}</Link><span aria-hidden="true">›</span><span data-testid="page-title" aria-current="page">{t(title)}</span></nav>
      </header>}
      <WorkspaceContext.Provider value={{ data, error, updated, setData }}>
        <S3Context.Provider value={{ data: snapshot, error }}>{children}</S3Context.Provider>
      </WorkspaceContext.Provider>
    </div>
    <SiteFooter admin={user?.role==="admin"} exploreHref={user?.role==="admin"?"/search":"/collectors"}/>
  </div>;
}
