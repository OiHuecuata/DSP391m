"use client";

import dynamic from "next/dynamic";
import { useCallback, useEffect, useState } from "react";
import {usePreferences} from "@/features/settings/PreferencesProvider";
import { snapshotDate } from "@/features/s3/status";
import { useS3Status } from "@/features/s3/status";
import { serviceLabels } from "./filters";
import { courtDownload } from "./download";
import type { CourtSnapshot } from "./types";
import styles from "./courts.module.css";

const VenueMap = dynamic(() => import("./VenueMap"), { ssr: false, loading: () => <div className={styles.mapLoading}>Loading interactive map…</div> });
const date = (value:string|null)=>value?snapshotDate(value):"Unavailable";
const pageSize = 10;
export function VenueSnapshot() {
  const {preferences}=usePreferences();
  const {data:snapshotStatus}=useS3Status();
  const [data, setData] = useState<CourtSnapshot | null>(null);
  const [error, setError] = useState("");
  const [reload, setReload] = useState(0);
  const [busy, setBusy] = useState(true);
  const [formatOverride, setDownloadFormat] = useState<string|null>(null);
  const downloadFormat=formatOverride??preferences.downloadFormat;
  const [downloadNotice, setDownloadNotice] = useState("");
  const [page, setPage] = useState(0);
  const [selected, setSelected] = useState<string | null>(null);
  const [focusToken, setFocusToken] = useState(0);
  const selectVenue = useCallback((id: string) => { setSelected(id); setFocusToken(value => value + 1); }, []);
  useEffect(() => {
    const abort = new AbortController();
    async function load() {
      setBusy(true);
      try {
        const response = await fetch("/api/pbplus/venues", { cache: "no-store", signal: AbortSignal.any([abort.signal, AbortSignal.timeout(15000)]) });
        const value = await response.json();
        if (!response.ok) throw new Error(value.error ?? "Unable to load court locations.");
        if (!Array.isArray(value.venues) || !Array.isArray(value.areas) || !["local_snapshot", "s3_snapshot"].includes(value.mode)) throw new Error("Invalid venue snapshot.");
        if (!abort.signal.aborted) { setData(value); setError(""); setSelected(null); }
      } catch (cause) { if (!abort.signal.aborted) setError(cause instanceof Error ? cause.message : "Unable to load court locations."); }
      finally { if (!abort.signal.aborted) setBusy(false); }
    }
    void load();
    return () => abort.abort();
  }, [reload,snapshotStatus?.syncedAt]);
  const venues = data?.venues ?? [];
  function downloadVenues() {
    if (!data) return;
    const venues = preferences.downloadScope==="page"?data.venues.slice(currentPage*pageSize,(currentPage+1)*pageSize):data.venues;
    if (!venues.length) return;
    const result = courtDownload(data, venues, downloadFormat, preferences.downloadScope);
    const url = URL.createObjectURL(new Blob([result.content], { type: result.mime }));
    const link = document.createElement("a"); link.href = url; link.download = result.filename;
    document.body.append(link); link.click(); link.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 1000);
    setDownloadNotice(`Download prepared: ${venues.length} venue listings as ${downloadFormat.toUpperCase()}.`);
  }
  const mapped = venues.filter(venue => venue.latitude != null).length;
  const pages = Math.max(1, Math.ceil(venues.length / pageSize));
  const currentPage = Math.min(page, pages - 1);
  const chosen = venues.find(venue => venue.id === selected);
  return <section className={styles.snapshot} aria-labelledby="court-locations">
    <div className={styles.heading}><div><h2 id="court-locations">Court locations</h2></div><div className={styles.headingActions}><button disabled={busy} onClick={() => setReload(value => value + 1)}>{busy ? "Loading…" : "Refresh snapshot"}</button></div></div>
    {error && <div role="alert" className={styles.error}>{error}{data && " Showing the previously loaded snapshot."}</div>}
    {!data && busy && <p role="status" className={styles.loading}>Reading local venue snapshot…</p>}
    {data && <>
      <VenueMap venues={venues} selected={selected} focusToken={focusToken} onSelect={selectVenue} />
      <div className={styles.mapCaption}><span>{mapped.toLocaleString()} mapped / {venues.length.toLocaleString()} total listings · {data.unmapped} without coordinates{data.s3?.localCoordinateMatches ? ` · ${data.s3.localCoordinateMatches} positions from local geocoding` : ""}</span><span>Click a cluster to zoom · Click a pin for details</span></div>
      {chosen && <aside className={styles.selected} aria-label="Selected venue"><div><h3>{chosen.name}</h3><p>{chosen.address || "Address unavailable"}</p><p>{chosen.courts == null ? "Court count unavailable" : chosen.courts + " courts · source reported, unverified"} · {chosen.latitude == null ? "No coordinates" : chosen.latitude.toFixed(5) + ", " + chosen.longitude!.toFixed(5)}</p><p>Services: {chosen.services.length ? chosen.services.map(service => serviceLabels[service] ?? service).join(" · ") : "Not reported"}</p><details className={styles.hoursDetails}><summary>Reported opening hours</summary>{chosen.openingHours?.length ? <ul>{chosen.openingHours.map((line,index) => <li key={index}>{line}</li>)}</ul> : <p>Opening hours not reported.</p>}</details>{chosen.url && <a href={chosen.url} target="_blank" rel="noopener noreferrer">View original listing ↗</a>}</div><button aria-label="Close selected venue" onClick={() => setSelected(null)}>×</button></aside>}
      <details className={styles.hoursDetails}><summary>Newest collected venues</summary><ul>{[...venues].filter(v=>v.collectedAt).sort((a,b)=>Date.parse(b.collectedAt!)-Date.parse(a.collectedAt!)).slice(0,3).map(v=><li key={v.id}><button onClick={()=>selectVenue(v.id)}>{v.name}</button> · {date(v.collectedAt!)}</li>)}</ul>{!venues.some(v=>v.collectedAt)&&<p>Individual collection dates are not available in this snapshot.</p>}</details>
      <div className={styles.listHeading}><h3>Court listings · CSV preview <span>{venues.length}</span></h3></div>
      <div className={styles.tableScroll}><table><caption className={styles.srOnly}>PickleballPlus court data preview</caption><thead><tr><th>Venue</th><th>Address area</th><th>Courts (reported)</th><th>Map position</th></tr></thead><tbody>{venues.slice(currentPage * pageSize, (currentPage + 1) * pageSize).map(venue => <tr key={venue.id} data-selected={selected === venue.id}><td><button onClick={() => selectVenue(venue.id)}>{venue.name}</button><p>{venue.address || "Address unavailable"}</p></td><td>{venue.area}</td><td>{venue.courts ?? "—"}</td><td>{venue.latitude == null ? <span className={styles.missing}>No coordinates</span> : <button className={styles.mapLink} onClick={() => selectVenue(venue.id)}>Show on map ↗</button>}</td></tr>)}</tbody></table>{!venues.length && <div role="status" className={styles.loading}>No court listings are available in this snapshot.</div>}</div>
      <div className={styles.pagination}><span>{venues.length ? currentPage * pageSize + 1 : 0}–{Math.min((currentPage + 1) * pageSize, venues.length)} of {venues.length} listings</span><div><button disabled={currentPage === 0} onClick={() => setPage(currentPage - 1)}>Previous</button><span>{currentPage + 1} / {pages}</span><button disabled={currentPage >= pages - 1} onClick={() => setPage(currentPage + 1)}>Next</button></div></div>
      <footer className={styles.provenance}><span>Source: {data.filename} · Collected {date(data.collectedAt)}</span><span>File updated {date(data.fileUpdatedAt)}</span></footer>
      <div className={styles.downloadBar} aria-label="Download court listings"><div><strong>Download court listings</strong><p>{preferences.downloadScope==="page"?Math.min(pageSize,venues.length-currentPage*pageSize):venues.length} listings · {preferences.downloadScope==="page"?"current page":"all listings"}</p></div><label><span>File format</span><select value={downloadFormat} onChange={event => setDownloadFormat(event.target.value)}><option value="csv">CSV · spreadsheet</option><option value="json">JSON · structured data</option></select></label><button disabled={!venues.length} onClick={downloadVenues}>Download file ↓</button></div>
      {downloadNotice && <p role="status" className={styles.filterHelp}>{downloadNotice}</p>}
    </>}
  </section>;
}
