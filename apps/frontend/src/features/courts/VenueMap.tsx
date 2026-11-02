"use client";

import { useEffect, useRef, useState } from "react";
import L from "leaflet";
import "leaflet.markercluster";
import "leaflet/dist/leaflet.css";
import "leaflet.markercluster/dist/MarkerCluster.css";
import type { CourtVenue } from "./types";
import styles from "./courts.module.css";

export default function VenueMap({ venues, selected, focusToken, onSelect }: {
  venues: CourtVenue[]; selected: string | null; focusToken: number; onSelect: (id: string) => void;
}) {
  const container = useRef<HTMLDivElement>(null);
  const map = useRef<L.Map | null>(null);
  const group = useRef<L.MarkerClusterGroup | null>(null);
  const markers = useRef(new Map<string, L.Marker>());
  const [tileError, setTileError] = useState(false);
  useEffect(() => {
    if (!container.current) return;
    const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    const instance = L.map(container.current, { scrollWheelZoom: false, zoomAnimation: !reduceMotion, fadeAnimation: !reduceMotion }).setView([16.2, 106.4], 6);
    map.current = instance;
    const tiles = L.tileLayer(process.env.NEXT_PUBLIC_MAP_TILE_URL ?? "https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 19, attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    }).addTo(instance);
    tiles.on("tileerror", () => setTileError(true));
    tiles.on("load", () => { if (container.current?.querySelector(".leaflet-tile-loaded")) setTileError(false); });
    group.current = L.markerClusterGroup({
      maxClusterRadius: 48, showCoverageOnHover: false, spiderfyOnMaxZoom: true, animate: !reduceMotion,
      iconCreateFunction: cluster => L.divIcon({
        className: styles.clusterIcon, html: '<span>' + cluster.getChildCount() + '</span>',
        iconSize: [40, 40], iconAnchor: [20, 20],
      }),
    }).addTo(instance);
    const observer = new ResizeObserver(() => instance.invalidateSize());
    observer.observe(container.current);
    return () => { observer.disconnect(); instance.remove(); map.current = null; group.current = null; };
  }, []);

  useEffect(() => {
    const instance = map.current, clusters = group.current;
    if (!instance || !clusters) return;
    clusters.clearLayers(); markers.current.clear();
    const mapped = venues.filter(v => v.latitude != null && v.longitude != null);
    for (const venue of mapped) {
      const marker = L.marker([venue.latitude!, venue.longitude!], {
        title: venue.name, alt: venue.name, keyboard: true,
        icon: L.divIcon({ className: styles.venuePin, html: "<span></span>", iconSize: [18, 18], iconAnchor: [9, 9] }),
      });
      const popup = document.createElement("div");
      const title = document.createElement("strong"); title.textContent = venue.name; popup.append(title);
      const address = document.createElement("p"); address.textContent = venue.address || "Address unavailable"; popup.append(address);
      const count = document.createElement("small"); count.textContent = venue.courts == null ? "Court count unavailable" : venue.courts + " courts · source reported, unverified"; popup.append(count);
      if (venue.url) { const link = document.createElement("a"); link.href = venue.url; link.target = "_blank"; link.rel = "noopener noreferrer"; link.textContent = "View PickleballPlus listing ↗"; popup.append(document.createElement("br"), link); }
      marker.bindPopup(popup).on("click", () => onSelect(venue.id));
      markers.current.set(venue.id, marker); clusters.addLayer(marker);
    }
    if (mapped.length) instance.fitBounds(L.latLngBounds(mapped.map(v => [v.latitude!, v.longitude!] as [number, number])), { padding: [30, 30], maxZoom: 14, animate: false });
    else instance.setView([16.2, 106.4], 6, { animate: false });
  }, [venues, onSelect]);

  useEffect(() => {
    const marker = selected ? markers.current.get(selected) : undefined;
    if (marker && group.current) group.current.zoomToShowLayer(marker, () => marker.openPopup());
  }, [selected, focusToken]);

  return <div className={styles.mapFrame}>
    <div ref={container} className={styles.map} role="region" aria-label="Map of geocoded PickleballPlus venue listings" />
    {!venues.some(v => v.latitude != null && v.longitude != null) && <div className={styles.mapEmpty}>No mapped listings match these filters.</div>}
    {tileError && <p role="status" className={styles.tileNotice}>Some map tiles could not load. Markers and the venue list are still available.</p>}
  </div>;
}
