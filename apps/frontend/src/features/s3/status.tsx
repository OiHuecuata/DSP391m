"use client";
import type { SnapshotChanges } from "./snapshot";
import {defaults,formatDate,preferenceKey,validatePreferences} from "@/features/settings/preferences";
import { createContext, useContext } from "react";
export type CollectionHistory = { timezone: string; basis: "silver_observations"; skippedUndated: number; days: { date: string; counts: { threads: number; youtube: number; pbplus: number } }[] };
export type S3Status = { freshness?:Record<string,string|null>; coverage?:{provinces:number;totalProvinces:number;sources:{id:string;total:number;matched:number;unmatched:number}[]}|null; changes?:SnapshotChanges|null; sync?: {state:string;message:string;checkedAt:string}|null; history?: CollectionHistory; syncedAt: string; collectedAt: string | null; uri: string; region: string; versions: Record<string, number>; localCoordinateMatches: number; counts: { courts: number; mapped: number; threads: number; youtube: number; comments: number } };
export const S3Context = createContext<{ data: S3Status | null; error: string }>({ data: null, error: "" });
export const useS3Status = () => useContext(S3Context);
export const snapshotDate = (value?:string|null)=>{let preferences=defaults;if(typeof window!=="undefined")try{preferences=validatePreferences(JSON.parse(localStorage.getItem(document.documentElement.dataset.preferenceKey??preferenceKey)??localStorage.getItem(preferenceKey)??"null"))}catch{}return formatDate(value,preferences);};
