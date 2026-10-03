/** @jsxImportSource react */
import type { CrawlerSource } from "../types";
import { TrackerTabs } from "./TrackerTabs";

export function CrawlerSourceTabs({ sources, selected, onSelect }: {
  sources: readonly CrawlerSource[];
  selected: string;
  onSelect: (id: string) => void;
}) {
  return <TrackerTabs id="crawler" label="Crawler sources" options={sources}
    selected={selected} onSelect={onSelect} variant="source" />;
}
