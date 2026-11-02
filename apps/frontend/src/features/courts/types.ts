export type CourtVenue = {
  id: string; name: string; address: string; area: string; url: string | null;
  courts: number | null; services: string[]; openingHours: string[]; latitude: number | null; longitude: number | null;
};
export type CourtSnapshot = {
  source: "pbplus"; mode: "local_snapshot"; filename: string;
  collectedAt: string | null; fileUpdatedAt: string; loadedAt: string;
  total: number; mapped: number; unmapped: number; duplicateCoordinates: number;
  areas: { name: string; total: number; mapped: number }[];
  venues: CourtVenue[];
};
