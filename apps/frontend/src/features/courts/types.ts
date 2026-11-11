export type CourtVenue = {
  collectedAt?: string|null; id: string; name: string; address: string; area: string; url: string | null;
  coordinateSource?: "local_geocoding" | "source_snapshot" | null;
  courts: number | null; services: string[]; openingHours: string[]; latitude: number | null; longitude: number | null;
};
export type CourtSnapshot = {
  source: "pbplus"; mode: "local_snapshot" | "s3_snapshot"; s3?: { uri: string; region: string; versions: Record<string, number>; localCoordinateMatches: number }; filename: string;
  collectedAt: string | null; fileUpdatedAt: string; loadedAt: string;
  total: number; mapped: number; unmapped: number; duplicateCoordinates: number;
  areas: { name: string; total: number; mapped: number }[];
  venues: CourtVenue[];
};
