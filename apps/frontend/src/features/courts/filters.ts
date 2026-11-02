import type { CourtVenue } from "./types";
import { normalizeSearch } from "./normalize";

export const weekdays = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
const dayNames = ["chu nhat", "thu hai", "thu ba", "thu tu", "thu nam", "thu sau", "thu bay"];
export const serviceLabels: Record<string, string> = {
  "Để xe": "Parking", "Có mái che": "Covered courts", "Vệ sinh nhà tắm": "Restrooms / showers",
  "Thuê vợt": "Paddle rental", "Thuê bóng": "Ball rental", "Ăn uống": "Food & drinks",
  "Tủ khoá đồ": "Lockers", "Thuê quần áo": "Sportswear rental", "Nhặt bóng": "Ball collection service",
  "Máy bắn bóng": "Ball machine", "Khác": "Other reported services", "Phòng xông hơi": "Sauna",
  "Chỗ chơi trẻ em": "Children’s play area", "Giặt là tại chỗ": "On-site laundry",
};
export function timeMinutes(value: string) {
  const match = /^(\d{2}):(\d{2})$/.exec(value);
  if (!match) return null;
  const hour = Number(match[1]), minute = Number(match[2]);
  return hour < 24 && minute < 60 ? hour * 60 + minute : null;
}
function intervals(hours: string[], day: number) {
  return hours.filter(line => normalizeSearch(line).startsWith(dayNames[day] + " ")).flatMap(line => {
    const pairs = [...line.matchAll(/(\d{1,2}):(\d{2})\s*[-–—]\s*(\d{1,2}):(\d{2})/g)];
    return pairs.flatMap(pair => {
      const start = Number(pair[1]) * 60 + Number(pair[2]), end = Number(pair[3]) * 60 + Number(pair[4]);
      if (Number(pair[1]) >= 24 || Number(pair[3]) > 24 || Number(pair[2]) >= 60 || Number(pair[4]) >= 60 || (Number(pair[3]) === 24 && Number(pair[4]) !== 0) || start === end) return [];
      return [[start, end < start ? end + 1440 : end] as const];
    });
  });
}
// Schedules are source-reported in Vietnam local time. Identical endpoints
// (e.g. 00:00–00:00) are ambiguous and must not be treated as 24-hour opening.
export function matchesHours(hours: string[], day: number, time: string) {
  const minutes = timeMinutes(time);
  if (minutes == null || day < 0 || day > 6) return false;
  return intervals(hours, day).some(([start,end]) => minutes >= start && minutes < end) ||
    intervals(hours, (day + 6) % 7).some(([start,end]) => minutes + 1440 >= start && minutes + 1440 < end);
}
export type VenueFilters = { search: string; area: string; coordinates: string; courtCount: string; services: string[]; day: number; time: string };
export function filterVenues(venues: CourtVenue[], filters: VenueFilters) {
  const query = normalizeSearch(filters.search);
  return venues.filter(venue => {
    const count = venue.courts;
    const courtsMatch = filters.courtCount === "all" || (filters.courtCount === "unknown" ? count == null : count != null && (
      filters.courtCount === "1-4" ? count >= 1 && count <= 4 : filters.courtCount === "5-8" ? count >= 5 && count <= 8 : filters.courtCount === "9+" && count >= 9));
    return (!query || normalizeSearch(venue.name + " " + venue.address + " " + venue.area).includes(query)) &&
      (!filters.area || venue.area === filters.area) && courtsMatch &&
      (filters.coordinates === "all" || (filters.coordinates === "mapped" ? venue.latitude != null : venue.latitude == null)) &&
      filters.services.every(service => venue.services.includes(service)) &&
      (!filters.time || matchesHours(venue.openingHours ?? [], filters.day, filters.time));
  });
}
