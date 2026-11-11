import type {MapData, Source} from './types';
export type SocialSource = Exclude<Source, 'pickleballplus'>;
export function compareProvinces(data: MapData, sources: SocialSource[]) {
  const selected = [...new Set(sources)];
  const listings = data.provinces.reduce((sum, row) => sum + row.pickleballplus, 0);
  const mentions = data.provinces.reduce((sum, row) => sum + selected.reduce((n, source) => n + row[source], 0), 0);
  return data.provinces.map(row => {
    const social = selected.reduce((sum, source) => sum + row[source], 0);
    const listingShare = listings > 0 ? row.pickleballplus / listings * 100 : null;
    const socialShare = mentions > 0 ? social / mentions * 100 : null;
    return {...row, social, listingShare, socialShare, difference: listingShare !== null && socialShare !== null ? socialShare - listingShare : null};
  });
}
