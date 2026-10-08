export function formatPhysicalSize(bytes: number): string {
  const gigabytes = bytes / 1_000_000_000;
  const decimalPlaces = gigabytes < 0.01 ? 4 : gigabytes < 1 ? 3 : 2;
  return `${gigabytes.toFixed(decimalPlaces)} GB`;
}
