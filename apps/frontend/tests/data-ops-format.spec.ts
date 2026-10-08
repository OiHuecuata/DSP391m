import { test, expect } from "@playwright/test";
import { formatPhysicalSize } from "../src/features/data-ops/format";

for (const [bytes, expected] of [
  [3_884_671, "0.0039 GB"],
  [500_000_000, "0.500 GB"],
  [1_500_000_000, "1.50 GB"],
  [0, "0.0000 GB"],
  [10_000_000, "0.010 GB"],
  [1_000_000_000, "1.00 GB"],
] as const) {
  test(`physical size formats ${bytes} bytes as ${expected}`, () => {
    expect(formatPhysicalSize(bytes)).toBe(expected);
  });
}
