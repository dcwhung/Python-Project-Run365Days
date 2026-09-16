import { describe, expect, it } from "vitest";
import { downsample } from "./downsample";

/**
 * Sample positions the Python samplers pick, read straight off
 * `run365days.api.service._even_positions` (which is itself held against
 * `run365days.dashboard.builder.downsample`).
 *
 * Every row here is a case where Python's round-half-to-even and JavaScript's
 * round-half-up land on different indices, so the table only stays green while
 * this module rounds the Python way. CUI-0021 measured 10,189 such pairs among
 * the 45,117 `(track_length, points)` combinations the real export can produce.
 */
const PYTHON_POSITIONS: ReadonlyArray<readonly [number, number, readonly number[]]> = [
  [6, 3, [0, 2, 5]],
  [10, 3, [0, 4, 9]],
  [11, 5, [0, 2, 5, 8, 10]],
  [13, 9, [0, 2, 3, 4, 6, 8, 9, 10, 12]],
  [14, 3, [0, 6, 13]],
  [250, 3, [0, 124, 249]], // the case CUI-0021 names: Python 124, Math.round 125
  [347, 5, [0, 86, 173, 260, 346]],
  [1250, 7, [0, 208, 416, 624, 833, 1041, 1249]],
];

/** What `Math.round` would have picked, so the table above cannot quietly stop testing anything. */
function halfUpPositions(length: number, points: number): number[] {
  const step = (length - 1) / (points - 1);
  return Array.from({ length: points }, (_, i) => Math.round(i * step));
}

describe("downsample cross-mode parity", () => {
  it.each(PYTHON_POSITIONS)(
    "picks the same indices as the Python sampler for length=%i points=%i",
    (length, points, expected) => {
      const rows = Array.from({ length }, (_, i) => i);
      expect(downsample(rows, points)).toEqual([...expected]);
    },
  );

  it.each(PYTHON_POSITIONS)(
    "length=%i points=%i really is a case Math.round gets wrong",
    (length, points, expected) => {
      // Without this the table above would still pass if someone reverted the
      // rounding, as long as they also picked non-divergent lengths.
      expect(halfUpPositions(length, points)).not.toEqual([...expected]);
    },
  );

  it("still includes the first and last row and stays strictly increasing", () => {
    for (const [length, points] of PYTHON_POSITIONS) {
      const picked = downsample(
        Array.from({ length }, (_, i) => i),
        points,
      );
      expect(picked).toHaveLength(points);
      expect(picked[0]).toBe(0);
      expect(picked[points - 1]).toBe(length - 1);
      expect(picked.every((v, i) => i === 0 || v > picked[i - 1])).toBe(true);
    }
  });

  it("leaves every track the export can hold untouched", () => {
    // Today's guard against any visible change: ActivityView asks for 600 and
    // the export stores at most 600, so every stored track is returned whole
    // and the rounding above is never reached in production.
    for (const length of [1, 2, 250, 599, 600]) {
      const rows = Array.from({ length }, (_, i) => i);
      expect(downsample(rows, 600)).toEqual(rows);
    }
  });
});
