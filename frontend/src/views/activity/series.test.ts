import { describe, expect, it } from "vitest";
import { buildSeries, indexAtTime, seriesRange } from "./series";
import { act } from "@/test/fixtures";
import type { TrackPoint } from "@/data/types";

const pt = (o: Partial<TrackPoint> & { sec: number }): TrackPoint => ({
  lat: 22.3,
  lon: 114.2,
  elevationM: 10,
  distanceM: o.sec * 3,
  speedMps: 3,
  cadence: 85,
  tempC: 20,
  ...o,
});

describe("buildSeries", () => {
  it("derives pace, cadence, distance and gps flag", () => {
    const track = [pt({ sec: 0 }), pt({ sec: 10, speedMps: 2.5 }), pt({ sec: 20, cadence: 40, tempC: null })];
    const s = buildSeries(act({ id: "a", date: "2021-01-01", durationSec: 20 }), track);
    expect(s.n).toBe(3);
    expect(s.hasGps).toBe(true);
    expect(s.t).toEqual([0, 10, 20]);
    expect(s.dist).toEqual([0, 30, 60]);
    expect(s.cad).toEqual([170, 170, NaN]);
    expect(s.temp[2]).toBeNaN();
    expect(s.pace[1]).toBeCloseTo((1000 / 3 / 60 + 1000 / 2.5 / 60 + 1000 / 3 / 60) / 3);
    expect(s.totalSec).toBe(20);
  });

  it("fills missing coordinates from neighbours and flags indoor runs", () => {
    const track = [pt({ sec: 0, lat: null, lon: null }), pt({ sec: 5, lat: 1, lon: 2 }), pt({ sec: 10, lat: null, lon: null })];
    const s = buildSeries(act({ id: "a", date: "2021-01-01" }), track);
    expect(s.latLon).toEqual([[1, 2], [1, 2], [1, 2]]);
    const indoor = buildSeries(act({ id: "b", date: "2021-01-02", hasGps: false }), [pt({ sec: 0, lat: null, lon: null })]);
    expect(indoor.hasGps).toBe(false);
    expect(indoor.latLon).toEqual([null]);
  });

  it("carries cumulative distance when a point lacks it", () => {
    const track = [pt({ sec: 0, distanceM: 0 }), pt({ sec: 5, distanceM: null }), pt({ sec: 10, distanceM: 40 })];
    expect(buildSeries(act({ id: "a", date: "2021-01-01" }), track).dist).toEqual([0, 0, 40]);
  });
});

describe("helpers", () => {
  it("indexAtTime clamps to the last sample", () => {
    expect(indexAtTime([0, 10, 20], 12)).toBe(2);
    expect(indexAtTime([0, 10, 20], 99)).toBe(2);
    expect(indexAtTime([0, 10, 20], -1)).toBe(0);
  });

  it("seriesRange pads and clamps pace", () => {
    const spec = { key: "pace", title: "", color: "", area: false, invert: true, pad: 0.3, format: String, unit: "" } as const;
    expect(seriesRange([2, 12], spec)).toEqual({ lo: 3, hi: 9 });
    const ele = { ...spec, key: "ele", pad: 4 } as const;
    expect(seriesRange([10, 20], ele)).toEqual({ lo: 6, hi: 24 });
    expect(seriesRange([NaN], ele)).toEqual({ lo: 0, hi: 1 });
  });
});
