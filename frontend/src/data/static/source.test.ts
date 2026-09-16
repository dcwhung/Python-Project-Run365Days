import { describe, expect, it, vi } from "vitest";
import { createStaticSource } from "./source";
import { STATIC_ACTIVITY } from "@/test/fixtures";

const FILES: Record<string, unknown> = {
  "/data/meta.json": { year: 2021, generated_at: "2026-01-01T00:00:00", counts: {} },
  "/data/activities.json": [
    STATIC_ACTIVITY,
    { ...STATIC_ACTIVITY, id: "b", date: "2021-01-09", distance_km: 3, has_gps: false, weather: null, warnings: [] },
  ],
  "/data/tracks/a.json": {
    columns: ["sec", "lat", "lon", "elevation_m", "distance_m", "speed_mps", "cadence", "temp_c"],
    rows: [0, 1, 2, 3, 4].map((i) => [i * 6, 22.3, 114.2, 330 + i, i * 16, 2.7, 83, 18]),
  },
  "/data/weight.json": [
    { date: "2021-01-08", weight_lbs: 154.8, weight_kg: 70.28, bmi: 24.3 },
    { date: "2021-01-09", weight_lbs: 154.2, weight_kg: 70.01, bmi: 24.2 },
  ],
  "/data/weather.json": [{ date: "2021-01-08", max_temp_c: 21, avg_temp_c: 18.5, min_temp_c: 15, humidity_pct: 70, rainfall_mm: null, wind_kmh: 12, sunrise: "07:03", sunset: "17:55" }],
  "/data/warnings.json": [
    { date: "2021-01-08", type: "Fire Danger", signal: "RED FIRE DANGER WARNING", start_time: null, end_time: null },
    { date: "2021-01-09", type: "Cold", signal: "COLD WEATHER WARNING", start_time: null, end_time: null },
  ],
};

function source() {
  const fetcher = vi.fn(async (url: string) => {
    if (!(url in FILES)) throw new Error(`404 ${url}`);
    return FILES[url];
  });
  return { src: createStaticSource("/data", fetcher), fetcher };
}

describe("static source", () => {
  it("reads meta and computes the year summary in the browser", async () => {
    const { src } = source();
    expect(src.mode).toBe("static");
    expect(await src.meta()).toEqual({ year: 2021, generatedAt: "2026-01-01T00:00:00" });
    const y = await src.year();
    expect(y.totals.runs).toBe(2);
    expect(y.totals.distanceKm).toBe(8);
    expect(y.weekly).toHaveLength(53);
  });

  it("filters activities like the API", async () => {
    const { src } = source();
    expect((await src.activities()).map((a) => a.id)).toEqual(["a", "b"]);
    expect((await src.activities({ fromDate: "2021-01-09" })).map((a) => a.id)).toEqual(["b"]);
    expect((await src.activities({ toDate: "2021-01-08" })).map((a) => a.id)).toEqual(["a"]);
    expect((await src.activities({ hasGps: false })).map((a) => a.id)).toEqual(["b"]);
    expect((await src.activities({ minKm: 4 })).map((a) => a.id)).toEqual(["a"]);
    expect((await src.activity("b"))?.weather).toBeNull();
    expect(await src.activity("nope")).toBeNull();
  });

  it("downsamples tracks on request and keeps first and last", async () => {
    const { src } = source();
    expect(await src.track("a")).toHaveLength(5);
    const two = await src.track("a", 2);
    expect(two.map((p) => p.sec)).toEqual([0, 24]);
  });

  // CUI-0025. api mode's `_track_points` refuses anything outside 1..1000; this
  // side used to accept every number and read the falsy ones as "no limit".
  // `test_track_points_of_zero_is_refused_rather_than_read_as_no_limit` in
  // tests/test_api.py is the other half of the pair.
  it("refuses points: 0 the way api mode does, rather than returning the whole track", async () => {
    const { src } = source();
    // The trap CUI-0021 laid: `points != null ? downsample(...) : rows` would
    // send 0 into downsample's `limit < 2` branch and return the LAST point
    // alone -- a third behaviour, quietly wrong in a new way. Asserting the
    // length would pass against that; asserting the throw is what does not.
    await expect(src.track("a", 0)).rejects.toThrow("points must be between 1 and 1000, got 0");
  });

  it("refuses points above the maximum api mode allows", async () => {
    const { src } = source();
    await expect(src.track("a", 1001)).rejects.toThrow("points must be between 1 and 1000, got 1001");
  });

  it("refuses a non-integer points, which downsample has no defined answer for", async () => {
    const { src } = source();
    // downsample's own docstring puts a non-integer `limit` outside its claim:
    // Python raises on range(2.5) where this side would quietly return 2 items.
    // api mode cannot express it at all -- the SDL argument is `Int!`.
    await expect(src.track("a", 2.5)).rejects.toThrow("points must be between 1 and 1000, got 2.5");
  });

  it("still returns the whole stored track when points is omitted", async () => {
    const { src } = source();
    // Pinned deliberately: `undefined` is not `0`. api mode has no such state
    // to disagree with -- its SDL argument is `Int! = 150`, NonNull with a
    // default -- and the export caps a stored track at 600 rows, so "all of it"
    // is bounded by construction on this side.
    expect(await src.track("a")).toHaveLength(5);
    expect(await src.track("a", undefined)).toHaveLength(5);
  });

  it("returns the last row alone for points: 1, as both modes do", async () => {
    const { src } = source();
    // CUI-0004's semantics, pinned here so the new bounds check cannot be
    // mistaken for a reason to reject 1 as well.
    expect((await src.track("a", 1)).map((p) => p.sec)).toEqual([24]);
  });

  it("applies date ranges to weight, weather and warnings", async () => {
    const { src } = source();
    expect((await src.weight({ fromDate: "2021-01-09" })).map((w) => w.date)).toEqual(["2021-01-09"]);
    expect(await src.weather({ toDate: "2021-01-07" })).toEqual([]);
    expect((await src.warnings({ toDate: "2021-01-08" })).map((w) => w.signal)).toEqual(["RED FIRE DANGER WARNING"]);
  });

  it("fetches each file once", async () => {
    const { src, fetcher } = source();
    await src.activities();
    await src.activities({ minKm: 1 });
    await src.activity("a");
    expect(fetcher).toHaveBeenCalledTimes(1);
  });
});
