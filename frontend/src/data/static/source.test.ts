import { describe, expect, it, vi } from "vitest";
import { MAX_TRACK_POINTS, MIN_TRACK_POINTS, createStaticSource } from "./source";
import { STATIC_ACTIVITY } from "@/test/fixtures";
// The schema the API publishes, generated from src/api/schema.py by `run365-schema`.
import SDL from "../../../schema.graphql?raw";

const FILES: Record<string, unknown> = {
  "/data/meta.json": { year: 2021, generated_at: "2026-01-01T00:00:00", counts: {} },
  "/data/activities.json": [
    STATIC_ACTIVITY,
    {
      ...STATIC_ACTIVITY,
      id: "b",
      date: "2021-01-09",
      distance_km: 3,
      has_gps: false,
      weather: null,
      warnings: [],
    },
  ],
  "/data/tracks/a.json": {
    columns: ["sec", "lat", "lon", "elevation_m", "distance_m", "speed_mps", "cadence", "temp_c"],
    rows: [0, 1, 2, 3, 4].map((i) => [i * 6, 22.3, 114.2, 330 + i, i * 16, 2.7, 83, 18]),
  },
  "/data/weight.json": [
    { date: "2021-01-08", weight_lbs: 154.8, weight_kg: 70.28, bmi: 24.3 },
    { date: "2021-01-09", weight_lbs: 154.2, weight_kg: 70.01, bmi: 24.2 },
  ],
  "/data/weather.json": [
    {
      date: "2021-01-08",
      max_temp_c: 21,
      avg_temp_c: 18.5,
      min_temp_c: 15,
      humidity_pct: 70,
      rainfall_mm: null,
      wind_kmh: 12,
      sunrise: "07:03",
      sunset: "17:55",
    },
  ],
  "/data/warnings.json": [
    {
      date: "2021-01-08",
      type: "Fire Danger",
      signal: "RED FIRE DANGER WARNING",
      start_time: null,
      end_time: null,
    },
    {
      date: "2021-01-09",
      type: "Cold",
      signal: "COLD WEATHER WARNING",
      start_time: null,
      end_time: null,
    },
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

  // ── S-011: one call means one thing in both data modes ──────────────────
  it("treats an unspecified point count as 'every stored sample'", async () => {
    const { src } = source();
    expect(await src.track("a")).toHaveLength(5);
    expect(await src.track("a", undefined)).toHaveLength(5);
  });

  it("rejects points: 0 the way the API does, instead of reading it as 'no limit'", async () => {
    const { src } = source();
    await expect(src.track("a", 0)).rejects.toThrow(
      `points must be between ${MIN_TRACK_POINTS} and ${MAX_TRACK_POINTS}, got 0`,
    );
  });

  it.each([-1, 1.5, Number.NaN, MAX_TRACK_POINTS + 1])(
    "rejects an out-of-range point count (%s)",
    async (points) => {
      const { src } = source();
      await expect(src.track("a", points)).rejects.toBeInstanceOf(RangeError);
    },
  );

  it("returns the final sample alone for points: 1, matching the API", async () => {
    const { src } = source();
    expect((await src.track("a", 1)).map((p) => p.sec)).toEqual([24]);
  });

  it("keeps its track-point bounds in step with the published schema", () => {
    const bounds = SDL.match(/Samples to return: (\d+) to (\d+)\./);
    expect(bounds, "schema.graphql no longer states the track-point bounds").not.toBeNull();
    expect([Number(bounds![1]), Number(bounds![2])]).toEqual([MIN_TRACK_POINTS, MAX_TRACK_POINTS]);
  });

  it("applies date ranges to weight, weather and warnings", async () => {
    const { src } = source();
    expect((await src.weight({ fromDate: "2021-01-09" })).map((w) => w.date)).toEqual([
      "2021-01-09",
    ]);
    expect(await src.weather({ toDate: "2021-01-07" })).toEqual([]);
    expect((await src.warnings({ toDate: "2021-01-08" })).map((w) => w.signal)).toEqual([
      "RED FIRE DANGER WARNING",
    ]);
  });

  it("fetches each file once", async () => {
    const { src, fetcher } = source();
    await src.activities();
    await src.activities({ minKm: 1 });
    await src.activity("a");
    expect(fetcher).toHaveBeenCalledTimes(1);
  });
});
