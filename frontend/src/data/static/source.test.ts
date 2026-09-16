import { describe, expect, it, vi } from "vitest";
import { buildSchema, type GraphQLObjectType } from "graphql";
// `?raw` rather than fs: Vite resolves the path at transform time, so this
// keeps pointing at frontend/schema.graphql no matter what directory the
// runner was started from.
import schemaSdl from "../../../schema.graphql?raw";
import { createStaticSource, MAX_TRACK_POINTS } from "./source";
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
  // CUI-0033 (a). A track longer than MAX_TRACK_POINTS, which no default
  // export writes and `run365-export --points 1250` does -- the measurement
  // that opened the ticket. Built here rather than exported, because the point
  // is what this module does with such a file, not how one comes to exist.
  // `sec` is the row's own index, so a returned `sec` reads as "this is the
  // row the sampler picked" and an even sample is distinguishable from the
  // first 1000 rows.
  "/data/tracks/long.json": {
    columns: ["sec", "lat", "lon", "elevation_m", "distance_m", "speed_mps", "cadence", "temp_c"],
    rows: Array.from({ length: 1250 }, (_, i) => [i, 22.3, 114.2, 330, i * 4, 2.7, 83, 18]),
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

  // CUI-0033 (c). The bounds check runs before the fetch, so an id that does
  // not exist cannot mask an illegal `points` -- the RangeError arrives instead
  // of the 404 the id would have earned. api mode reads the same call the other
  // way round: a null `activity` means the `track` resolver never runs, so it
  // answers `{ activity: null }` with no error.
  // `test_an_unknown_activity_swallows_an_illegal_points_that_static_mode_refuses`
  // in tests/test_api.py is the other half of this pair.
  it("refuses an illegal points before the fetch, even for an id that does not exist", async () => {
    const { src, fetcher } = source();
    await expect(src.track("no-such-activity", 0)).rejects.toThrow(
      "points must be between 1 and 1000, got 0",
    );
    // The assertion that carries the claim: asserting only the throw would
    // pass just as well against a check that ran after the request went out,
    // and "costs no request" is the property worth keeping.
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("still returns the whole stored track when points is omitted", async () => {
    const { src } = source();
    // Pinned deliberately: `undefined` is not `0`. api mode has no such state
    // to disagree with -- its SDL argument is `Int! = 150`, NonNull with a
    // default. What bounds "all of it" is MAX_TRACK_POINTS and nothing else:
    // the 600 rows a default export writes is `run365-export --points`'s
    // default value, not a property of the format, and `--points 1250` writes
    // 1250 of them (CUI-0033 (a) measured exactly that against real data).
    // Every track under the ceiling still comes back entire, which is this.
    expect(await src.track("a")).toHaveLength(5);
    expect(await src.track("a", undefined)).toHaveLength(5);
  });

  // CUI-0033 (a). The other half: the ceiling is a ceiling on the output, so
  // the same module cannot refuse `points: 1250` and then hand 1250 rows to a
  // caller that named no number. `test_an_omitted_points_is_capped_at_the
  // _ceiling_the_field_refuses_above` in tests/test_api.py is the api-mode
  // half of the pair.
  it("caps an omitted points at the ceiling it refuses above", async () => {
    const { src } = source();
    const all = await src.track("long");

    expect(all).toHaveLength(MAX_TRACK_POINTS);
    // Against the explicit ask rather than against a length alone: `rows
    // .slice(0, MAX_TRACK_POINTS)` is also 1000 rows long and would pass a
    // length assertion while returning the front of the track instead of an
    // even sample of it. The last `sec` is 1249 here and would be 999 there.
    expect(all).toEqual(await src.track("long", MAX_TRACK_POINTS));
    expect(all[all.length - 1].sec).toBe(1249);
  });

  it("returns the last row alone for points: 1, as both modes do", async () => {
    const { src } = source();
    // CUI-0004's semantics, pinned here so the new bounds check cannot be
    // mistaken for a reason to reject 1 as well.
    expect((await src.track("a", 1)).map((p) => p.sec)).toEqual([24]);
  });

  // CUI-0034. `MAX_TRACK_POINTS` is written down twice, once per language, and
  // until now only one direction was guarded. Editing the constant below turns
  // three assertions red because they spell `1000` out; editing the Python one
  // (src/api/service.py since CUI-0033 (a), quoted into the SDL text by
  // src/api/schema.py) turns `run365-schema --check` red, but regenerating the
  // SDL clears that -- and nothing downstream compares the regenerated file to
  // this side, so the two modes could enforce different ceilings with every
  // gate green.
  //
  // frontend/schema.graphql closes that direction because it sits between the
  // two: run365-schema holds it equal to the Python constant, and this holds
  // the TypeScript constant equal to it. Reading the number back out of the
  // file is the whole point -- an assertion spelling `1000` here would be a
  // fourth copy rather than a link (S-062, and S-065 on the same mistake).
  it("agrees with the ceiling the generated SDL publishes", () => {
    const sdl = buildSchema(schemaSdl);
    const track = (sdl.getType("Activity") as GraphQLObjectType).getFields().track;

    // CUI-0025 put the range into the description precisely so a client could
    // read it without tripping over it first. If that phrasing ever moves,
    // this fails loudly rather than skipping the comparison and going green
    // on a description it could no longer find the number in.
    const range = /\(1-(\d+)\)/.exec(track.description ?? "");
    expect(range, `Activity.track description states no (1-N) range: ${track.description}`).not.toBeNull();
    expect(Number(range![1])).toBe(MAX_TRACK_POINTS);
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
