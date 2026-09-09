import { describe, expect, it } from "vitest";
import { DEFAULT_FILTERS, filterAndSort, matches, toCsv } from "./model";
import { act } from "@/test/fixtures";

const ACTS = [
  act({ id: "a", date: "2021-01-08", startTime: "07:00", distanceKm: 5, paceSecPerKm: 300, weather: { description: "Rain", tempC: 19, humidityPct: 80, windKmh: 10 }, warnings: ["THUNDERSTORM WARNING"] }),
  act({ id: "b", date: "2021-02-01", startTime: "18:30", distanceKm: 10, paceSecPerKm: 360, hasGps: false }),
  act({ id: "c", date: "2021-02-15", startTime: "06:10", distanceKm: 3, paceSecPerKm: null, calories: null }),
];

describe("activities model", () => {
  it("matches by month, gps, min km and free text", () => {
    expect(ACTS.filter((a) => matches(a, { ...DEFAULT_FILTERS, month: 2 })).map((a) => a.id)).toEqual(["b", "c"]);
    expect(ACTS.filter((a) => matches(a, { ...DEFAULT_FILTERS, gps: false })).map((a) => a.id)).toEqual(["b"]);
    expect(ACTS.filter((a) => matches(a, { ...DEFAULT_FILTERS, minKm: 5 })).map((a) => a.id)).toEqual(["a", "b"]);
    expect(ACTS.filter((a) => matches(a, { ...DEFAULT_FILTERS, query: "thunder" })).map((a) => a.id)).toEqual(["a"]);
    expect(ACTS.filter((a) => matches(a, { ...DEFAULT_FILTERS, query: "indoor" })).map((a) => a.id)).toEqual(["b"]);
  });

  it("sorts by key and direction, nulls last for pace", () => {
    expect(filterAndSort(ACTS, DEFAULT_FILTERS, "date", -1).map((a) => a.id)).toEqual(["c", "b", "a"]);
    expect(filterAndSort(ACTS, DEFAULT_FILTERS, "distanceKm", 1).map((a) => a.id)).toEqual(["c", "a", "b"]);
    expect(filterAndSort(ACTS, DEFAULT_FILTERS, "paceSecPerKm", 1).map((a) => a.id)).toEqual(["a", "b", "c"]);
  });

  it("exports CSV with a header and escaped fields", () => {
    const csv = toCsv([ACTS[0]]);
    const [head, row] = csv.split("\n");
    expect(head.startsWith("date,time,activity_id,km")).toBe(true);
    expect(row).toBe("2021-01-08,07:00,a,5,1500,300,170,,300,19,Rain,80,THUNDERSTORM WARNING,1");
    expect(toCsv([act({ id: "q", date: "2021-01-01", warnings: ['A "B", C'] })])).toContain('"A ""B"", C"');
  });
});
