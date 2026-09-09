import { describe, expect, it } from "vitest";
import { byMonth, byWeekday, paceOf, timeOfDay, weeksWithActivities } from "./analytics";
import { act } from "@/test/fixtures";
import { weekly } from "@/data/stats";

const ACTS = [
  act({ id: "a", date: "2021-01-04", distanceKm: 5, durationSec: 1500 }), // Monday
  act({ id: "b", date: "2021-01-05", distanceKm: 10, durationSec: 3600 }),
  act({ id: "c", date: "2021-02-07", distanceKm: 2, durationSec: 400 }), // Sunday
];

describe("analytics helpers", () => {
  it("paceOf is distance-weighted and null for nothing", () => {
    expect(paceOf(ACTS)).toBeCloseTo(5500 / 17);
    expect(paceOf([])).toBeNull();
  });
  it("timeOfDay buckets", () => {
    expect(["04:59", "05:00", "08:00", "12:00", "17:00", "21:00"].map(timeOfDay)).toEqual([4, 0, 1, 2, 3, 4]);
  });
  it("groups by weekday and month", () => {
    const wd = byWeekday(ACTS);
    expect(wd[0].map((a) => a.id)).toEqual(["a"]);
    expect(wd[6].map((a) => a.id)).toEqual(["c"]);
    expect(byMonth(ACTS)[1].map((a) => a.id)).toEqual(["c"]);
  });
  it("attaches activities to weeks", () => {
    const weeks = weeksWithActivities(weekly(ACTS, 2021), ACTS);
    expect(weeks[1].activities.map((a) => a.id)).toEqual(["a", "b"]);
    expect(weeks[0].activities).toEqual([]);
  });
});
