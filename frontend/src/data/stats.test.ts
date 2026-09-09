import { describe, expect, it } from "vitest";
import * as stats from "./stats";
import { STATS_ACTS } from "@/test/fixtures";

describe("stats (port of src/dashboard/stats.py)", () => {
  it("daysInYear", () => {
    expect(stats.daysInYear(2021)).toBe(365);
    expect(stats.daysInYear(2020)).toBe(366);
    expect(stats.daysInYear(1900)).toBe(365);
  });

  it("totals", () => {
    const t = stats.totals(STATS_ACTS, 2021);
    expect(t.runs).toBe(4);
    expect(t.days).toBe(365);
    expect(t.activeDays).toBe(3);
    expect(t.distanceKm).toBe(23);
    expect(t.durationSec).toBe(7120);
    expect(t.calories).toBe(1400);
    expect(t.avgPaceSecPerKm).toBe(Math.round(7120 / 23));
    expect(t.avgCadence).toBeCloseTo((170 + 180 + 170) / 3);
    expect(t.avgDistanceKm).toBe(5.75);
  });

  it("totals of nothing", () => {
    const t = stats.totals([], 2021);
    expect(t.runs).toBe(0);
    expect(t.avgPaceSecPerKm).toBeNull();
    expect(t.avgDistanceKm).toBe(0);
  });

  it("monthly has twelve rows and best pace needs 5 km", () => {
    const rows = stats.monthly(STATS_ACTS);
    expect(rows).toHaveLength(12);
    expect(rows[0].runs).toBe(3);
    expect(rows[0].distanceKm).toBe(17);
    expect(rows[0].bestPaceActivityId).toBe("a");
    expect(rows[1].bestPaceActivityId).toBe("d");
    expect(rows[2].runs).toBe(0);
    expect(rows[2].avgPaceSecPerKm).toBeNull();
  });

  it("weekIndex and weekly (2021-01-01 is a Friday)", () => {
    expect(stats.weekIndex("2021-01-01", 2021)).toBe(0);
    expect(stats.weekIndex("2021-01-03", 2021)).toBe(0);
    expect(stats.weekIndex("2021-01-04", 2021)).toBe(1);
    const weeks = stats.weekly(STATS_ACTS, 2021);
    expect(weeks).toHaveLength(53);
    expect(weeks[0].weekStart).toBe("2021-01-01");
    expect(weeks[1].weekStart).toBe("2021-01-04");
    expect(weeks[0].runs).toBe(3);
    expect(weeks[0].distanceKm).toBe(17);
    expect(weeks[0].longestKm).toBe(10);
    expect(weeks[0].activityIds).toEqual(["a", "b", "c"]);
    expect(weeks[0].avgPaceSecPerKm).toBe(Math.round(5500 / 17));
    expect(weeks[1].runs).toBe(0);
    expect(weeks[52].weekStart).toBe("2021-12-27");
  });

  it("dailyDistance covers the year and sums same-day runs", () => {
    const daily = stats.dailyDistance(STATS_ACTS, 2021);
    expect(daily).toHaveLength(365);
    expect(daily[0]).toEqual({ date: "2021-01-01", distanceKm: 5, activityId: "a" });
    expect(daily[1].distanceKm).toBe(12);
    expect(daily[1].activityId).toBe("b");
    expect(daily[2]).toEqual({ date: "2021-01-03", distanceKm: 0, activityId: null });
    expect(daily[364].date).toBe("2021-12-31");
  });

  it("trainingLoad is an exponential average", () => {
    const daily = [0, 1, 2].map((i) => ({ date: `d${i}`, distanceKm: 42, activityId: null }));
    const load = stats.trainingLoad(daily);
    expect(load[0]).toEqual({ date: "d0", ctl: 1, atl: 6, tsb: -5 });
    expect(load[1].ctl).toBe(Math.round((1 + (42 - 1) / 42) * 100) / 100);
  });

  it("personalBests", () => {
    const pb = stats.personalBests(STATS_ACTS);
    expect(pb.longest?.id).toBe("b");
    expect(pb.fastest?.id).toBe("d");
    expect(pb.longestTime?.id).toBe("b");
    expect(pb.mostCalories?.id).toBe("b");
    expect(pb.topCadence?.id).toBe("b");
    expect(stats.personalBests([])).toEqual({
      longest: null,
      fastest: null,
      longestTime: null,
      mostCalories: null,
      topCadence: null,
    });
  });

  it("yearSummary bundles everything", () => {
    const y = stats.yearSummary(STATS_ACTS, 2021);
    expect(y.year).toBe(2021);
    expect(y.totals.runs).toBe(4);
    expect(y.trainingLoad).toHaveLength(365);
    expect(y.weekly).toHaveLength(53);
  });
});
