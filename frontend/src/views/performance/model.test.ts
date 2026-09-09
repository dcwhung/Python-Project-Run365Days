import { describe, expect, it } from "vitest";
import { distanceHistogram, monthlyTable, paceSeries, performanceKpis, timeOfDayPace, weekdayPace } from "./model";
import { STATS_ACTS } from "@/test/fixtures";
import { dailyDistance } from "@/data/stats";

describe("performance model", () => {
  it("kpis", () => {
    const k = performanceKpis(STATS_ACTS);
    expect(k.runs).toBe(4);
    expect(k.avgPace).toBeCloseTo(7120 / 23);
    expect(k.fastest?.id).toBe("d");
    expect(k.longest?.id).toBe("b");
    expect(k.avgKm).toBe(5.75);
    expect(k.maxCadence).toBe(180);
    expect(k.longRuns).toBe(1);
    expect(k.midRuns).toBe(1);
  });

  it("paceSeries picks the first run per day and builds a 30-day trend", () => {
    const daily = dailyDistance(STATS_ACTS, 2021);
    const { points, trend, dayAct } = paceSeries(STATS_ACTS, daily);
    expect(points[0]).toBe(5);
    expect(points[1]).toBe(6); // run b (first that day), 360 s/km
    expect(points[2]).toBeNull();
    expect(dayAct[1]?.id).toBe("b");
    expect(trend[1]).toBeCloseTo(5100 / 60 / 15, 2); // a + b only (c is not the day's first run)
    expect(trend[3]).toBe(trend[1]);
  });

  it("histogram, weekday and time-of-day groups", () => {
    const h = distanceHistogram(STATS_ACTS);
    expect(h.map((b) => b.count)).toEqual([1, 0, 1, 1, 0, 0, 1]);
    expect(weekdayPace(STATS_ACTS).reduce((s, w) => s + w.runs, 0)).toBe(4);
    const tod = timeOfDayPace(STATS_ACTS);
    expect(tod[0].runs).toBe(4); // fixtures start at 07:00
    expect(tod[1].pace).toBeNull();
  });

  it("monthly table skips empty months and computes best pace", () => {
    const rows = monthlyTable(STATS_ACTS);
    expect(rows.map((r) => r.month)).toEqual([1, 2]);
    expect(rows[0].runs).toBe(3);
    expect(rows[0].best).toBe(300);
    expect(rows[0].kcal).toBe(1100);
  });
});
