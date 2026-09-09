import { describe, expect, it } from "vitest";
import { dailyDeltas, dailySeries, lastOfMonth, monthlyChange, monthlyUpDown, weekdayDelta, weeklyKmVsChange, weightKpis } from "./model";
import { dailyDistance, weekly } from "@/data/stats";
import { act } from "@/test/fixtures";

const W = [
  { date: "2021-01-01", weightLbs: 150, weightKg: 68.04, bmi: 23.5 },
  { date: "2021-01-02", weightLbs: 149, weightKg: 67.59, bmi: 23.4 },
  { date: "2021-01-04", weightLbs: 151, weightKg: 68.49, bmi: 23.7 }, // gap: no delta
  { date: "2021-01-05", weightLbs: 151, weightKg: 68.49, bmi: 23.7 },
  { date: "2021-02-01", weightLbs: 148, weightKg: 67.13, bmi: 23.2 },
];

describe("weight model", () => {
  it("kpis", () => {
    const k = weightKpis(W, 170, 365)!;
    expect(k.first.date).toBe("2021-01-01");
    expect(k.last.weightLbs).toBe(148);
    expect(k.min.weightLbs).toBe(148);
    expect(k.max.weightLbs).toBe(151);
    expect(k.loss).toBe(2);
    expect(k.lossPct).toBeCloseTo(1.333, 2);
    expect(k.bmiEnd).toBeCloseTo(23.23, 1);
    expect(k.missing).toBe(360);
    expect(weightKpis([], 170, 365)).toBeNull();
  });

  it("daily series with 7-day average", () => {
    const { series, ma7 } = dailySeries(W, ["2021-01-01", "2021-01-02", "2021-01-03"]);
    expect(series).toEqual([150, 149, null]);
    expect(ma7[2]).toBe(149.5);
  });

  it("month ends and monthly change", () => {
    const ends = lastOfMonth(W);
    expect(ends[0]).toBe(151);
    expect(ends[1]).toBe(148);
    expect(ends[2]).toBeNull();
    const ch = monthlyChange(W);
    expect(ch[0]).toBe(1); // 151 - first record 150
    expect(ch[1]).toBe(-3);
    expect(ch[2]).toBeNull();
  });

  it("deltas only between consecutive days", () => {
    const d = dailyDeltas(W);
    expect(d).toEqual([
      { date: "2021-01-02", value: -1 },
      { date: "2021-01-05", value: 0 },
    ]);
    const wd = weekdayDelta(d);
    expect(wd[5]).toBe(-1); // Saturday 2 Jan
    expect(wd[1]).toBe(0); // Tuesday 5 Jan
    expect(wd[0]).toBeNull();
    const ud = monthlyUpDown(d, W);
    expect(ud).toEqual([{ month: 1, up: 0, down: 1, same: 1, net: -1, end: 151 }]);
  });

  it("weekly km vs weight change needs two weigh-ins and a run", () => {
    const acts = [act({ id: "a", date: "2021-01-04", distanceKm: 5 })];
    const pts = weeklyKmVsChange(weekly(acts, 2021), W, dailyDistance(acts, 2021), 2021);
    expect(pts).toEqual([{ x: 5, y: 0, weekStart: "2021-01-04" }]);
  });
});
