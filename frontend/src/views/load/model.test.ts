import { describe, expect, it } from "vitest";
import { loadKpis, rollingWeeks, topWeeks, weekChange, weekdayAvgKm } from "./model";
import { STATS_ACTS } from "@/test/fixtures";
import { dailyDistance, trainingLoad, weekly } from "@/data/stats";
import { weeksWithActivities } from "@/lib/analytics";

describe("training load model", () => {
  const weeks = weeksWithActivities(weekly(STATS_ACTS, 2021), STATS_ACTS);
  const load = trainingLoad(dailyDistance(STATS_ACTS, 2021));

  it("kpis", () => {
    const k = loadKpis(load, weeks, 23, 365, 3);
    expect(k.ctl).toBe(load[364].ctl);
    expect(k.peakCtl).toBe(Math.max(...load.map((p) => p.ctl)));
    expect(k.biggest.week).toBe(0);
    expect(k.avgWeekKm).toBeCloseTo(23 / (365 / 7));
    expect(k.avgFullWeekKm).toBeNull();
  });

  it("rolling weeks, weekday km, top weeks, change", () => {
    expect(rollingWeeks([{ distanceKm: 10 }, { distanceKm: 20 }, { distanceKm: 30 }, { distanceKm: 40 }, { distanceKm: 50 }])).toEqual([10, 15, 20, 25, 35]);
    const wd = weekdayAvgKm(STATS_ACTS);
    expect(wd[4]).toBe(5); // Friday 1 Jan
    expect(wd[5]).toBe(6); // Saturday 2 Jan: (10 + 2) / 2
    expect(topWeeks(weeks, 2).map((w) => w.week)).toEqual([0, 7]); // Feb 15 is day 46 -> week 7
    expect(weekChange([{ distanceKm: 10 }, { distanceKm: 15 }], 1)).toBe(50);
    expect(weekChange([{ distanceKm: 0 }, { distanceKm: 15 }], 1)).toBeNull();
  });
});
