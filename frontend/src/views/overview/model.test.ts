import { describe, expect, it } from "vitest";
import { heatLevel, personalBestRows, rolling7, weatherStrip } from "./model";
import { act, STATS_ACTS } from "@/test/fixtures";
import { personalBests, totals } from "@/data/stats";

describe("overview model", () => {
  it("heatLevel bands", () => {
    expect([0, 3.9, 4, 6, 7, 9, 20].map(heatLevel)).toEqual([0, 1, 2, 3, 4, 5, 5]);
  });

  it("personalBestRows uses the average run as the bar and clamps to 1", () => {
    const rows = personalBestRows(personalBests(STATS_ACTS), totals(STATS_ACTS, 2021));
    expect(rows.map((r) => r.label)).toEqual(["Longest", "Fastest", "Longest time", "Most kcal", "Top cadence"]);
    const longest = rows[0];
    expect(longest.activity.id).toBe("b");
    expect(longest.value).toBe("10.00 km");
    expect(longest.ratio).toBeCloseTo(5.75 / 10);
    rows.forEach((r) => expect(r.ratio).toBeLessThanOrEqual(1));
  });

  it("weatherStrip counts rainy, severe and finds the best temperature band", () => {
    const acts = [
      ...[20, 21, 22, 23, 23.5].map((t, i) => act({ id: `w${i}`, date: `2021-03-0${i + 1}`, avgTempC: t, paceSecPerKm: 300 })),
      ...[28, 29, 30, 31, 31.5].map((t, i) => act({ id: `h${i}`, date: `2021-06-0${i + 1}`, avgTempC: t, paceSecPerKm: 340 })),
      act({ id: "x", date: "2021-06-06", avgTempC: 18, paceSecPerKm: 250 }), // fast but alone in its band
      act({ id: "s", date: "2021-07-01", avgTempC: null, weather: { description: "Rain", tempC: 25, humidityPct: 90, windKmh: 5 }, warnings: ["STRONG WIND SIGNAL NO. 3"] }),
    ];
    const weather = [{ date: "2021-07-01", rainfallMm: 12 }, { date: "2021-03-01", rainfallMm: 0 }].map((w) => ({
      date: w.date,
      maxTempC: null,
      avgTempC: null,
      minTempC: null,
      humidityPct: null,
      rainfallMm: w.rainfallMm,
      windKmh: null,
      sunrise: null,
      sunset: null,
    }));
    const s = weatherStrip(acts, weather);
    expect(s.rainyRuns).toBe(1);
    expect(s.severeRuns).toBe(1);
    expect(s.minTemp).toBe(18);
    expect(s.maxTemp).toBe(31.5);
    expect(s.bestBand).toEqual({ from: 20, to: 24, pace: 300, runs: 5 });
  });

  it("rolling7 averages the trailing week", () => {
    const daily = [1, 2, 3, 4, 5, 6, 7, 8].map((km, i) => ({ date: `d${i}`, distanceKm: km, activityId: null }));
    const r = rolling7(daily);
    expect(r[0]).toBe(1);
    expect(r[6]).toBe(4);
    expect(r[7]).toBe(5);
  });
});
