import { describe, expect, it } from "vitest";
import { conditions, extremes, humidityPoints, temperatureBands, temperatureRange, warningTable, weatherKpis } from "./model";
import { act } from "@/test/fixtures";
import type { DailyWeather } from "@/data/types";

const wx = (description: string, tempC: number, humidityPct = 70) => ({ description, tempC, humidityPct, windKmh: 10 });
const ACTS = [
  act({ id: "a", date: "2021-01-01", startTime: "06:30", avgTempC: 12, paceSecPerKm: 300, weather: wx("Clear weather", 11) }),
  act({ id: "b", date: "2021-01-02", startTime: "08:00", avgTempC: 30, paceSecPerKm: 340, weather: wx("Rain", 29, 90), warnings: ["RED RAINSTORM WARNING SIGNAL", "THUNDERSTORM WARNING"] }),
  act({ id: "c", date: "2021-01-03", startTime: "07:00", avgTempC: null, paceSecPerKm: 320, weather: wx("Thunderstorm", 25), warnings: ["THUNDERSTORM WARNING"] }),
  act({ id: "d", date: "2021-01-04", startTime: "07:00", avgTempC: 14, paceSecPerKm: 310, weather: null }),
];
const day = (date: string, o: Partial<DailyWeather>): DailyWeather => ({ date, maxTempC: null, avgTempC: null, minTempC: null, humidityPct: null, rainfallMm: null, windKmh: null, sunrise: null, sunset: null, ...o });
const WEATHER = [day("2021-01-01", { maxTempC: 16, minTempC: 9, sunrise: "07:03" }), day("2021-01-02", { rainfallMm: 30, sunrise: "07:03" })];

describe("weather model", () => {
  it("kpis", () => {
    const k = weatherKpis(ACTS, WEATHER);
    expect(k.avgTemp).toBeCloseTo((12 + 30 + 25 + 14) / 4);
    expect(k.hottest?.id).toBe("b");
    expect(k.coldest?.id).toBe("a");
    expect(k.wetStart).toBe(2);
    expect(k.rainyDays).toBe(1);
    expect(k.severe).toBe(1);
    expect(k.beforeSunrise).toBe(1);
  });

  it("temperature range joins HKO and the run", () => {
    const r = temperatureRange([{ date: "2021-01-01", activityId: "a" }, { date: "2021-01-05", activityId: null }], ACTS, WEATHER);
    expect(r[0]).toEqual({ date: "2021-01-01", max: 16, min: 9, run: 12 });
    expect(r[1]).toEqual({ date: "2021-01-05", max: null, min: null, run: null });
  });

  it("conditions, bands, humidity", () => {
    expect(conditions(ACTS).map((c) => [c.label, c.runs])).toEqual([["Clear weather", 1], ["Rain", 1], ["Thunderstorm", 1]]);
    expect(temperatureBands(ACTS).map((b) => b.label)).toEqual(["12–16°", "24–28°", "28–32°"]);
    expect(temperatureBands(ACTS)[0].runs).toBe(2);
    expect(humidityPoints(ACTS).map((p) => p.x)).toEqual([70, 90, 70]);
  });

  it("warning table and extremes", () => {
    const t = warningTable(ACTS);
    expect(t[0]).toMatchObject({ signal: "THUNDERSTORM WARNING", name: "Thunderstorm Warning", runs: 2 });
    expect(t[1].runs).toBe(1);
    const e = extremes(ACTS);
    expect(e.hottest[0].id).toBe("b");
    expect(e.coldest[e.coldest.length - 1].id).toBe("a");
  });
});
