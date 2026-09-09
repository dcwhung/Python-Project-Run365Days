import { describe, expect, it } from "vitest";
import { mapActivity, mapMeta, mapTrack, mapWarning, mapWeather, mapWeight } from "./mappers";
import { STATIC_ACTIVITY } from "@/test/fixtures";

describe("static mappers", () => {
  it("maps activity keys and nested weather", () => {
    const a = mapActivity(STATIC_ACTIVITY);
    expect(a).toMatchObject({
      id: "a",
      startTime: "12:00",
      dayOfYear: 8,
      paceSecPerKm: 360,
      avgTempC: 18.4,
      elevationMaxM: 339,
      hasGps: true,
      warnings: ["RED FIRE DANGER WARNING"],
    });
    expect(a.weather).toEqual({ description: "Clear weather", tempC: 19, humidityPct: 65, windKmh: 10 });
  });

  it("keeps null weather and missing warnings safe", () => {
    const a = mapActivity({ ...STATIC_ACTIVITY, weather: null, warnings: undefined as unknown as string[] });
    expect(a.weather).toBeNull();
    expect(a.warnings).toEqual([]);
  });

  it("maps track rows by column name", () => {
    const pts = mapTrack({
      columns: ["sec", "lat", "lon", "elevation_m", "distance_m", "speed_mps", "cadence", "temp_c"],
      rows: [
        [0, 22.3, 114.2, 330, 0, 2.7, 83, 18],
        [6, null, null, 331, 16, null, 84, null],
      ],
    });
    expect(pts[0]).toEqual({ sec: 0, lat: 22.3, lon: 114.2, elevationM: 330, distanceM: 0, speedMps: 2.7, cadence: 83, tempC: 18 });
    expect(pts[1].lat).toBeNull();
    expect(pts[1].tempC).toBeNull();
  });

  it("rejects unknown track columns", () => {
    expect(() => mapTrack({ columns: ["sec", "bogus"], rows: [] })).toThrow(/bogus/);
  });

  it("maps weight, weather, warning and meta", () => {
    expect(mapWeight({ date: "2021-01-08", weight_lbs: 154.8, weight_kg: 70.28, bmi: 24.3 })).toEqual({
      date: "2021-01-08",
      weightLbs: 154.8,
      weightKg: 70.28,
      bmi: 24.3,
    });
    expect(
      mapWeather({
        date: "2021-01-08",
        max_temp_c: 21,
        avg_temp_c: 18.5,
        min_temp_c: 15,
        humidity_pct: 70,
        rainfall_mm: null,
        wind_kmh: 12,
        sunrise: "07:03",
        sunset: "17:55",
      }),
    ).toMatchObject({ maxTempC: 21, rainfallMm: null, sunrise: "07:03" });
    expect(
      mapWarning({ date: "2021-01-08", type: "Fire Danger", signal: "RED FIRE DANGER WARNING", start_time: null, end_time: null }),
    ).toMatchObject({ signal: "RED FIRE DANGER WARNING", startTime: null });
    expect(mapMeta({ year: 2021, generated_at: "x" })).toEqual({ year: 2021, generatedAt: "x" });
  });
});
