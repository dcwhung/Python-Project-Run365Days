import type { Activity, DailyWeather, DataSource, WeightEntry, YearSummary } from "@/data/types";
import { yearSummary } from "@/data/stats";
import type { StaticActivity } from "@/data/static/mappers";

export function act(overrides: Partial<Activity> & { id: string; date: string }): Activity {
  return {
    startTime: "07:00",
    dayOfYear: 1,
    distanceKm: 5,
    durationSec: 1500,
    paceSecPerKm: 300,
    calories: 300,
    avgCadence: 170,
    avgTempC: null,
    elevationMinM: null,
    elevationMaxM: null,
    ascentM: null,
    hasGps: true,
    numPoints: 0,
    weather: null,
    warnings: [],
    ...overrides,
  };
}

/** Same four runs as tests/test_dashboard_stats.py so both ports pin identical numbers. */
export const STATS_ACTS: Activity[] = [
  act({ id: "a", date: "2021-01-01", distanceKm: 5, durationSec: 1500 }),
  act({ id: "b", date: "2021-01-02", distanceKm: 10, durationSec: 3600, paceSecPerKm: 360, calories: 800, avgCadence: 180 }),
  act({ id: "c", date: "2021-01-02", distanceKm: 2, durationSec: 400, paceSecPerKm: 200, calories: null, avgCadence: null }),
  act({ id: "d", date: "2021-02-15", distanceKm: 6, durationSec: 1620, paceSecPerKm: 270 }),
];

export const STATIC_ACTIVITY: StaticActivity = {
  id: "a",
  date: "2021-01-08",
  start_time: "12:00",
  day_of_year: 8,
  distance_km: 5,
  duration_sec: 1800,
  pace_sec_per_km: 360,
  calories: 300,
  avg_cadence: 166,
  avg_temp_c: 18.4,
  elevation_min_m: 330,
  elevation_max_m: 339,
  ascent_m: 9,
  has_gps: true,
  num_points: 10,
  weather: { description: "Clear weather", temp_c: 19, humidity_pct: 65, wind_kmh: 10 },
  warnings: ["RED FIRE DANGER WARNING"],
};

export const WEIGHT: WeightEntry[] = [
  { date: "2021-01-01", weightLbs: 154.8, weightKg: 70.21, bmi: 24.3 },
  { date: "2021-01-02", weightLbs: 154.2, weightKg: 69.94, bmi: 24.2 },
  { date: "2021-02-15", weightLbs: 150.0, weightKg: 68.04, bmi: 23.5 },
];

export const WEATHER: DailyWeather[] = [
  { date: "2021-01-01", maxTempC: 15, avgTempC: 12, minTempC: 9, humidityPct: 60, rainfallMm: 0, windKmh: 12, sunrise: "07:03", sunset: "17:52" },
  { date: "2021-01-02", maxTempC: 18, avgTempC: 14, minTempC: 10, humidityPct: 70, rainfallMm: 5, windKmh: 10, sunrise: "07:03", sunset: "17:53" },
];

export function fakeSource(activities: Activity[] = STATS_ACTS, year = 2021): DataSource {
  const summary: YearSummary = yearSummary(activities, year);
  return {
    mode: "static",
    meta: async () => ({ year, generatedAt: "2026-01-01T00:00:00" }),
    year: async () => summary,
    activities: async () => activities,
    activity: async (id) => activities.find((a) => a.id === id) ?? null,
    track: async () => [],
    weight: async () => WEIGHT,
    weather: async () => WEATHER,
    warnings: async () => [],
  };
}
