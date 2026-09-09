import type {
  Activity,
  DailyWeather,
  Meta,
  TrackPoint,
  WeatherWarning,
  WeightEntry,
} from "../types";

/**
 * Map the snake_case JSON written by `run365-export --static-dir` onto the
 * camelCase domain types, so views never see which mode produced the data.
 */

export type StaticActivity = {
  id: string;
  date: string;
  start_time: string;
  day_of_year: number;
  distance_km: number;
  duration_sec: number;
  pace_sec_per_km: number | null;
  calories: number | null;
  avg_cadence: number | null;
  avg_temp_c: number | null;
  elevation_min_m: number | null;
  elevation_max_m: number | null;
  ascent_m: number | null;
  has_gps: boolean;
  num_points: number;
  weather: {
    description: string | null;
    temp_c: number | null;
    humidity_pct: number | null;
    wind_kmh: number | null;
  } | null;
  warnings: string[];
};

export type StaticTrack = { columns: string[]; rows: (number | null)[][] };
export type StaticWeight = { date: string; weight_lbs: number; weight_kg: number; bmi: number };
export type StaticWeather = {
  date: string;
  max_temp_c: number | null;
  avg_temp_c: number | null;
  min_temp_c: number | null;
  humidity_pct: number | null;
  rainfall_mm: number | null;
  wind_kmh: number | null;
  sunrise: string | null;
  sunset: string | null;
};
export type StaticWarning = {
  date: string;
  type: string | null;
  signal: string;
  start_time: string | null;
  end_time: string | null;
};
export type StaticMeta = { year: number; generated_at: string };

export function mapActivity(a: StaticActivity): Activity {
  return {
    id: a.id,
    date: a.date,
    startTime: a.start_time,
    dayOfYear: a.day_of_year,
    distanceKm: a.distance_km,
    durationSec: a.duration_sec,
    paceSecPerKm: a.pace_sec_per_km,
    calories: a.calories,
    avgCadence: a.avg_cadence,
    avgTempC: a.avg_temp_c,
    elevationMinM: a.elevation_min_m,
    elevationMaxM: a.elevation_max_m,
    ascentM: a.ascent_m,
    hasGps: a.has_gps,
    numPoints: a.num_points,
    weather: a.weather
      ? {
          description: a.weather.description,
          tempC: a.weather.temp_c,
          humidityPct: a.weather.humidity_pct,
          windKmh: a.weather.wind_kmh,
        }
      : null,
    warnings: a.warnings ?? [],
  };
}

const TRACK_KEYS: Record<string, keyof TrackPoint> = {
  sec: "sec",
  lat: "lat",
  lon: "lon",
  elevation_m: "elevationM",
  distance_m: "distanceM",
  speed_mps: "speedMps",
  cadence: "cadence",
  temp_c: "tempC",
};

export function mapTrack(t: StaticTrack): TrackPoint[] {
  const keys = t.columns.map((c) => {
    const k = TRACK_KEYS[c];
    if (!k) throw new Error(`Unknown track column: ${c}`);
    return k;
  });
  return t.rows.map((row) => {
    const p = {} as Record<keyof TrackPoint, number | null>;
    keys.forEach((k, i) => {
      p[k] = row[i] ?? null;
    });
    return p as unknown as TrackPoint;
  });
}

export function mapWeight(w: StaticWeight): WeightEntry {
  return { date: w.date, weightLbs: w.weight_lbs, weightKg: w.weight_kg, bmi: w.bmi };
}

export function mapWeather(d: StaticWeather): DailyWeather {
  return {
    date: d.date,
    maxTempC: d.max_temp_c,
    avgTempC: d.avg_temp_c,
    minTempC: d.min_temp_c,
    humidityPct: d.humidity_pct,
    rainfallMm: d.rainfall_mm,
    windKmh: d.wind_kmh,
    sunrise: d.sunrise,
    sunset: d.sunset,
  };
}

export function mapWarning(w: StaticWarning): WeatherWarning {
  return {
    date: w.date,
    type: w.type,
    signal: w.signal,
    startTime: w.start_time,
    endTime: w.end_time,
  };
}

export function mapMeta(m: StaticMeta): Meta {
  return { year: m.year, generatedAt: m.generated_at };
}
