import type { Activity } from "@/data/types";
import { actTemp } from "@/lib/weather";

/** Pure filtering, sorting and CSV export for the Activities table. */

export type SortKey =
  | "date"
  | "distanceKm"
  | "durationSec"
  | "paceSecPerKm"
  | "avgCadence"
  | "ascentM"
  | "calories"
  | "temp"
  | "weather"
  | "warnings"
  | "hasGps";

export interface Filters {
  query: string;
  month: number | null;
  gps: boolean | null;
  minKm: number;
}

export const DEFAULT_FILTERS: Filters = { query: "", month: null, gps: null, minKm: 0 };

const SORT_VALUE: Record<SortKey, (a: Activity) => number | string> = {
  date: (a) => a.date + a.startTime,
  distanceKm: (a) => a.distanceKm,
  durationSec: (a) => a.durationSec,
  paceSecPerKm: (a) => a.paceSecPerKm ?? 9e9,
  avgCadence: (a) => a.avgCadence ?? 0,
  ascentM: (a) => a.ascentM ?? 0,
  calories: (a) => a.calories ?? 0,
  temp: (a) => actTemp(a) ?? -99,
  weather: (a) => a.weather?.description ?? "",
  warnings: (a) => a.warnings.length,
  hasGps: (a) => (a.hasGps ? 1 : 0),
};

export function matches(a: Activity, f: Filters): boolean {
  if (f.month != null && Number(a.date.slice(5, 7)) !== f.month) return false;
  if (f.gps != null && a.hasGps !== f.gps) return false;
  if (a.distanceKm < f.minKm) return false;
  const needle = f.query.trim().toLowerCase();
  if (!needle) return true;
  const hay = [a.date, a.startTime, a.weather?.description, ...a.warnings, a.hasGps ? "outdoor" : "indoor"]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  return hay.includes(needle);
}

export function filterAndSort(activities: Activity[], f: Filters, sort: SortKey, dir: 1 | -1): Activity[] {
  const value = SORT_VALUE[sort];
  return activities
    .filter((a) => matches(a, f))
    .sort((x, y) => {
      const a = value(x);
      const b = value(y);
      return (a > b ? 1 : a < b ? -1 : 0) * dir;
    });
}

export const CSV_HEADER = [
  "date",
  "time",
  "activity_id",
  "km",
  "seconds",
  "pace_sec_per_km",
  "cadence_spm",
  "ascent_m",
  "kcal",
  "temp_c",
  "weather",
  "humidity",
  "warnings",
  "gps",
];

const esc = (v: unknown) => {
  if (v == null) return "";
  const s = String(v);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};

export function toCsv(activities: Activity[]): string {
  const lines = activities.map((a) =>
    [
      a.date,
      a.startTime,
      a.id,
      a.distanceKm,
      a.durationSec,
      a.paceSecPerKm,
      a.avgCadence,
      a.ascentM,
      a.calories,
      actTemp(a),
      a.weather?.description,
      a.weather?.humidityPct,
      a.warnings.join("; "),
      a.hasGps ? 1 : 0,
    ]
      .map(esc)
      .join(","),
  );
  return `${CSV_HEADER.join(",")}\n${lines.join("\n")}`;
}
