import type { Activity, DailyWeather, DayDistance, PersonalBests, Totals } from "@/data/types";
import { actTemp, isSevere } from "@/lib/weather";
import { mean } from "@/lib/stats-helpers";
import { fmtDuration, fmtPace } from "@/lib/format";

/** Pure view-model helpers for the Overview; every function is unit-tested. */

export const HEATMAP_LEVELS = [
  { max: 0, level: 0 },
  { max: 4, level: 1 },
  { max: 6, level: 2 },
  { max: 7, level: 3 },
  { max: 9, level: 4 },
  { max: Infinity, level: 5 },
] as const;

/** 0 = rest day, 1..5 = distance band used for the heatmap colour. */
export function heatLevel(km: number): number {
  if (km <= 0) return 0;
  return HEATMAP_LEVELS.find((b) => km < b.max)?.level ?? 5;
}

export interface PersonalBestRow {
  label: string;
  activity: Activity;
  value: string;
  /** Typical (average) run relative to the record, 0..1, drawn as the bar width. */
  ratio: number;
  color: "violet" | "accent" | "accent2" | "warn" | "danger";
}

export function personalBestRows(pb: PersonalBests, totals: Totals): PersonalBestRow[] {
  const avgKm = totals.runs ? totals.distanceKm / totals.runs : 0;
  const avgSec = totals.runs ? totals.durationSec / totals.runs : 0;
  const avgKcal = totals.runs ? totals.calories / totals.runs : 0;
  const avgCad = totals.avgCadence ?? 0;
  const rows: PersonalBestRow[] = [];
  if (pb.longest)
    rows.push({ label: "Longest", activity: pb.longest, value: `${pb.longest.distanceKm.toFixed(2)} km`, ratio: avgKm / pb.longest.distanceKm, color: "violet" });
  if (pb.fastest && pb.fastest.paceSecPerKm)
    rows.push({ label: "Fastest", activity: pb.fastest, value: `${fmtPace(pb.fastest.paceSecPerKm)}/km`, ratio: avgKm ? pb.fastest.paceSecPerKm / (avgSec / avgKm) : 0, color: "accent" });
  if (pb.longestTime)
    rows.push({ label: "Longest time", activity: pb.longestTime, value: fmtDuration(pb.longestTime.durationSec), ratio: avgSec / pb.longestTime.durationSec, color: "accent2" });
  if (pb.mostCalories && pb.mostCalories.calories)
    rows.push({ label: "Most kcal", activity: pb.mostCalories, value: `${pb.mostCalories.calories} kcal`, ratio: avgKcal / pb.mostCalories.calories, color: "warn" });
  if (pb.topCadence && pb.topCadence.avgCadence)
    rows.push({ label: "Top cadence", activity: pb.topCadence, value: `${Math.round(pb.topCadence.avgCadence)} spm`, ratio: avgCad / pb.topCadence.avgCadence, color: "danger" });
  return rows.map((r) => ({ ...r, ratio: Math.max(0, Math.min(1, r.ratio)) }));
}

export interface WeatherStripStats {
  avgTemp: number | null;
  minTemp: number | null;
  maxTemp: number | null;
  rainyRuns: number;
  severeRuns: number;
  bestBand: { from: number; to: number; pace: number; runs: number } | null;
}

export const TEMP_BAND_WIDTH = 4;
const BEST_BAND_MIN_RUNS = 5;

export function weatherStrip(activities: Activity[], weather: DailyWeather[]): WeatherStripStats {
  const byDate = new Map(weather.map((w) => [w.date, w]));
  const temps = activities.map(actTemp).filter((t): t is number => t != null);
  const rainyRuns = activities.filter((a) => (byDate.get(a.date)?.rainfallMm ?? 0) > 0).length;
  const severeRuns = activities.filter((a) => a.warnings.some(isSevere)).length;
  const bands = new Map<number, number[]>();
  for (const a of activities) {
    const t = actTemp(a);
    if (t == null || !a.paceSecPerKm) continue;
    const b = Math.floor(t / TEMP_BAND_WIDTH) * TEMP_BAND_WIDTH;
    bands.set(b, [...(bands.get(b) ?? []), a.paceSecPerKm]);
  }
  let bestBand: WeatherStripStats["bestBand"] = null;
  for (const [from, paces] of bands) {
    if (paces.length < BEST_BAND_MIN_RUNS) continue;
    const pace = mean(paces)!;
    if (!bestBand || pace < bestBand.pace) bestBand = { from, to: from + TEMP_BAND_WIDTH, pace, runs: paces.length };
  }
  return {
    avgTemp: mean(temps),
    minTemp: temps.length ? Math.min(...temps) : null,
    maxTemp: temps.length ? Math.max(...temps) : null,
    rainyRuns,
    severeRuns,
    bestBand,
  };
}

/** Rolling 7-day mean of daily distance, aligned with `daily`. */
export function rolling7(daily: DayDistance[]): number[] {
  return daily.map((_, i) => {
    const lo = Math.max(0, i - 6);
    let s = 0;
    for (let j = lo; j <= i; j++) s += daily[j].distanceKm;
    return Math.round((s / (i - lo + 1)) * 100) / 100;
  });
}
