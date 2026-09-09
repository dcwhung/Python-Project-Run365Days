import type { Activity, DayDistance } from "@/data/types";
import { byMonth, byWeekday, paceOf, timeOfDay, TIME_OF_DAY } from "@/lib/analytics";
import { maxBy, mean, minBy, sum } from "@/lib/stats-helpers";
import { FASTEST_MIN_KM } from "@/data/stats";

export const DISTANCE_BINS: [number, number, string][] = [
  [0, 4, "< 4"],
  [4, 5, "4–5"],
  [5, 6, "5–6"],
  [6, 7, "6–7"],
  [7, 8, "7–8"],
  [8, 10, "8–10"],
  [10, 99, "10+"],
];
export const TREND_DAYS = 30;
export const LONG_RUN_KM = 10;
export const MID_RUN_KM = 7;

export function fastestOver(list: Activity[], minKm = FASTEST_MIN_KM): Activity | null {
  return minBy(
    list.filter((a) => a.distanceKm >= minKm && a.paceSecPerKm),
    (a) => a.paceSecPerKm,
  );
}

export function performanceKpis(activities: Activity[]) {
  const km = sum(activities.map((a) => a.distanceKm));
  const sec = sum(activities.map((a) => a.durationSec));
  const cads = activities.map((a) => a.avgCadence).filter((c): c is number => c != null);
  return {
    runs: activities.length,
    avgPace: km ? sec / km : null,
    fastest: fastestOver(activities),
    avgKm: activities.length ? km / activities.length : 0,
    avgSec: activities.length ? sec / activities.length : 0,
    longest: maxBy(activities, (a) => a.distanceKm),
    avgCadence: mean(cads),
    maxCadence: cads.length ? Math.max(...cads) : null,
    longRuns: activities.filter((a) => a.distanceKm >= LONG_RUN_KM).length,
    midRuns: activities.filter((a) => a.distanceKm >= MID_RUN_KM).length,
  };
}

/** Per-day pace (min/km) of the first run each day, plus a trailing 30-day distance-weighted trend. */
export function paceSeries(activities: Activity[], daily: DayDistance[]) {
  const byId = new Map(activities.map((a) => [a.id, a]));
  const dayAct = daily.map((d) => (d.activityId ? byId.get(d.activityId) ?? null : null));
  const points = dayAct.map((a) => (a?.paceSecPerKm ? Math.round((a.paceSecPerKm / 60) * 100) / 100 : null));
  const trend = daily.map((_, i) => {
    let s = 0;
    let k = 0;
    for (let d = Math.max(0, i - TREND_DAYS + 1); d <= i; d++) {
      const a = dayAct[d];
      if (a?.paceSecPerKm) {
        s += a.durationSec;
        k += a.distanceKm;
      }
    }
    return k ? Math.round((s / 60 / k) * 100) / 100 : null;
  });
  return { points, trend, dayAct };
}

export function distanceHistogram(activities: Activity[]) {
  return DISTANCE_BINS.map(([lo, hi, label]) => ({
    label: `${label} km`,
    count: activities.filter((a) => a.distanceKm >= lo && a.distanceKm < hi).length,
  }));
}

export function weekdayPace(activities: Activity[]) {
  return byWeekday(activities).map((g) => ({ pace: paceOf(g), runs: g.length }));
}

export function timeOfDayPace(activities: Activity[]) {
  const groups: Activity[][] = TIME_OF_DAY.map(() => []);
  for (const a of activities) groups[timeOfDay(a.startTime)].push(a);
  return groups.map((g, i) => ({ label: TIME_OF_DAY[i], pace: paceOf(g), runs: g.length }));
}

export function monthlyTable(activities: Activity[]) {
  return byMonth(activities)
    .map((g, i) => {
      if (!g.length) return null;
      const km = sum(g.map((a) => a.distanceKm));
      const sec = sum(g.map((a) => a.durationSec));
      return {
        month: i + 1,
        runs: g.length,
        km,
        sec,
        pace: km ? sec / km : null,
        best: fastestOver(g)?.paceSecPerKm ?? null,
        cadence: mean(g.map((a) => a.avgCadence)),
        kcal: sum(g.map((a) => a.calories)),
      };
    })
    .filter((r): r is NonNullable<typeof r> => r != null);
}
