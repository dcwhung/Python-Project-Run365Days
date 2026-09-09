import type { Activity, TrainingLoadPoint } from "@/data/types";
import { byWeekday, type WeekWithActivities } from "@/lib/analytics";
import { mean, sum } from "@/lib/stats-helpers";

export const FULL_WEEK_RUNS = 7;
export const ROLLING_WEEKS = 4;
export const TOP_WEEKS = 8;

export function loadKpis(load: TrainingLoadPoint[], weeks: WeekWithActivities[], totalKm: number, days: number, activeDays: number) {
  const last = load[load.length - 1];
  const peak = load.reduce((b, p) => (p.ctl > b.ctl ? p : b), load[0]);
  const biggest = weeks.reduce((b, w) => (w.distanceKm > b.distanceKm ? w : b), weeks[0]);
  const full = weeks.filter((w) => w.runs === FULL_WEEK_RUNS);
  return {
    ctl: last.ctl,
    atl: last.atl,
    tsb: last.tsb,
    peakCtl: peak.ctl,
    peakDate: peak.date,
    biggest,
    avgWeekKm: totalKm / (days / 7),
    avgFullWeekKm: mean(full.map((w) => w.distanceKm)),
    activeDays,
    days,
  };
}

/** Trailing 4-week mean of weekly distance. */
export function rollingWeeks(weeks: { distanceKm: number }[]): number[] {
  return weeks.map((_, i) => Math.round(mean(weeks.slice(Math.max(0, i - ROLLING_WEEKS + 1), i + 1).map((w) => w.distanceKm))! * 10) / 10);
}

export function weekdayAvgKm(activities: Activity[]): number[] {
  return byWeekday(activities).map((g) => Math.round((sum(g.map((a) => a.distanceKm)) / Math.max(1, g.length)) * 100) / 100);
}

export function topWeeks(weeks: WeekWithActivities[], n = TOP_WEEKS): WeekWithActivities[] {
  return [...weeks].sort((x, y) => y.distanceKm - x.distanceKm).slice(0, n);
}

export function weekChange(weeks: { distanceKm: number }[], i: number): number | null {
  const prev = i ? weeks[i - 1].distanceKm : 0;
  return prev ? ((weeks[i].distanceKm - prev) / prev) * 100 : null;
}
