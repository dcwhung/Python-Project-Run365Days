import type {
  Activity,
  DayDistance,
  MonthSummary,
  PersonalBests,
  Totals,
  TrainingLoadPoint,
  WeekSummary,
  YearSummary,
} from "./types";

/**
 * Year aggregations for static mode. This is a line-for-line port of
 * `src/dashboard/stats.py`; the Python tests and `stats.test.ts` pin the
 * same expected values so both modes agree.
 */

export const CTL_DAYS = 42;
export const ATL_DAYS = 7;
export const FASTEST_MIN_KM = 5.0;
const MONTHS_PER_YEAR = 12;
const DAYS_PER_WEEK = 7;
const MS_PER_DAY = 86_400_000;

export function daysInYear(year: number): number {
  return (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0 ? 366 : 365;
}

/** Local-date helpers that avoid time-zone drift: everything is UTC midnight. */
function utc(year: number, month: number, day: number): Date {
  return new Date(Date.UTC(year, month - 1, day));
}
function iso(d: Date): string {
  return d.toISOString().slice(0, 10);
}
function parse(date: string): Date {
  const [y, m, d] = date.split("-").map(Number);
  return utc(y, m, d);
}
function dayOfYear(d: Date): number {
  return Math.round((d.getTime() - utc(d.getUTCFullYear(), 1, 1).getTime()) / MS_PER_DAY) + 1;
}
/** Monday = 0 ... Sunday = 6, matching Python's weekday(). */
function weekday(d: Date): number {
  return (d.getUTCDay() + 6) % 7;
}

const round = (v: number, digits: number) => {
  const f = 10 ** digits;
  return Math.round(v * f) / f;
};
const sum = (values: (number | null | undefined)[]) =>
  values.reduce<number>((acc, v) => acc + (v ?? 0), 0);
const mean = (values: (number | null | undefined)[]): number | null => {
  const vals = values.filter((v): v is number => v != null);
  return vals.length ? sum(vals) / vals.length : null;
};
const pace = (km: number, sec: number): number | null => (km ? Math.round(sec / km) : null);

function fastest(activities: Activity[]): Activity | null {
  const eligible = activities.filter(
    (a) => a.paceSecPerKm && (a.distanceKm ?? 0) >= FASTEST_MIN_KM,
  );
  if (!eligible.length) return null;
  return eligible.reduce((b, a) => (a.paceSecPerKm! < b.paceSecPerKm! ? a : b));
}

export function totals(activities: Activity[], year: number): Totals {
  const km = sum(activities.map((a) => a.distanceKm));
  const sec = sum(activities.map((a) => a.durationSec));
  return {
    runs: activities.length,
    days: daysInYear(year),
    activeDays: new Set(activities.map((a) => a.date)).size,
    distanceKm: round(km, 2),
    durationSec: Math.trunc(sec),
    calories: Math.trunc(sum(activities.map((a) => a.calories))),
    avgPaceSecPerKm: pace(km, sec),
    avgCadence: mean(activities.map((a) => a.avgCadence)),
    avgDistanceKm: activities.length ? round(km / activities.length, 2) : 0,
  };
}

export function monthly(activities: Activity[]): MonthSummary[] {
  const rows: MonthSummary[] = [];
  for (let month = 1; month <= MONTHS_PER_YEAR; month++) {
    const group = activities.filter((a) => Number(a.date.slice(5, 7)) === month);
    const km = sum(group.map((a) => a.distanceKm));
    const sec = sum(group.map((a) => a.durationSec));
    const best = fastest(group);
    rows.push({
      month,
      runs: group.length,
      distanceKm: round(km, 2),
      durationSec: Math.trunc(sec),
      calories: Math.trunc(sum(group.map((a) => a.calories))),
      avgPaceSecPerKm: pace(km, sec),
      avgCadence: mean(group.map((a) => a.avgCadence)),
      bestPaceSecPerKm: best?.paceSecPerKm ?? null,
      bestPaceActivityId: best?.id ?? null,
    });
  }
  return rows;
}

export function weekIndex(date: string, year: number): number {
  const startDow = weekday(utc(year, 1, 1));
  return Math.floor((dayOfYear(parse(date)) - 1 + startDow) / DAYS_PER_WEEK);
}

export function weekly(activities: Activity[], year: number): WeekSummary[] {
  const jan1 = utc(year, 1, 1);
  const nWeeks = weekIndex(`${year}-12-31`, year) + 1;
  const weeks: WeekSummary[] = [];
  for (let w = 0; w < nWeeks; w++) {
    const start = new Date(jan1.getTime() + (w * DAYS_PER_WEEK - weekday(jan1)) * MS_PER_DAY);
    weeks.push({
      week: w,
      weekStart: iso(start < jan1 ? jan1 : start),
      runs: 0,
      distanceKm: 0,
      durationSec: 0,
      avgPaceSecPerKm: null,
      longestKm: 0,
      activityIds: [],
    });
  }
  for (const a of activities) {
    const wk = weeks[weekIndex(a.date, year)];
    wk.runs += 1;
    wk.distanceKm += a.distanceKm ?? 0;
    wk.durationSec += a.durationSec ?? 0;
    wk.longestKm = Math.max(wk.longestKm, a.distanceKm ?? 0);
    wk.activityIds.push(a.id);
  }
  for (const wk of weeks) {
    wk.distanceKm = round(wk.distanceKm, 2);
    wk.avgPaceSecPerKm = pace(wk.distanceKm, wk.durationSec);
  }
  return weeks;
}

export function dailyDistance(activities: Activity[], year: number): DayDistance[] {
  const perDay = new Map<string, { distanceKm: number; activityId: string }>();
  for (const a of activities) {
    const row = perDay.get(a.date) ?? { distanceKm: 0, activityId: a.id };
    row.distanceKm += a.distanceKm ?? 0;
    perDay.set(a.date, row);
  }
  const jan1 = utc(year, 1, 1);
  const out: DayDistance[] = [];
  for (let i = 0; i < daysInYear(year); i++) {
    const d = iso(new Date(jan1.getTime() + i * MS_PER_DAY));
    const row = perDay.get(d);
    out.push({
      date: d,
      distanceKm: row ? round(row.distanceKm, 2) : 0,
      activityId: row?.activityId ?? null,
    });
  }
  return out;
}

export function trainingLoad(daily: DayDistance[]): TrainingLoadPoint[] {
  let ctl = 0;
  let atl = 0;
  return daily.map((row) => {
    ctl += (row.distanceKm - ctl) / CTL_DAYS;
    atl += (row.distanceKm - atl) / ATL_DAYS;
    return { date: row.date, ctl: round(ctl, 2), atl: round(atl, 2), tsb: round(ctl - atl, 2) };
  });
}

export function personalBests(activities: Activity[]): PersonalBests {
  if (!activities.length) {
    return { longest: null, fastest: null, longestTime: null, mostCalories: null, topCadence: null };
  }
  const maxBy = (list: Activity[], key: (a: Activity) => number) =>
    list.reduce((b, a) => (key(a) > key(b) ? a : b));
  const withKcal = activities.filter((a) => a.calories != null);
  const withCad = activities.filter((a) => a.avgCadence != null);
  return {
    longest: maxBy(activities, (a) => a.distanceKm ?? 0),
    fastest: fastest(activities),
    longestTime: maxBy(activities, (a) => a.durationSec ?? 0),
    mostCalories: withKcal.length ? maxBy(withKcal, (a) => a.calories!) : null,
    topCadence: withCad.length ? maxBy(withCad, (a) => a.avgCadence!) : null,
  };
}

export function yearSummary(activities: Activity[], year: number): YearSummary {
  const daily = dailyDistance(activities, year);
  return {
    year,
    totals: totals(activities, year),
    monthly: monthly(activities),
    weekly: weekly(activities, year),
    dailyDistance: daily,
    trainingLoad: trainingLoad(daily),
    personalBests: personalBests(activities),
  };
}
