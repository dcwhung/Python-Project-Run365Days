import type { DayDistance, WeekSummary, WeightEntry } from "@/data/types";
import { MS_PER_DAY, weekday } from "@/lib/dates";
import { mean, sum } from "@/lib/stats-helpers";
import { weekIndex } from "@/data/stats";

export const WEEKS_PER_YEAR = 52;

export function weightKpis(entries: WeightEntry[], heightCm: number, days: number) {
  if (!entries.length) return null;
  const first = entries[0];
  const last = entries[entries.length - 1];
  const min = entries.reduce((b, r) => (r.weightLbs < b.weightLbs ? r : b));
  const max = entries.reduce((b, r) => (r.weightLbs > b.weightLbs ? r : b));
  const loss = first.weightLbs - last.weightLbs;
  const bmi = (lbs: number) => (lbs * 0.45359237) / (heightCm / 100) ** 2;
  return {
    first,
    last,
    min,
    max,
    loss,
    lossPct: (loss / first.weightLbs) * 100,
    lossPerWeek: loss / WEEKS_PER_YEAR,
    bmiStart: bmi(first.weightLbs),
    bmiEnd: bmi(last.weightLbs),
    weighIns: entries.length,
    missing: days - entries.length,
  };
}

/** Daily series aligned to `labels`, plus a trailing 7-day average of present values. */
export function dailySeries(entries: WeightEntry[], labels: string[]) {
  const byDate = new Map(entries.map((w) => [w.date, w.weightLbs]));
  const series = labels.map((d) => byDate.get(d) ?? null);
  const ma7 = series.map((_, i) => {
    const v = series.slice(Math.max(0, i - 6), i + 1).filter((x): x is number => x != null);
    return v.length ? mean(v) : null;
  });
  return { series, ma7 };
}

/** Last weigh-in of each month (null when none). */
export function lastOfMonth(entries: WeightEntry[]): (number | null)[] {
  return Array.from({ length: 12 }, (_, m) => {
    const r = entries.filter((w) => Number(w.date.slice(5, 7)) === m + 1);
    return r.length ? r[r.length - 1].weightLbs : null;
  });
}

/** Month-end weight minus previous month-end (January vs the first record). */
export function monthlyChange(entries: WeightEntry[]): (number | null)[] {
  if (!entries.length) return Array(12).fill(null);
  const ends = lastOfMonth(entries);
  return ends.map((v, m) => (v == null ? null : v - (m ? ends[m - 1] ?? entries[0].weightLbs : entries[0].weightLbs)));
}

export interface Delta {
  date: string;
  value: number;
}

/** Day-to-day changes between consecutive-day weigh-ins only. */
export function dailyDeltas(entries: WeightEntry[]): Delta[] {
  const out: Delta[] = [];
  for (let i = 1; i < entries.length; i++) {
    const prev = Date.parse(`${entries[i - 1].date}T00:00:00Z`);
    const cur = Date.parse(`${entries[i].date}T00:00:00Z`);
    if ((cur - prev) / MS_PER_DAY === 1) out.push({ date: entries[i].date, value: entries[i].weightLbs - entries[i - 1].weightLbs });
  }
  return out;
}

export function weekdayDelta(deltas: Delta[]): (number | null)[] {
  return Array.from({ length: 7 }, (_, i) => mean(deltas.filter((d) => weekday(d.date) === i).map((d) => d.value)));
}

/** Weekly km against the weight change from the first to the last weigh-in of that week. */
export function weeklyKmVsChange(weeks: WeekSummary[], entries: WeightEntry[], daily: DayDistance[], year: number) {
  const byWeek = new Map<number, number[]>();
  const byDate = new Map(entries.map((w) => [w.date, w.weightLbs]));
  for (const d of daily) {
    const v = byDate.get(d.date);
    if (v == null) continue;
    const w = weekIndex(d.date, year);
    byWeek.set(w, [...(byWeek.get(w) ?? []), v]);
  }
  return weeks
    .map((wk) => {
      const ws = byWeek.get(wk.week) ?? [];
      if (ws.length < 2 || !wk.runs) return null;
      return { x: wk.distanceKm, y: ws[ws.length - 1] - ws[0], weekStart: wk.weekStart };
    })
    .filter((p): p is NonNullable<typeof p> => p != null);
}

export function monthlyUpDown(deltas: Delta[], entries: WeightEntry[]) {
  const ends = lastOfMonth(entries);
  return Array.from({ length: 12 }, (_, i) => {
    const d = deltas.filter((x) => Number(x.date.slice(5, 7)) === i + 1);
    if (!d.length) return null;
    const up = d.filter((x) => x.value > 0).length;
    const down = d.filter((x) => x.value < 0).length;
    return { month: i + 1, up, down, same: d.length - up - down, net: sum(d.map((x) => x.value)), end: ends[i] };
  }).filter((r): r is NonNullable<typeof r> => r != null);
}
