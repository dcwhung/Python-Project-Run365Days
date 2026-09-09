import type { Activity, WeightEntry } from "@/data/types";
import { byMonth } from "@/lib/analytics";
import { sum } from "@/lib/stats-helpers";

export function monthlyHours(activities: Activity[]): number[] {
  return byMonth(activities).map((g) => sum(g.map((a) => a.durationSec)) / 3600);
}

export function weightReview(entries: WeightEntry[]) {
  if (!entries.length) return null;
  const first = entries[0];
  const last = entries[entries.length - 1];
  const min = entries.reduce((b, r) => (r.weightLbs < b.weightLbs ? r : b));
  return { first, last, min, lossPct: ((first.weightLbs - last.weightLbs) / first.weightLbs) * 100 };
}
