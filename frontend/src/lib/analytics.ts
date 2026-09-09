import type { Activity, WeekSummary } from "@/data/types";
import { weekday } from "./dates";
import { sum } from "./stats-helpers";

/** Distance-weighted pace in seconds per km for a group of runs, or null. */
export function paceOf(list: Activity[]): number | null {
  const km = sum(list.map((a) => a.distanceKm));
  const sec = sum(list.map((a) => a.durationSec));
  return km ? sec / km : null;
}

export const TIME_OF_DAY = [
  "Early (05–08)",
  "Morning (08–12)",
  "Afternoon (12–17)",
  "Evening (17–21)",
  "Night (21–05)",
] as const;

/** Bucket index into TIME_OF_DAY for an "HH:MM" start time. */
export function timeOfDay(startTime: string): number {
  const h = Number(startTime.slice(0, 2));
  return h < 5 ? 4 : h < 8 ? 0 : h < 12 ? 1 : h < 17 ? 2 : h < 21 ? 3 : 4;
}

export function byWeekday(activities: Activity[]): Activity[][] {
  const groups: Activity[][] = Array.from({ length: 7 }, () => []);
  for (const a of activities) groups[weekday(a.date)].push(a);
  return groups;
}

export function byMonth(activities: Activity[]): Activity[][] {
  const groups: Activity[][] = Array.from({ length: 12 }, () => []);
  for (const a of activities) groups[Number(a.date.slice(5, 7)) - 1].push(a);
  return groups;
}

/** Attach the activity objects to each week summary (weekly only carries ids). */
export function weeksWithActivities(weeks: WeekSummary[], activities: Activity[]) {
  const byId = new Map(activities.map((a) => [a.id, a]));
  return weeks.map((w) => ({ ...w, activities: w.activityIds.map((id) => byId.get(id)!).filter(Boolean) }));
}
export type WeekWithActivities = ReturnType<typeof weeksWithActivities>[number];
