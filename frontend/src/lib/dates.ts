import { MONTHS } from "./format";

export const MS_PER_DAY = 86_400_000;
export const DAYS_PER_WEEK = 7;
export const WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

export function isLeap(year: number): boolean {
  return (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0;
}
export function daysInYear(year: number): number {
  return isLeap(year) ? 366 : 365;
}
export function monthLengths(year: number): number[] {
  return [31, isLeap(year) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
}
/** ISO date for day-of-year `doy` (1-based) in `year`, computed in UTC to avoid DST drift. */
export function doyToIso(year: number, doy: number): string {
  return new Date(Date.UTC(year, 0, doy)).toISOString().slice(0, 10);
}
export function isoToDoy(iso: string): number {
  const [y, m, d] = iso.split("-").map(Number);
  return Math.round((Date.UTC(y, m - 1, d) - Date.UTC(y, 0, 1)) / MS_PER_DAY) + 1;
}
/** Monday = 0 ... Sunday = 6. */
export function weekday(iso: string): number {
  const [y, m, d] = iso.split("-").map(Number);
  return (new Date(Date.UTC(y, m - 1, d)).getUTCDay() + 6) % 7;
}
/** One ISO label per day of the year. */
export function dayLabels(year: number): string[] {
  return Array.from({ length: daysInYear(year) }, (_, i) => doyToIso(year, i + 1));
}
/** Chart tick callback: show the month name on the first of each month. */
export function monthTick(labels: string[]) {
  return (_value: unknown, index: number) =>
    labels[index]?.endsWith("-01") ? MONTHS[Number(labels[index].slice(5, 7)) - 1] : null;
}
export function longDate(iso: string): string {
  const [y, m, d] = iso.split("-").map(Number);
  return new Date(Date.UTC(y, m - 1, d)).toUTCString().slice(0, 16);
}
