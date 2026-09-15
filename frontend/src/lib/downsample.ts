import { roundHalfEven } from "./rounding";

/** Keep at most `limit` evenly spaced items, always including first and last (mirrors builder.downsample). */
export function downsample<T>(items: readonly T[], limit: number): T[] {
  const n = items.length;
  if (n <= limit) return [...items];
  if (limit < 2) return [items[n - 1]];
  const step = (n - 1) / (limit - 1);
  // Python's round, not Math.round: a half-integer index picks the even
  // neighbour there and the upper one here, so the two ports would return a
  // different GPS sample for the same request (AU-009).
  return Array.from({ length: limit }, (_, i) => items[roundHalfEven(i * step)]);
}
