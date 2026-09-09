/** Keep at most `limit` evenly spaced items, always including first and last (mirrors builder.downsample). */
export function downsample<T>(items: readonly T[], limit: number): T[] {
  const n = items.length;
  if (n <= limit) return [...items];
  if (limit < 2) return [items[n - 1]];
  const step = (n - 1) / (limit - 1);
  return Array.from({ length: limit }, (_, i) => items[Math.round(i * step)]);
}
