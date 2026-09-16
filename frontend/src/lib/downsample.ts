/**
 * Round half to even, the way Python's built-in `round` does.
 *
 * `Math.round` rounds a half up, so it drifts one index away from the Python
 * samplers on any step that lands exactly on .5 -- 22.6% of the
 * `(track_length, points)` pairs the export can produce (CUI-0021). The two
 * modes draw the same field, so this side follows Python rather than the other
 * way round: the static JSON this module reads was itself written by
 * `run365days.dashboard.builder.downsample`.
 *
 * Only called with non-negative values, which is why the fractional part can be
 * taken as `value - floor` (exact in IEEE-754 below 2^52) without a sign case.
 */
function roundHalfToEven(value: number): number {
  const lower = Math.floor(value);
  const fraction = value - lower;
  if (fraction > 0.5) return lower + 1;
  if (fraction < 0.5) return lower;
  return lower % 2 === 0 ? lower : lower + 1;
}

/**
 * Keep at most `limit` evenly spaced items, always including first and last.
 *
 * Mirrors `run365days.dashboard.builder.downsample` and
 * `run365days.api.service._even_positions` exactly, rounding included, so api
 * mode and static mode return the same rows. `limit < 2` yields the last item
 * alone, matching both (CUI-0004).
 */
export function downsample<T>(items: readonly T[], limit: number): T[] {
  const n = items.length;
  if (n <= limit) return [...items];
  if (limit < 2) return [items[n - 1]];
  const step = (n - 1) / (limit - 1);
  return Array.from({ length: limit }, (_, i) => items[roundHalfToEven(i * step)]);
}
