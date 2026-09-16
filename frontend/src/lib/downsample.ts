/**
 * Round half to even, the way Python's built-in `round` does.
 *
 * `Math.round` rounds a half up, so it drifts one index away from the Python
 * samplers on any step that lands exactly on .5. CUI-0021 measured that over
 * the lengths the export really writes: rebuild it with `run365-export`, then
 * `SELECT COUNT(DISTINCT cnt) FROM (SELECT COUNT(*) AS cnt FROM track_points
 * GROUP BY activity_id)` yields 123 distinct track lengths between 250 and
 * 600. Pairing each of those with every `points` the sampler can be *asked*
 * for -- `1 .. length - 1` -- gives 45,117 `(track_length, points)` pairs, and
 * the two roundings pick different indices for 10,189 of them (22.6%).
 *
 * "Can be asked for" is the load-bearing half of that sentence: none of those
 * 10,189 pairs is reachable today. The export caps a track at 600 rows and the
 * only caller asks for `TRACK_POINTS = 600`, so every real call is `n <= limit`
 * and returns the track untouched, never reaching this function. Widening the
 * range instead -- every length in `2..1000` rather than the exported 123 --
 * gives 21.1%, which is why the figure only reproduces against the definition
 * spelled out above.
 *
 * Do not smoke-test this by lowering `points` to an even number: a tie needs
 * `i * (n - 1) / (limit - 1)` to be a half, so `2(n - 1)i = (limit - 1)(2k + 1)`,
 * and an odd `limit - 1` divides `(n - 1)i` outright and forces an integer.
 * An even `limit` therefore cannot produce a single tie at any length --
 * verified over every `limit` in `2..400` against every `n` up to 1000, zero
 * divergent pairs, against 68,503 for odd `limit`. Dropping to 150 pulls all
 * 365 exported tracks into the sampler and still reports 0% divergence, which
 * reads as proof the two modes agree and is nothing of the kind. Use an odd
 * `points` to see the real behaviour: 97, 193 and 241 each split more than 80%
 * of the exported lengths.
 *
 * The two modes draw the same field, so this side follows Python rather than
 * the other way round: the static JSON this module reads was itself written by
 * `run365days.dashboard.builder.downsample`.
 *
 * `value - floor` is exact rather than merely close: `lower <= value <
 * lower + 1` puts the two operands within a factor of two of each other, so
 * Sterbenz's lemma makes the subtraction error-free. Above 2^52 no halves
 * exist and this degenerates to identity. The tie branch is sign-correct as
 * written, since JS `%` keeps the dividend's sign and `-0 === 0`, so negative
 * ties land on the even side exactly as Python does (-0.5 -> 0, -1.5 -> -2,
 * -2.5 -> -2). `downsample` only ever passes non-negative values, but a sign
 * case would be a regression here, not a fix.
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
 * `run365days.api.service._even_positions` exactly for an integer `limit`,
 * rounding included, so api mode and static mode return the same rows.
 * `limit < 2` yields the last item alone, matching both (CUI-0004). A
 * non-integer `limit` is outside the claim -- Python raises on `range(2.5)`
 * and on `range(NaN)` where this quietly returns 2 items and `[]` -- but
 * every call site passes the integer literal 600.
 */
export function downsample<T>(items: readonly T[], limit: number): T[] {
  const n = items.length;
  if (n <= limit) return [...items];
  if (limit < 2) return [items[n - 1]];
  const step = (n - 1) / (limit - 1);
  return Array.from({ length: limit }, (_, i) => items[roundHalfToEven(i * step)]);
}
