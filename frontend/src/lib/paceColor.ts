/** Pace colour: slow red -> amber -> fast green (Garmin convention). */
const C_SLOW = [248, 113, 113];
const C_MID = [245, 158, 11];
const C_FAST = [52, 211, 153];
export const NO_PACE_COLOR = "#2e3250";

const mix = (a: number[], b: number[], k: number) =>
  `rgb(${a.map((v, i) => Math.round(v + (b[i] - v) * k)).join(",")})`;

/** `paceMinPerKm` in minutes; `fast`/`slow` are the bounds from Settings. */
export function paceColor(paceMinPerKm: number, fast: number, slow: number): string {
  if (!Number.isFinite(paceMinPerKm)) return NO_PACE_COLOR;
  const t = 1 - Math.max(0, Math.min(1, (paceMinPerKm - fast) / (slow - fast)));
  return t < 0.5 ? mix(C_SLOW, C_MID, t / 0.5) : mix(C_MID, C_FAST, (t - 0.5) / 0.5);
}
