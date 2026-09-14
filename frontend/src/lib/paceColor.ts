import { TOKENS, rgbChannels } from "@/styles/tokens";

/** Pace colour: slow red -> amber -> fast green (Garmin convention). */
const C_SLOW = rgbChannels(TOKENS.danger);
const C_MID = rgbChannels(TOKENS.warn);
const C_FAST = rgbChannels(TOKENS.accent2);
export const NO_PACE_COLOR = TOKENS.border;

/** The same three stops as CSS colour stops, for the legend swatch. */
export const PACE_RAMP = [TOKENS.danger, TOKENS.warn, TOKENS.accent2];

const mix = (a: number[], b: number[], k: number) =>
  `rgb(${a.map((v, i) => Math.round(v + (b[i] - v) * k)).join(",")})`;

/** `paceMinPerKm` in minutes; `fast`/`slow` are the bounds from Settings. */
export function paceColor(paceMinPerKm: number, fast: number, slow: number): string {
  if (!Number.isFinite(paceMinPerKm)) return NO_PACE_COLOR;
  const t = 1 - Math.max(0, Math.min(1, (paceMinPerKm - fast) / (slow - fast)));
  return t < 0.5 ? mix(C_SLOW, C_MID, t / 0.5) : mix(C_MID, C_FAST, (t - 0.5) / 0.5);
}
