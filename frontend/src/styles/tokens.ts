/**
 * The TypeScript mirror of the `@theme` block in `src/index.css`.
 *
 * Why a mirror exists at all: Chart.js and the raw canvas 2D contexts take
 * colour strings, not CSS custom properties, so `var(--color-accent)` is not
 * something they can resolve. Reading the values back with
 * `getComputedStyle(document.documentElement)` would trade the duplication for
 * a runtime dependency on a live, styled document -- which returns "" under
 * jsdom and during any server-side render, and would silently paint charts
 * black rather than fail loudly.
 *
 * So the values are duplicated, but exactly once, and `tokens.test.ts` reads
 * `index.css` and fails if the two copies ever disagree in either direction.
 * The stylesheet is the owner; this file follows it.
 */
export const TOKENS = {
  bg: "#0f1117",
  surface: "#1a1d27",
  surface2: "#22263a",
  border: "#2e3250",
  accent: "#4f8ef7",
  accent2: "#34d399",
  warn: "#f59e0b",
  danger: "#f87171",
  text: "#e2e8f0",
  muted: "#7c85a8",
  violet: "#a78bfa",
  blue: "#60a5fa",
} as const;

export type TokenName = keyof typeof TOKENS;

/** Mirrors `--font-sans`. Canvas needs the stack as a plain string. */
export const FONT_SANS = '"Segoe UI", system-ui, sans-serif';

/** `#4f8ef7` -> `[79, 142, 247]`. Six-digit hex only, which is all a token is. */
export function rgbChannels(hex: string): [number, number, number] {
  const n = Number.parseInt(hex.slice(1), 16);
  return [(n >> 16) & 0xff, (n >> 8) & 0xff, n & 0xff];
}

/**
 * A token at partial opacity, in the `rgba()` form canvas and Chart.js parse.
 * Fills, hover tints and gradient stops all used to hardcode these channels.
 */
export function alpha(hex: string, opacity: number): string {
  const [r, g, b] = rgbChannels(hex);
  return `rgba(${r},${g},${b},${opacity})`;
}
