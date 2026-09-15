/**
 * The five accent colours, each as the Tailwind background class that paints
 * it. Two places draw a bar in an accent colour -- the rule under a KPI card
 * and the ratio bar in the personal-best list -- and each carried its own copy
 * of this map, which is two owners for one palette. This is the one owner;
 * `AccentName` is the one list of names that goes with it.
 *
 * The class strings are spelled out in full because Tailwind's source scanner
 * reads them as text: a class name assembled from the key would generate
 * nothing at build time.
 */
export const ACCENT_BG = {
  accent: "bg-accent",
  accent2: "bg-accent2",
  warn: "bg-warn",
  danger: "bg-danger",
  violet: "bg-violet",
} as const;

export type AccentName = keyof typeof ACCENT_BG;
