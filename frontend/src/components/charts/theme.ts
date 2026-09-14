import { FONT_SANS, TOKENS } from "@/styles/tokens";

// Re-exported so a chart file needs one import for "a colour" and "that
// colour, faded" -- the fills under lines and the scatter dots want the latter.
export { alpha } from "@/styles/tokens";
import {
  BarController,
  BarElement,
  CategoryScale,
  Chart,
  Filler,
  Legend,
  LineController,
  LineElement,
  LinearScale,
  PointElement,
  ScatterController,
  Tooltip,
} from "chart.js";

Chart.register(
  BarController,
  BarElement,
  CategoryScale,
  Filler,
  Legend,
  LineController,
  LineElement,
  LinearScale,
  PointElement,
  ScatterController,
  Tooltip,
);

Chart.defaults.color = TOKENS.muted;
Chart.defaults.borderColor = TOKENS.border;
Chart.defaults.font.family = FONT_SANS;

/**
 * The chart palette, named by the job each colour does on a chart rather than
 * by the token it comes from -- `grid` is the border token, and callers should
 * not have to know that. Every value comes from `TOKENS`; nothing is declared
 * here, so the palette can only ever say what index.css says.
 */
export const COLORS = {
  accent: TOKENS.accent,
  accent2: TOKENS.accent2,
  warn: TOKENS.warn,
  danger: TOKENS.danger,
  violet: TOKENS.violet,
  blue: TOKENS.blue,
  grid: TOKENS.border,
};

export const BASE = { responsive: true, maintainAspectRatio: false } as const;
export const GRID = { color: COLORS.grid };
export const NO_GRID = { display: false };
export const NO_LEGEND = { legend: { display: false } };

/** Day-of-year x axis: month names on the 1st, nothing else. */
export function dayAxis(labels: string[]) {
  return {
    grid: NO_GRID,
    ticks: {
      callback: (_v: unknown, i: number) =>
        labels[i]?.endsWith("-01")
          ? ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][
              Number(labels[i].slice(5, 7)) - 1
            ]
          : null,
      autoSkip: false,
      maxRotation: 0,
      font: { size: 10 },
    },
  };
}
