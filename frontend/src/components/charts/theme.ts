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

Chart.defaults.color = "#7c85a8";
Chart.defaults.borderColor = "#2e3250";
Chart.defaults.font.family = "'Segoe UI', system-ui, sans-serif";

export const COLORS = {
  accent: "#4f8ef7",
  accent2: "#34d399",
  warn: "#f59e0b",
  danger: "#f87171",
  violet: "#a78bfa",
  blue: "#60a5fa",
  grid: "#2e3250",
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
