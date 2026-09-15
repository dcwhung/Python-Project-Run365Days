import { fmtPace } from "@/lib/format";
import { minToSec, paceToPlotMin } from "@/lib/units";
import { GRID } from "@/components/charts/theme";

/** Pace in seconds/km as the minutes value the pace axes are plotted in. */
export const toPlotMinutes = (sec: number | null) => (sec ? paceToPlotMin(sec) : null);

/**
 * A Chart.js value axis for pace: plotted in minutes, reversed so faster is
 * higher, and ticked back out as m:ss. Four charts on this view need it, so it
 * is declared once and given whatever extra options a chart adds on top.
 */
export function paceScale(extra = {}) {
  return {
    grid: GRID,
    reverse: true,
    ticks: { callback: (v: unknown) => fmtPace(minToSec(Number(v))) },
    ...extra,
  };
}
