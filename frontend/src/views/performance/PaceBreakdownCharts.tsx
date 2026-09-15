import { Bar } from "react-chartjs-2";
import { fmtPace } from "@/lib/format";
import { WEEKDAYS } from "@/lib/dates";
import { minToSec } from "@/lib/units";
import { BASE, COLORS, GRID, NO_GRID, NO_LEGEND, alpha } from "@/components/charts/theme";
import { Card } from "@/components/ui/Card";
import { paceScale, toPlotMinutes } from "./paceAxis";
import type { distanceHistogram, timeOfDayPace, weekdayPace } from "./model";

/** Padding above and below the weekday bars, in plotted minutes. */
const WEEKDAY_AXIS_MARGIN_MIN = 0.2;
/** Stand-in pace when a weekday has no runs, so Math.min/max still resolve. */
const NO_PACE_MIN = 9;

type Histogram = ReturnType<typeof distanceHistogram>;
type WeekdayPace = ReturnType<typeof weekdayPace>;
type TimeOfDayPace = ReturnType<typeof timeOfDayPace>;

/** How many runs fell into each distance bin. */
function DistanceHistogramChart({ histogram }: { histogram: Histogram }) {
  return (
    <Card title="Distance distribution">
      <div className="h-52">
        <Bar
          data={{
            labels: histogram.map((h) => h.label),
            datasets: [
              {
                data: histogram.map((h) => h.count),
                backgroundColor: alpha(COLORS.violet, 0.7),
                borderRadius: 5,
              },
            ],
          }}
          options={{
            ...BASE,
            plugins: NO_LEGEND,
            scales: {
              x: { grid: NO_GRID },
              y: { grid: GRID, ticks: { callback: (v) => `${v} runs` } },
            },
          }}
        />
      </div>
    </Card>
  );
}

/** Average pace for each day of the week, on a tightened axis. */
function WeekdayPaceChart({ weekdayPaces }: { weekdayPaces: WeekdayPace }) {
  const plotted = weekdayPaces.map((w) => toPlotMinutes(w.pace));
  return (
    <Card title="Pace by weekday">
      <div className="h-52">
        <Bar
          data={{
            labels: WEEKDAYS,
            datasets: [
              { data: plotted, backgroundColor: alpha(COLORS.accent, 0.7), borderRadius: 5 },
            ],
          }}
          options={{
            ...BASE,
            plugins: {
              ...NO_LEGEND,
              tooltip: {
                callbacks: {
                  label: (c) =>
                    `${fmtPace(minToSec(c.parsed.y ?? 0))} /km · ${weekdayPaces[c.dataIndex].runs} runs`,
                },
              },
            },
            scales: {
              x: { grid: NO_GRID },
              y: paceScale({
                min: Math.min(...plotted.map((p) => p ?? NO_PACE_MIN)) - WEEKDAY_AXIS_MARGIN_MIN,
                max: Math.max(...plotted.map((p) => p ?? 0)) + WEEKDAY_AXIS_MARGIN_MIN,
              }),
            },
          }}
        />
      </div>
    </Card>
  );
}

/** Average pace by the part of the day the run started in. */
function TimeOfDayPaceChart({ timeOfDayPaces }: { timeOfDayPaces: TimeOfDayPace }) {
  return (
    <Card title="Pace by time of day">
      <div className="h-52">
        <Bar
          data={{
            labels: timeOfDayPaces.map((t) => t.label.split(" ")[0]),
            datasets: [
              {
                data: timeOfDayPaces.map((t) => toPlotMinutes(t.pace)),
                backgroundColor: alpha(COLORS.warn, 0.7),
                borderRadius: 5,
              },
            ],
          }}
          options={{
            ...BASE,
            plugins: {
              ...NO_LEGEND,
              tooltip: {
                callbacks: {
                  title: (c) => timeOfDayPaces[c[0].dataIndex].label,
                  label: (c) =>
                    `${fmtPace(minToSec(c.parsed.y ?? 0))} /km · ${timeOfDayPaces[c.dataIndex].runs} runs`,
                },
              },
            },
            scales: { x: { grid: NO_GRID }, y: paceScale() },
          }}
        />
      </div>
    </Card>
  );
}

/** The three small charts that share a row under the pace trend. */
export function PaceBreakdownCharts({
  histogram,
  weekdayPaces,
  timeOfDayPaces,
}: {
  histogram: Histogram;
  weekdayPaces: WeekdayPace;
  timeOfDayPaces: TimeOfDayPace;
}) {
  return (
    <div className="grid gap-4 lg:grid-cols-3">
      <DistanceHistogramChart histogram={histogram} />
      <WeekdayPaceChart weekdayPaces={weekdayPaces} />
      <TimeOfDayPaceChart timeOfDayPaces={timeOfDayPaces} />
    </div>
  );
}
