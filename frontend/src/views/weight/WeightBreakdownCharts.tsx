import { Bar, Scatter } from "react-chartjs-2";
import { MONTHS, fmtShortDate, fmtSigned } from "@/lib/format";
import { WEEKDAYS } from "@/lib/dates";
import type { Prefs } from "@/lib/prefs";
import { BASE, COLORS, GRID, NO_GRID, NO_LEGEND, alpha } from "@/components/charts/theme";
import { Card } from "@/components/ui/Card";
import type { weeklyKmVsChange } from "./model";

/** Green for a loss or no change, red for a gain -- the only colour signal here. */
const upDown = (v: number | null) =>
  (v ?? 0) <= 0 ? alpha(COLORS.accent2, 0.7) : alpha(COLORS.danger, 0.7);

type WeeklyPoint = ReturnType<typeof weeklyKmVsChange>[number];

/** Month-end weight against the previous month end. */
function MonthlyChangeChart({
  monthlyChanges,
  weightUnit,
}: {
  monthlyChanges: (number | null)[];
  weightUnit: Prefs["weightUnit"];
}) {
  return (
    <Card title="Change by month (month end vs previous)">
      <div className="h-52">
        <Bar
          data={{
            labels: MONTHS,
            datasets: [
              {
                data: monthlyChanges,
                backgroundColor: monthlyChanges.map(upDown),
                borderRadius: 5,
              },
            ],
          }}
          options={{
            ...BASE,
            plugins: {
              ...NO_LEGEND,
              tooltip: {
                callbacks: { label: (c) => `${fmtSigned(c.parsed.y ?? 0)} ${weightUnit}` },
              },
            },
            scales: {
              x: { grid: NO_GRID },
              y: { grid: GRID, ticks: { callback: (v) => `${v} ${weightUnit}` } },
            },
          }}
        />
      </div>
    </Card>
  );
}

/** Mean overnight change, grouped by the day of the week it landed on. */
function WeekdayChangeChart({
  weekdayDeltas,
  weightUnit,
}: {
  weekdayDeltas: (number | null)[];
  weightUnit: Prefs["weightUnit"];
}) {
  return (
    <Card title="Day-to-day change by weekday">
      <div className="h-52">
        <Bar
          data={{
            labels: WEEKDAYS,
            datasets: [
              { data: weekdayDeltas, backgroundColor: weekdayDeltas.map(upDown), borderRadius: 5 },
            ],
          }}
          options={{
            ...BASE,
            plugins: {
              ...NO_LEGEND,
              tooltip: {
                callbacks: {
                  label: (c) => `${fmtSigned(c.parsed.y ?? 0)} ${weightUnit} vs previous day`,
                },
              },
            },
            scales: {
              x: { grid: NO_GRID },
              y: { grid: GRID, ticks: { callback: (v) => `${v} ${weightUnit}` } },
            },
          }}
        />
      </div>
    </Card>
  );
}

/** One dot per week: kilometres run against the weight change that week. */
function WeeklyKmVsChangeChart({
  weeklyPoints,
  weightUnit,
}: {
  weeklyPoints: WeeklyPoint[];
  weightUnit: Prefs["weightUnit"];
}) {
  return (
    <Card title="Weekly km vs weight change">
      <div className="h-52">
        <Scatter
          data={{
            datasets: [
              {
                data: weeklyPoints,
                backgroundColor: alpha(COLORS.accent, 0.5),
                pointRadius: 4,
                pointHoverRadius: 6,
              },
            ],
          }}
          options={{
            ...BASE,
            plugins: {
              ...NO_LEGEND,
              tooltip: {
                callbacks: {
                  label: (c) => {
                    const p = c.raw as WeeklyPoint;
                    return `week of ${fmtShortDate(p.weekStart)} · ${p.x} km · ${fmtSigned(p.y)} ${weightUnit}`;
                  },
                },
              },
            },
            scales: {
              x: { grid: GRID, title: { display: true, text: "km that week" } },
              y: { grid: GRID, title: { display: true, text: `weight change (${weightUnit})` } },
            },
          }}
        />
      </div>
    </Card>
  );
}

/** The three small charts that share a row under the weight curve. */
export function WeightBreakdownCharts({
  monthlyChanges,
  weekdayDeltas,
  weeklyPoints,
  weightUnit,
}: {
  monthlyChanges: (number | null)[];
  weekdayDeltas: (number | null)[];
  weeklyPoints: WeeklyPoint[];
  weightUnit: Prefs["weightUnit"];
}) {
  return (
    <div className="grid gap-4 lg:grid-cols-3">
      <MonthlyChangeChart monthlyChanges={monthlyChanges} weightUnit={weightUnit} />
      <WeekdayChangeChart weekdayDeltas={weekdayDeltas} weightUnit={weightUnit} />
      <WeeklyKmVsChangeChart weeklyPoints={weeklyPoints} weightUnit={weightUnit} />
    </div>
  );
}
