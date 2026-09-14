import { Bar, Line } from "react-chartjs-2";
import type { MonthSummary } from "@/data/types";
import { MONTHS, fmtPace } from "@/lib/format";
import { minToSec, secToMin } from "@/lib/units";
import { BASE, COLORS, GRID, NO_GRID, NO_LEGEND, alpha } from "@/components/charts/theme";
import { Card } from "@/components/Card";

export function MonthlyDistanceChart({ monthly }: { monthly: MonthSummary[] }) {
  return (
    <Card title="Monthly Distance">
      <div className="h-56">
        <Bar
          data={{
            labels: MONTHS,
            datasets: [
              {
                data: monthly.map((m) => Math.round(m.distanceKm * 10) / 10),
                backgroundColor: alpha(COLORS.accent, 0.7),
                borderRadius: 5,
              },
            ],
          }}
          options={{
            ...BASE,
            plugins: NO_LEGEND,
            scales: {
              x: { grid: NO_GRID },
              y: { grid: GRID, ticks: { callback: (v) => `${v} km` } },
            },
          }}
        />
      </div>
    </Card>
  );
}

export function MonthlyPaceChart({ monthly }: { monthly: MonthSummary[] }) {
  return (
    <Card title="Monthly Avg. Pace">
      <div className="h-56">
        <Line
          data={{
            labels: MONTHS,
            datasets: [
              {
                data: monthly.map((m) => (m.avgPaceSecPerKm ? secToMin(m.avgPaceSecPerKm) : null)),
                borderColor: COLORS.accent2,
                backgroundColor: alpha(COLORS.accent2, 0.1),
                tension: 0.4,
                fill: true,
                pointRadius: 4,
                pointBackgroundColor: COLORS.accent2,
              },
            ],
          }}
          options={{
            ...BASE,
            plugins: {
              ...NO_LEGEND,
              tooltip: { callbacks: { label: (c) => `${fmtPace(minToSec(c.parsed.y ?? 0))} /km` } },
            },
            scales: {
              x: { grid: NO_GRID },
              y: {
                grid: GRID,
                reverse: true,
                ticks: { callback: (v) => fmtPace(minToSec(Number(v))) },
              },
            },
          }}
        />
      </div>
    </Card>
  );
}
