import { Bar, Line } from "react-chartjs-2";
import type { MonthSummary } from "@/data/types";
import { MONTHS, fmtPace } from "@/lib/format";
import { BASE, COLORS, GRID, NO_GRID, NO_LEGEND } from "@/components/charts/theme";
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
                backgroundColor: "rgba(79,142,247,0.7)",
                borderRadius: 5,
              },
            ],
          }}
          options={{
            ...BASE,
            plugins: NO_LEGEND,
            scales: { x: { grid: NO_GRID }, y: { grid: GRID, ticks: { callback: (v) => `${v} km` } } },
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
                data: monthly.map((m) => (m.avgPaceSecPerKm ? m.avgPaceSecPerKm / 60 : null)),
                borderColor: COLORS.accent2,
                backgroundColor: "rgba(52,211,153,0.1)",
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
              tooltip: { callbacks: { label: (c) => `${fmtPace((c.parsed.y ?? 0) * 60)} /km` } },
            },
            scales: {
              x: { grid: NO_GRID },
              y: { grid: GRID, reverse: true, ticks: { callback: (v) => fmtPace(Number(v) * 60) } },
            },
          }}
        />
      </div>
    </Card>
  );
}
