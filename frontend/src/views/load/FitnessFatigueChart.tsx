import { Line } from "react-chartjs-2";
import type { TrainingLoadPoint } from "@/data/types";
import { fmtShortDate, fmtSigned } from "@/lib/format";
import { BASE, COLORS, GRID, NO_GRID, alpha, dayAxis } from "@/components/charts/theme";
import { Card } from "@/components/ui/Card";

/** CTL, ATL and TSB over the year; TSB rides its own right-hand axis. */
export function FitnessFatigueChart({
  trainingLoad,
  labels,
}: {
  trainingLoad: TrainingLoadPoint[];
  labels: string[];
}) {
  return (
    <Card title="Fitness, fatigue and form">
      <div className="h-64">
        <Line
          data={{
            labels,
            datasets: [
              {
                label: "Fitness (CTL)",
                data: trainingLoad.map((p) => p.ctl),
                borderColor: COLORS.accent,
                backgroundColor: alpha(COLORS.accent, 0.1),
                fill: true,
                borderWidth: 2,
                pointRadius: 0,
                tension: 0.3,
                yAxisID: "y",
              },
              {
                label: "Fatigue (ATL)",
                data: trainingLoad.map((p) => p.atl),
                borderColor: COLORS.danger,
                borderWidth: 1.5,
                borderDash: [4, 2],
                pointRadius: 0,
                tension: 0.3,
                yAxisID: "y",
              },
              {
                label: "Form (TSB)",
                data: trainingLoad.map((p) => p.tsb),
                borderColor: COLORS.accent2,
                borderWidth: 1.5,
                pointRadius: 0,
                tension: 0.3,
                yAxisID: "y2",
              },
            ],
          }}
          options={{
            ...BASE,
            interaction: { mode: "index", intersect: false },
            plugins: {
              legend: { position: "top", labels: { boxWidth: 10, padding: 10 } },
              tooltip: { callbacks: { title: (c) => fmtShortDate(labels[c[0].dataIndex]) } },
            },
            scales: {
              x: dayAxis(labels),
              y: { grid: GRID, position: "left", ticks: { callback: (v) => `${v} km` } },
              y2: {
                grid: NO_GRID,
                position: "right",
                ticks: { callback: (v) => fmtSigned(Number(v)) },
              },
            },
          }}
        />
      </div>
    </Card>
  );
}
