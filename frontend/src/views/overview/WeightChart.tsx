import { Line } from "react-chartjs-2";
import type { DayDistance, WeightEntry } from "@/data/types";
import { BASE, COLORS, GRID, NO_GRID, dayAxis } from "@/components/charts/theme";
import { Card } from "@/components/Card";
import { fmtShortDate } from "@/lib/format";
import { toWeightUnit, usePrefs } from "@/lib/prefs";
import { rolling7 } from "./model";

export function WeightChart({ weight, daily }: { weight: WeightEntry[]; daily: DayDistance[] }) {
  const { weightUnit } = usePrefs();
  const labels = daily.map((d) => d.date);
  const byDate = new Map(weight.map((w) => [w.date, w.weightLbs]));
  return (
    <Card title="Weight vs 7-day Avg. Distance">
      <div className="h-56">
        <Line
          data={{
            labels,
            datasets: [
              { label: `Weight (${weightUnit})`, data: labels.map((d) => toWeightUnit(byDate.get(d), weightUnit)), borderColor: COLORS.warn, backgroundColor: "rgba(245,158,11,0.08)", tension: 0.3, fill: true, pointRadius: 0, borderWidth: 2, yAxisID: "y", spanGaps: true },
              { label: "7-day avg km", data: rolling7(daily), borderColor: COLORS.accent, backgroundColor: "transparent", tension: 0.3, borderDash: [5, 3], pointRadius: 0, borderWidth: 1.5, yAxisID: "y2" },
            ],
          }}
          options={{
            ...BASE,
            interaction: { mode: "index", intersect: false },
            plugins: {
              legend: { position: "top", labels: { boxWidth: 12, padding: 10 } },
              tooltip: { callbacks: { title: (c) => fmtShortDate(labels[c[0].dataIndex]) } },
            },
            scales: {
              x: dayAxis(labels),
              y: { grid: GRID, position: "left", ticks: { callback: (v) => `${v} ${weightUnit}` } },
              y2: { grid: NO_GRID, position: "right", ticks: { callback: (v) => `${v} km` } },
            },
          }}
        />
      </div>
    </Card>
  );
}
