import { Line } from "react-chartjs-2";
import { fmtShortDate } from "@/lib/format";
import type { Prefs } from "@/lib/prefs";
import { BASE, COLORS, GRID, alpha, dayAxis } from "@/components/charts/theme";
import { Card } from "@/components/ui/Card";

/**
 * Every weigh-in of the year with the 7-day average over it. Both series
 * arrive already converted to the display unit.
 */
export function WeightTrendChart({
  labels,
  daily,
  movingAvg7,
  weightUnit,
}: {
  labels: string[];
  daily: (number | null)[];
  movingAvg7: (number | null)[];
  weightUnit: Prefs["weightUnit"];
}) {
  return (
    <Card title="Daily weight and 7-day average">
      <div className="h-64">
        <Line
          data={{
            labels,
            datasets: [
              {
                label: `Daily (${weightUnit})`,
                data: daily,
                borderColor: alpha(COLORS.warn, 0.45),
                borderWidth: 1,
                pointRadius: 0,
                spanGaps: true,
              },
              {
                label: "7-day average",
                data: movingAvg7,
                borderColor: COLORS.warn,
                backgroundColor: alpha(COLORS.warn, 0.08),
                fill: true,
                borderWidth: 2.5,
                pointRadius: 0,
                tension: 0.3,
                spanGaps: true,
              },
            ],
          }}
          options={{
            ...BASE,
            interaction: { mode: "index", intersect: false },
            plugins: {
              legend: { position: "top", labels: { boxWidth: 10, padding: 10 } },
              tooltip: {
                callbacks: {
                  title: (c) => fmtShortDate(labels[c[0].dataIndex]),
                  label: (c) => `${c.dataset.label}: ${c.parsed.y} ${weightUnit}`,
                },
              },
            },
            scales: {
              x: dayAxis(labels),
              y: { grid: GRID, ticks: { callback: (v) => `${v} ${weightUnit}` } },
            },
          }}
        />
      </div>
    </Card>
  );
}
