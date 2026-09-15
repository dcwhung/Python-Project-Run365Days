import { Line } from "react-chartjs-2";
import { fmtShortDate } from "@/lib/format";
import { BASE, COLORS, GRID, alpha, dayAxis } from "@/components/charts/theme";
import { Card } from "@/components/ui/Card";
import type { temperatureRange } from "./model";

/** The HKO daily min/max band with the Garmin run temperature drawn over it. */
export function TemperatureRangeChart({
  range,
  labels,
}: {
  range: ReturnType<typeof temperatureRange>;
  labels: string[];
}) {
  return (
    <Card title="HKO daily range and temperature during the run">
      <div className="h-64">
        <Line
          data={{
            labels,
            datasets: [
              {
                label: "HKO max",
                data: range.map((r) => r.max),
                borderColor: alpha(COLORS.danger, 0.5),
                backgroundColor: alpha(COLORS.danger, 0.1),
                borderWidth: 1,
                pointRadius: 0,
                fill: "+1",
                spanGaps: true,
              },
              {
                label: "HKO min",
                data: range.map((r) => r.min),
                borderColor: alpha(COLORS.blue, 0.5),
                borderWidth: 1,
                pointRadius: 0,
                spanGaps: true,
              },
              {
                label: "During run (Garmin)",
                data: range.map((r) => r.run),
                borderColor: COLORS.warn,
                borderWidth: 2,
                pointRadius: 0,
                tension: 0.2,
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
                  label: (c) => `${c.dataset.label}: ${c.parsed.y} °C`,
                },
              },
            },
            scales: {
              x: dayAxis(labels),
              y: { grid: GRID, ticks: { callback: (v) => `${v} °C` } },
            },
          }}
        />
      </div>
    </Card>
  );
}
