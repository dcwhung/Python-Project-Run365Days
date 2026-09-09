import { Line } from "react-chartjs-2";
import type { TrainingLoadPoint } from "@/data/types";
import { BASE, COLORS, GRID, dayAxis } from "@/components/charts/theme";
import { Card } from "@/components/Card";

export function TrainingLoadChart({ load }: { load: TrainingLoadPoint[] }) {
  const labels = load.map((p) => p.date);
  return (
    <Card title="Training Load (42-day fitness vs 7-day fatigue)">
      <div className="h-56">
        <Line
          data={{
            labels,
            datasets: [
              { label: "Fitness (CTL)", data: load.map((p) => p.ctl), borderColor: COLORS.accent, backgroundColor: "rgba(79,142,247,0.1)", tension: 0.3, fill: true, pointRadius: 0, borderWidth: 1.5 },
              { label: "Fatigue (ATL)", data: load.map((p) => p.atl), borderColor: COLORS.danger, backgroundColor: "transparent", tension: 0.3, borderDash: [4, 2], pointRadius: 0, borderWidth: 1.5 },
            ],
          }}
          options={{
            ...BASE,
            interaction: { mode: "index", intersect: false },
            plugins: { legend: { position: "top", labels: { boxWidth: 10, padding: 8, font: { size: 11 } } } },
            scales: { x: dayAxis(labels), y: { grid: GRID, ticks: { font: { size: 10 } } } },
          }}
        />
      </div>
    </Card>
  );
}
