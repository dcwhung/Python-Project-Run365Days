import { useNavigate } from "react-router-dom";
import { Scatter } from "react-chartjs-2";
import type { Activity } from "@/data/types";
import { BASE, COLORS, GRID, NO_LEGEND, alpha } from "@/components/charts/theme";
import { Card } from "@/components/Card";
import { fmtPace, fmtShortDate } from "@/lib/format";
import { minToSec, paceToPlotMin } from "@/lib/units";
import { actTemp } from "@/lib/weather";

export function TempPaceChart({ activities }: { activities: Activity[] }) {
  const navigate = useNavigate();
  const pts = activities
    .filter((a) => actTemp(a) != null && a.paceSecPerKm)
    .map((a) => ({
      x: actTemp(a)!,
      y: paceToPlotMin(a.paceSecPerKm!),
      id: a.id,
      d: a.date,
    }));
  return (
    <Card title="Temperature vs Pace">
      <div className="h-56">
        <Scatter
          data={{
            datasets: [
              {
                data: pts,
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
                    const p = c.raw as (typeof pts)[number];
                    return `${fmtShortDate(p.d)} · ${p.x}°C · ${fmtPace(minToSec(p.y))}/km`;
                  },
                },
              },
            },
            onClick: (_e, els) => {
              if (els.length) navigate(`/activity/${pts[els[0].index].id}`);
            },
            scales: {
              x: { grid: GRID, title: { display: true, text: "Temperature (°C)" } },
              y: {
                grid: GRID,
                reverse: true,
                title: { display: true, text: "Pace (min/km)" },
                ticks: { callback: (v) => fmtPace(minToSec(Number(v))) },
              },
            },
          }}
        />
      </div>
    </Card>
  );
}
