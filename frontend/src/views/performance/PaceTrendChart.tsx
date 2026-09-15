import { useNavigate } from "react-router-dom";
import { Line } from "react-chartjs-2";
import type { Activity } from "@/data/types";
import { fmtPace, fmtShortDate } from "@/lib/format";
import { minToSec } from "@/lib/units";
import { BASE, COLORS, alpha, dayAxis } from "@/components/charts/theme";
import { Card } from "@/components/ui/Card";
import { paceScale } from "./paceAxis";

/**
 * A dot per run plus the 30-day trend line. Clicking a dot -- and only a dot,
 * never the trend -- opens that run.
 */
export function PaceTrendChart({
  labels,
  points,
  trend,
  dayActivities,
}: {
  labels: string[];
  points: (number | null)[];
  trend: (number | null)[];
  /** Same length as `labels`: the run on that day, or null for a rest day. */
  dayActivities: (Activity | null)[];
}) {
  const navigate = useNavigate();
  return (
    <Card title="Pace per run and 30-day trend">
      <div className="h-64">
        <Line
          data={{
            labels,
            datasets: [
              {
                label: "Run",
                data: points,
                showLine: false,
                pointRadius: 3,
                pointBackgroundColor: alpha(COLORS.accent, 0.55),
                pointBorderWidth: 0,
              },
              {
                label: "30-day trend",
                data: trend,
                borderColor: COLORS.accent2,
                borderWidth: 2,
                pointRadius: 0,
                tension: 0.3,
                spanGaps: true,
              },
            ],
          }}
          options={{
            ...BASE,
            interaction: { mode: "nearest", intersect: true },
            onClick: (_e, els) => {
              const el = els.find((x) => x.datasetIndex === 0);
              const a = el && dayActivities[el.index];
              if (a) navigate(`/activity/${a.id}`);
            },
            plugins: {
              legend: { position: "top", labels: { boxWidth: 10, padding: 10 } },
              tooltip: {
                callbacks: {
                  title: (c) => fmtShortDate(labels[c[0].dataIndex]),
                  label: (c) => `${c.dataset.label}: ${fmtPace(minToSec(c.parsed.y ?? 0))} /km`,
                },
              },
            },
            scales: { x: dayAxis(labels), y: paceScale() },
          }}
        />
      </div>
    </Card>
  );
}
