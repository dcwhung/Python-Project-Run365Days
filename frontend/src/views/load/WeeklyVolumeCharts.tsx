import { Bar, Chart } from "react-chartjs-2";
import type { ChartData } from "chart.js";
import { fmtShortDate } from "@/lib/format";
import { WEEKDAYS } from "@/lib/dates";
import type { WeekWithActivities } from "@/lib/analytics";
import { BASE, COLORS, GRID, NO_GRID, NO_LEGEND, alpha } from "@/components/charts/theme";
import { Card } from "@/components/ui/Card";
import { weekChange } from "./model";

/** Weekly kilometres with the 4-week average over them; a bar picks that week. */
function WeeklyDistanceChart({
  weeks,
  fourWeekAvg,
  onSelectWeek,
}: {
  weeks: WeekWithActivities[];
  fourWeekAvg: number[];
  onSelectWeek: (week: WeekWithActivities) => void;
}) {
  return (
    <Card
      title="Weekly distance and 4-week average"
      action={<span className="text-[10px] text-muted">click a bar for that week</span>}
    >
      <div className="h-64">
        <Chart
          type="bar"
          data={
            {
              labels: weeks.map((w) => fmtShortDate(w.weekStart)),
              datasets: [
                {
                  type: "line",
                  label: "4-week avg",
                  data: fourWeekAvg,
                  borderColor: COLORS.warn,
                  borderWidth: 2,
                  pointRadius: 0,
                  tension: 0.3,
                  order: 0,
                },
                {
                  type: "bar",
                  label: "Week km",
                  data: weeks.map((w) => w.distanceKm),
                  backgroundColor: alpha(COLORS.accent, 0.7),
                  borderRadius: 3,
                  order: 1,
                },
              ],
            } as ChartData<"bar" | "line">
          }
          options={{
            ...BASE,
            interaction: { mode: "index", intersect: false },
            onClick: (_e, els) => {
              if (els.length) onSelectWeek(weeks[els[0].index]);
            },
            plugins: {
              legend: { position: "top", labels: { boxWidth: 10, padding: 10 } },
              tooltip: {
                callbacks: {
                  title: (c) => `week of ${c[0].label}`,
                  afterBody: (c) => {
                    const i = c[0].dataIndex;
                    const ch = weekChange(weeks, i);
                    return ch == null
                      ? `${weeks[i].runs} runs`
                      : `${ch.toFixed(0)}% vs previous week · ${weeks[i].runs} runs`;
                  },
                },
              },
            },
            scales: {
              x: { grid: NO_GRID, ticks: { maxTicksLimit: 12, maxRotation: 0 } },
              y: { grid: GRID, ticks: { callback: (v) => `${v} km` } },
            },
          }}
        />
      </div>
    </Card>
  );
}

/** Mean kilometres per run for each day of the week. */
function WeekdayDistanceChart({ weekdayKm }: { weekdayKm: number[] }) {
  return (
    <Card title="Average distance by weekday">
      <div className="h-64">
        <Bar
          data={{
            labels: WEEKDAYS,
            datasets: [
              { data: weekdayKm, backgroundColor: alpha(COLORS.violet, 0.7), borderRadius: 5 },
            ],
          }}
          options={{
            ...BASE,
            plugins: {
              ...NO_LEGEND,
              tooltip: { callbacks: { label: (c) => `${c.parsed.y} km per run` } },
            },
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

/** The two volume charts that share a row under the fitness curve. */
export function WeeklyVolumeCharts({
  weeks,
  fourWeekAvg,
  weekdayKm,
  onSelectWeek,
}: {
  weeks: WeekWithActivities[];
  fourWeekAvg: number[];
  weekdayKm: number[];
  onSelectWeek: (week: WeekWithActivities) => void;
}) {
  return (
    <div className="grid gap-4 lg:grid-cols-[3fr_2fr]">
      <WeeklyDistanceChart weeks={weeks} fourWeekAvg={fourWeekAvg} onSelectWeek={onSelectWeek} />
      <WeekdayDistanceChart weekdayKm={weekdayKm} />
    </div>
  );
}
