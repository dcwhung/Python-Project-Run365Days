import { Line } from "react-chartjs-2";
import { useActivities, useWeight, useYear } from "@/data/hooks";
import { MONTHS, fmtShortDate } from "@/lib/format";
import { secToHours, secToMin } from "@/lib/units";
import { toWeightUnit, usePrefs } from "@/lib/prefs";
import { BASE, COLORS, GRID, NO_LEGEND, alpha, dayAxis } from "@/components/charts/theme";
import { Card } from "@/components/ui/Card";
import { DiamondGrid } from "./DiamondGrid";
import { monthlyHours, weightReview } from "./model";

function Big({
  value,
  unit,
  label,
  color = "text-text",
}: {
  value: string;
  unit?: string;
  label: string;
  color?: string;
}) {
  return (
    <div>
      <div className={`text-4xl font-bold tabular-nums ${color}`}>
        {value}
        {unit && <span className="ml-1 text-base font-medium text-muted">{unit}</span>}
      </div>
      <div className="text-xs uppercase tracking-wide text-muted">{label}</div>
    </div>
  );
}

export function YearView() {
  const yearQuery = useYear();
  const activitiesQuery = useActivities();
  const weightQuery = useWeight();
  const { weightUnit } = usePrefs();
  if (yearQuery.isPending || activitiesQuery.isPending)
    return <p className="text-muted">Loading…</p>;
  if (yearQuery.isError)
    return <p className="text-danger">Could not load data: {yearQuery.error.message}</p>;
  const year = yearQuery.data;
  const totals = year.totals;
  const hours = monthlyHours(activitiesQuery.data ?? []);
  const peakMonth = hours.indexOf(Math.max(...hours));
  const weightSummary = weightReview(weightQuery.data ?? []);
  const labels = year.dailyDistance.map((d) => d.date);
  const byDate = new Map((weightQuery.data ?? []).map((e) => [e.date, e.weightLbs]));

  return (
    <div className="space-y-4" data-testid="year-view">
      <div className="rounded-card border border-warn/40 bg-gradient-to-br from-surface to-bg p-6">
        <div className="text-xs uppercase tracking-[0.3em] text-warn">
          #365DaysChallenge · {year.year}
        </div>
        <h1 className="mt-1 text-3xl font-bold">Year in Review</h1>
        <div className="mt-6 grid gap-6 md:grid-cols-4">
          <Big
            value={String(totals.activeDays)}
            unit={`/ ${totals.days}`}
            label="Days run"
            color="text-violet"
          />
          <Big
            value={totals.distanceKm.toLocaleString(undefined, { maximumFractionDigits: 1 })}
            unit="km"
            label={`${(totals.distanceKm / totals.days).toFixed(1)} km/day · ${(totals.distanceKm / (totals.days / 7)).toFixed(1)} km/week`}
            color="text-accent"
          />
          <Big
            value={secToHours(totals.durationSec).toFixed(1)}
            unit="hrs"
            label={`${Math.round(secToMin(totals.durationSec) / totals.days)} min/day`}
            color="text-accent2"
          />
          <Big
            value={totals.calories.toLocaleString()}
            unit="kcal"
            label={`${(totals.calories / totals.days).toFixed(1)} kcal/day`}
            color="text-danger"
          />
        </div>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card title="Hours per month">
          <div className="space-y-2" data-testid="stairs">
            {[0, 4, 8].map((first, row) => (
              <div key={first} className="flex gap-2" style={{ marginLeft: row * 28 }}>
                {MONTHS.slice(first, first + 4).map((month, i) => (
                  <div
                    key={month}
                    className={`flex-1 rounded border p-2 text-center ${first + i === peakMonth ? "border-warn bg-warn/10" : "border-border bg-surface2"}`}
                  >
                    <div className="text-[10px] text-muted">{month.toUpperCase()}</div>
                    <div className="text-lg font-semibold">{hours[first + i].toFixed(1)}</div>
                  </div>
                ))}
              </div>
            ))}
          </div>
        </Card>
        <Card title="Every day of the year">
          <DiamondGrid daily={year.dailyDistance} year={year.year} />
        </Card>
      </div>

      {weightSummary && (
        <Card title="Weight">
          <div className="mb-3 grid grid-cols-2 gap-4 md:grid-cols-4" data-testid="year-weight">
            <Big
              value={String(toWeightUnit(weightSummary.first.weightLbs, weightUnit))}
              unit={weightUnit}
              label={`start · ${fmtShortDate(weightSummary.first.date)}`}
            />
            <Big
              value={String(toWeightUnit(weightSummary.last.weightLbs, weightUnit))}
              unit={weightUnit}
              label={`end · ${fmtShortDate(weightSummary.last.date)}`}
              color="text-warn"
            />
            <Big
              value={String(toWeightUnit(weightSummary.min.weightLbs, weightUnit))}
              unit={weightUnit}
              label={`lowest · ${fmtShortDate(weightSummary.min.date)}`}
              color="text-accent2"
            />
            <Big
              value={weightSummary.lossPct.toFixed(1)}
              unit="%"
              label="lost over the year"
              color="text-accent"
            />
          </div>
          <div className="h-48">
            <Line
              data={{
                labels,
                datasets: [
                  {
                    data: labels.map((d) => toWeightUnit(byDate.get(d), weightUnit)),
                    borderColor: COLORS.warn,
                    backgroundColor: alpha(COLORS.warn, 0.08),
                    borderWidth: 2,
                    tension: 0.3,
                    fill: true,
                    pointRadius: 0,
                    spanGaps: true,
                  },
                ],
              }}
              options={{
                ...BASE,
                animation: false,
                interaction: { mode: "index", intersect: false },
                plugins: {
                  ...NO_LEGEND,
                  tooltip: {
                    callbacks: {
                      title: (c) => fmtShortDate(labels[c[0].dataIndex]),
                      label: (c) => `${c.parsed.y} ${weightUnit}`,
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
      )}
    </div>
  );
}
