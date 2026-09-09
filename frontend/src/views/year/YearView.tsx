import { Line } from "react-chartjs-2";
import { useActivities, useWeight, useYear } from "@/data/hooks";
import { MONTHS, fmtShortDate } from "@/lib/format";
import { toWeightUnit, usePrefs } from "@/lib/prefs";
import { BASE, COLORS, GRID, NO_LEGEND, dayAxis } from "@/components/charts/theme";
import { Card } from "@/components/Card";
import { DiamondGrid } from "./DiamondGrid";
import { monthlyHours, weightReview } from "./model";

function Big({ value, unit, label, color = "text-text" }: { value: string; unit?: string; label: string; color?: string }) {
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
  const year = useYear();
  const activities = useActivities();
  const weight = useWeight();
  const { weightUnit } = usePrefs();
  if (year.isPending || activities.isPending) return <p className="text-muted">Loading…</p>;
  if (year.isError) return <p className="text-danger">Could not load data: {year.error.message}</p>;
  const y = year.data;
  const t = y.totals;
  const hours = monthlyHours(activities.data ?? []);
  const hi = hours.indexOf(Math.max(...hours));
  const w = weightReview(weight.data ?? []);
  const labels = y.dailyDistance.map((d) => d.date);
  const byDate = new Map((weight.data ?? []).map((e) => [e.date, e.weightLbs]));

  return (
    <div className="space-y-4" data-testid="year-view">
      <div className="rounded-card border border-warn/40 bg-gradient-to-br from-surface to-bg p-6">
        <div className="text-xs uppercase tracking-[0.3em] text-warn">#365DaysChallenge · {y.year}</div>
        <h1 className="mt-1 text-3xl font-bold">Year in Review</h1>
        <div className="mt-6 grid gap-6 md:grid-cols-4">
          <Big value={String(t.activeDays)} unit={`/ ${t.days}`} label="Days run" color="text-violet" />
          <Big value={t.distanceKm.toLocaleString(undefined, { maximumFractionDigits: 1 })} unit="km" label={`${(t.distanceKm / t.days).toFixed(1)} km/day · ${(t.distanceKm / (t.days / 7)).toFixed(1)} km/week`} color="text-accent" />
          <Big value={(t.durationSec / 3600).toFixed(1)} unit="hrs" label={`${Math.round(t.durationSec / 60 / t.days)} min/day`} color="text-accent2" />
          <Big value={t.calories.toLocaleString()} unit="kcal" label={`${(t.calories / t.days).toFixed(1)} kcal/day`} color="text-danger" />
        </div>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card title="Hours per month">
          <div className="space-y-2" data-testid="stairs">
            {[0, 4, 8].map((s, r) => (
              <div key={s} className="flex gap-2" style={{ marginLeft: r * 28 }}>
                {MONTHS.slice(s, s + 4).map((m, i) => (
                  <div key={m} className={`flex-1 rounded border p-2 text-center ${s + i === hi ? "border-warn bg-warn/10" : "border-border bg-surface2"}`}>
                    <div className="text-[10px] text-muted">{m.toUpperCase()}</div>
                    <div className="text-lg font-semibold">{hours[s + i].toFixed(1)}</div>
                  </div>
                ))}
              </div>
            ))}
          </div>
        </Card>
        <Card title="Every day of the year">
          <DiamondGrid daily={y.dailyDistance} year={y.year} />
        </Card>
      </div>

      {w && (
        <Card title="Weight">
          <div className="mb-3 grid grid-cols-2 gap-4 md:grid-cols-4" data-testid="year-weight">
            <Big value={String(toWeightUnit(w.first.weightLbs, weightUnit))} unit={weightUnit} label={`start · ${fmtShortDate(w.first.date)}`} />
            <Big value={String(toWeightUnit(w.last.weightLbs, weightUnit))} unit={weightUnit} label={`end · ${fmtShortDate(w.last.date)}`} color="text-warn" />
            <Big value={String(toWeightUnit(w.min.weightLbs, weightUnit))} unit={weightUnit} label={`lowest · ${fmtShortDate(w.min.date)}`} color="text-accent2" />
            <Big value={w.lossPct.toFixed(1)} unit="%" label="lost over the year" color="text-accent" />
          </div>
          <div className="h-48">
            <Line
              data={{ labels, datasets: [{ data: labels.map((d) => toWeightUnit(byDate.get(d), weightUnit)), borderColor: COLORS.warn, backgroundColor: "rgba(245,158,11,.08)", borderWidth: 2, tension: 0.3, fill: true, pointRadius: 0, spanGaps: true }] }}
              options={{ ...BASE, animation: false, interaction: { mode: "index", intersect: false }, plugins: { ...NO_LEGEND, tooltip: { callbacks: { title: (c) => fmtShortDate(labels[c[0].dataIndex]), label: (c) => `${c.parsed.y} ${weightUnit}` } } }, scales: { x: dayAxis(labels), y: { grid: GRID, ticks: { callback: (v) => `${v} ${weightUnit}` } } } }}
            />
          </div>
        </Card>
      )}
    </div>
  );
}
