import { Bar, Line, Scatter } from "react-chartjs-2";
import { useWeight, useYear } from "@/data/hooks";
import { MONTHS, fmtShortDate, fmtSigned } from "@/lib/format";
import { WEEKDAYS } from "@/lib/dates";
import { toWeightUnit, usePrefs } from "@/lib/prefs";
import { BASE, COLORS, GRID, NO_GRID, NO_LEGEND, alpha, dayAxis } from "@/components/charts/theme";
import { Card } from "@/components/ui/Card";
import { KpiCard } from "@/components/ui/KpiCard";
import { DataTable } from "@/components/ui/DataTable";
import {
  dailyDeltas,
  dailySeries,
  lastOfMonth,
  monthlyChange,
  monthlyUpDown,
  weekdayDelta,
  weeklyKmVsChange,
  weightKpis,
} from "./model";

const upDown = (v: number | null) =>
  (v ?? 0) <= 0 ? alpha(COLORS.accent2, 0.7) : alpha(COLORS.danger, 0.7);

export function WeightView() {
  const yearQuery = useYear();
  const weightQuery = useWeight();
  const { weightUnit, heightCm } = usePrefs();
  if (yearQuery.isPending || weightQuery.isPending) return <p className="text-muted">Loading…</p>;
  if (yearQuery.isError || weightQuery.isError)
    return <p className="text-danger">Could not load data.</p>;
  const year = yearQuery.data;
  const weight = weightQuery.data;
  const kpis = weightKpis(weight, heightCm, year.totals.days);
  if (!kpis)
    return (
      <Card title="Weight">
        <p className="text-muted">No weight data.</p>
      </Card>
    );
  const labels = year.dailyDistance.map((d) => d.date);
  const { series, ma7: movingAvg7 } = dailySeries(weight, labels);
  const toDisplayUnit = (v: number | null) => toWeightUnit(v, weightUnit);
  const monthlyChanges = monthlyChange(weight).map((v) => (v == null ? null : toDisplayUnit(v)));
  const deltas = dailyDeltas(weight);
  const weekdayDeltas = weekdayDelta(deltas).map((v) =>
    v == null ? null : Math.round(toDisplayUnit(v)! * 100) / 100,
  );
  const weeklyPoints = weeklyKmVsChange(year.weekly, weight, year.dailyDistance, year.year).map(
    (p) => ({
      ...p,
      y: toDisplayUnit(p.y)!,
    }),
  );
  const monthEndWeights = lastOfMonth(weight);

  return (
    <div className="space-y-4" data-testid="weight-view">
      <h1 className="text-lg font-semibold">
        Weight{" "}
        <span className="text-sm font-normal text-muted">· height {heightCm} cm (Settings)</span>
      </h1>
      <div
        className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6"
        data-testid="weight-kpis"
      >
        <KpiCard
          label="Start"
          value={`${toDisplayUnit(kpis.first.weightLbs)}`}
          unit={weightUnit}
          sub={fmtShortDate(kpis.first.date)}
          accent="accent"
        />
        <KpiCard
          label="End"
          value={`${toDisplayUnit(kpis.last.weightLbs)}`}
          unit={weightUnit}
          sub={fmtShortDate(kpis.last.date)}
          accent="warn"
        />
        <KpiCard
          label="Lowest"
          value={`${toDisplayUnit(kpis.min.weightLbs)}`}
          unit={weightUnit}
          sub={fmtShortDate(kpis.min.date)}
          accent="accent2"
        />
        <KpiCard
          label="Total loss"
          value={`${toDisplayUnit(kpis.loss)}`}
          unit={weightUnit}
          sub={`${kpis.lossPct.toFixed(1)}% · ${(toDisplayUnit(kpis.lossPerWeek) ?? 0).toFixed(2)} ${weightUnit}/week`}
          accent="accent"
        />
        <KpiCard
          label="BMI"
          value={kpis.bmiEnd.toFixed(1)}
          sub={`from ${kpis.bmiStart.toFixed(1)} at start`}
          accent="violet"
        />
        <KpiCard
          label="Weigh-ins"
          value={String(kpis.weighIns)}
          sub={kpis.missing === 0 ? "every day" : `${kpis.missing} days missing`}
          accent="danger"
        />
      </div>

      <Card title="Daily weight and 7-day average">
        <div className="h-64">
          <Line
            data={{
              labels,
              datasets: [
                {
                  label: `Daily (${weightUnit})`,
                  data: series.map(toDisplayUnit),
                  borderColor: alpha(COLORS.warn, 0.45),
                  borderWidth: 1,
                  pointRadius: 0,
                  spanGaps: true,
                },
                {
                  label: "7-day average",
                  data: movingAvg7.map(toDisplayUnit),
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

      <div className="grid gap-4 lg:grid-cols-3">
        <Card title="Change by month (month end vs previous)">
          <div className="h-52">
            <Bar
              data={{
                labels: MONTHS,
                datasets: [
                  {
                    data: monthlyChanges,
                    backgroundColor: monthlyChanges.map(upDown),
                    borderRadius: 5,
                  },
                ],
              }}
              options={{
                ...BASE,
                plugins: {
                  ...NO_LEGEND,
                  tooltip: {
                    callbacks: { label: (c) => `${fmtSigned(c.parsed.y ?? 0)} ${weightUnit}` },
                  },
                },
                scales: {
                  x: { grid: NO_GRID },
                  y: { grid: GRID, ticks: { callback: (v) => `${v} ${weightUnit}` } },
                },
              }}
            />
          </div>
        </Card>
        <Card title="Day-to-day change by weekday">
          <div className="h-52">
            <Bar
              data={{
                labels: WEEKDAYS,
                datasets: [
                  {
                    data: weekdayDeltas,
                    backgroundColor: weekdayDeltas.map(upDown),
                    borderRadius: 5,
                  },
                ],
              }}
              options={{
                ...BASE,
                plugins: {
                  ...NO_LEGEND,
                  tooltip: {
                    callbacks: {
                      label: (c) => `${fmtSigned(c.parsed.y ?? 0)} ${weightUnit} vs previous day`,
                    },
                  },
                },
                scales: {
                  x: { grid: NO_GRID },
                  y: { grid: GRID, ticks: { callback: (v) => `${v} ${weightUnit}` } },
                },
              }}
            />
          </div>
        </Card>
        <Card title="Weekly km vs weight change">
          <div className="h-52">
            <Scatter
              data={{
                datasets: [
                  {
                    data: weeklyPoints,
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
                        const p = c.raw as (typeof weeklyPoints)[number];
                        return `week of ${fmtShortDate(p.weekStart)} · ${p.x} km · ${fmtSigned(p.y)} ${weightUnit}`;
                      },
                    },
                  },
                },
                scales: {
                  x: { grid: GRID, title: { display: true, text: "km that week" } },
                  y: {
                    grid: GRID,
                    title: { display: true, text: `weight change (${weightUnit})` },
                  },
                },
              }}
            />
          </div>
        </Card>
      </div>

      <Card title="Up, down or unchanged days per month">
        <DataTable
          testId="weight-table"
          cols={[
            { h: "Month" },
            { h: "Up", num: true },
            { h: "Down", num: true },
            { h: "Same", num: true },
            { h: `Net (${weightUnit})`, num: true },
            { h: "Month end", num: true },
          ]}
          rows={monthlyUpDown(deltas, weight).map((r) => ({
            key: String(r.month),
            c: [
              MONTHS[r.month - 1],
              <span className="text-danger">{r.up}</span>,
              <span className="text-accent2">{r.down}</span>,
              r.same,
              fmtSigned(toDisplayUnit(r.net)!),
              monthEndWeights[r.month - 1] != null
                ? toDisplayUnit(monthEndWeights[r.month - 1])
                : "–",
            ],
          }))}
        />
      </Card>
    </div>
  );
}
