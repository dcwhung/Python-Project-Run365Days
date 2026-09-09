import { Bar, Line, Scatter } from "react-chartjs-2";
import { useWeight, useYear } from "@/data/hooks";
import { MONTHS, fmtShortDate } from "@/lib/format";
import { WEEKDAYS } from "@/lib/dates";
import { toWeightUnit, usePrefs } from "@/lib/prefs";
import { BASE, COLORS, GRID, NO_GRID, NO_LEGEND, dayAxis } from "@/components/charts/theme";
import { Card } from "@/components/Card";
import { KpiCard } from "@/components/KpiCard";
import { DataTable } from "@/components/DataTable";
import { dailyDeltas, dailySeries, lastOfMonth, monthlyChange, monthlyUpDown, weekdayDelta, weeklyKmVsChange, weightKpis } from "./model";

const signed = (v: number) => `${v > 0 ? "+" : ""}${v}`;
const upDown = (v: number | null) => ((v ?? 0) <= 0 ? "rgba(52,211,153,.7)" : "rgba(248,113,113,.7)");

export function WeightView() {
  const year = useYear();
  const weight = useWeight();
  const { weightUnit: u, heightCm } = usePrefs();
  if (year.isPending || weight.isPending) return <p className="text-muted">Loading…</p>;
  if (year.isError || weight.isError) return <p className="text-danger">Could not load data.</p>;
  const y = year.data;
  const W = weight.data;
  const k = weightKpis(W, heightCm, y.totals.days);
  if (!k) return <Card title="Weight"><p className="text-muted">No weight data.</p></Card>;
  const labels = y.dailyDistance.map((d) => d.date);
  const { series, ma7 } = dailySeries(W, labels);
  const conv = (v: number | null) => toWeightUnit(v, u);
  const mChange = monthlyChange(W).map((v) => (v == null ? null : conv(v)));
  const deltas = dailyDeltas(W);
  const wdDelta = weekdayDelta(deltas).map((v) => (v == null ? null : Math.round(conv(v)! * 100) / 100));
  const wkPts = weeklyKmVsChange(y.weekly, W, y.dailyDistance, y.year).map((p) => ({ ...p, y: conv(p.y)! }));
  const ends = lastOfMonth(W);

  return (
    <div className="space-y-4" data-testid="weight-view">
      <h1 className="text-lg font-semibold">Weight <span className="text-sm font-normal text-muted">· height {heightCm} cm (Settings)</span></h1>
      <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6" data-testid="weight-kpis">
        <KpiCard label="Start" value={`${conv(k.first.weightLbs)}`} unit={u} sub={fmtShortDate(k.first.date)} accent="accent" />
        <KpiCard label="End" value={`${conv(k.last.weightLbs)}`} unit={u} sub={fmtShortDate(k.last.date)} accent="warn" />
        <KpiCard label="Lowest" value={`${conv(k.min.weightLbs)}`} unit={u} sub={fmtShortDate(k.min.date)} accent="accent2" />
        <KpiCard label="Total loss" value={`${conv(k.loss)}`} unit={u} sub={`${k.lossPct.toFixed(1)}% · ${(conv(k.lossPerWeek) ?? 0).toFixed(2)} ${u}/week`} accent="accent" />
        <KpiCard label="BMI" value={k.bmiEnd.toFixed(1)} sub={`from ${k.bmiStart.toFixed(1)} at start`} accent="violet" />
        <KpiCard label="Weigh-ins" value={String(k.weighIns)} sub={k.missing === 0 ? "every day" : `${k.missing} days missing`} accent="danger" />
      </div>

      <Card title="Daily weight and 7-day average">
        <div className="h-64">
          <Line
            data={{ labels, datasets: [
              { label: `Daily (${u})`, data: series.map(conv), borderColor: "rgba(245,158,11,.45)", borderWidth: 1, pointRadius: 0, spanGaps: true },
              { label: "7-day average", data: ma7.map(conv), borderColor: COLORS.warn, backgroundColor: "rgba(245,158,11,.08)", fill: true, borderWidth: 2.5, pointRadius: 0, tension: 0.3, spanGaps: true },
            ] }}
            options={{ ...BASE, interaction: { mode: "index", intersect: false }, plugins: { legend: { position: "top", labels: { boxWidth: 10, padding: 10 } }, tooltip: { callbacks: { title: (c) => fmtShortDate(labels[c[0].dataIndex]), label: (c) => `${c.dataset.label}: ${c.parsed.y} ${u}` } } }, scales: { x: dayAxis(labels), y: { grid: GRID, ticks: { callback: (v) => `${v} ${u}` } } } }}
          />
        </div>
      </Card>

      <div className="grid gap-4 lg:grid-cols-3">
        <Card title="Change by month (month end vs previous)">
          <div className="h-52"><Bar data={{ labels: MONTHS, datasets: [{ data: mChange, backgroundColor: mChange.map(upDown), borderRadius: 5 }] }} options={{ ...BASE, plugins: { ...NO_LEGEND, tooltip: { callbacks: { label: (c) => `${signed(c.parsed.y ?? 0)} ${u}` } } }, scales: { x: { grid: NO_GRID }, y: { grid: GRID, ticks: { callback: (v) => `${v} ${u}` } } } }} /></div>
        </Card>
        <Card title="Day-to-day change by weekday">
          <div className="h-52"><Bar data={{ labels: WEEKDAYS, datasets: [{ data: wdDelta, backgroundColor: wdDelta.map(upDown), borderRadius: 5 }] }} options={{ ...BASE, plugins: { ...NO_LEGEND, tooltip: { callbacks: { label: (c) => `${signed(c.parsed.y ?? 0)} ${u} vs previous day` } } }, scales: { x: { grid: NO_GRID }, y: { grid: GRID, ticks: { callback: (v) => `${v} ${u}` } } } }} /></div>
        </Card>
        <Card title="Weekly km vs weight change">
          <div className="h-52"><Scatter data={{ datasets: [{ data: wkPts, backgroundColor: "rgba(79,142,247,.5)", pointRadius: 4, pointHoverRadius: 6 }] }} options={{ ...BASE, plugins: { ...NO_LEGEND, tooltip: { callbacks: { label: (c) => { const p = c.raw as (typeof wkPts)[number]; return `week of ${fmtShortDate(p.weekStart)} · ${p.x} km · ${signed(p.y)} ${u}`; } } } }, scales: { x: { grid: GRID, title: { display: true, text: "km that week" } }, y: { grid: GRID, title: { display: true, text: `weight change (${u})` } } } }} /></div>
        </Card>
      </div>

      <Card title="Up, down or unchanged days per month">
        <DataTable
          testId="weight-table"
          cols={[{ h: "Month" }, { h: "Up", num: true }, { h: "Down", num: true }, { h: "Same", num: true }, { h: `Net (${u})`, num: true }, { h: "Month end", num: true }]}
          rows={monthlyUpDown(deltas, W).map((r) => ({ key: String(r.month), c: [MONTHS[r.month - 1], <span className="text-danger">{r.up}</span>, <span className="text-accent2">{r.down}</span>, r.same, signed(conv(r.net)!), ends[r.month - 1] != null ? conv(ends[r.month - 1]) : "–"] }))}
        />
      </Card>
    </div>
  );
}
