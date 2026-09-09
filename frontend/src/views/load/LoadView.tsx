import { useState } from "react";
import { Bar, Chart, Line } from "react-chartjs-2";
import type { ChartData } from "chart.js";
import { useActivities, useYear } from "@/data/hooks";
import { fmtDuration, fmtKm, fmtPace, fmtShortDate } from "@/lib/format";
import { WEEKDAYS } from "@/lib/dates";
import { paceOf, weeksWithActivities, type WeekWithActivities } from "@/lib/analytics";
import { actTemp, wxEmoji } from "@/lib/weather";
import { BASE, COLORS, GRID, NO_GRID, NO_LEGEND, dayAxis } from "@/components/charts/theme";
import { Card } from "@/components/Card";
import { KpiCard } from "@/components/KpiCard";
import { DataTable } from "@/components/DataTable";
import { loadKpis, rollingWeeks, topWeeks, weekChange, weekdayAvgKm } from "./model";

const signed = (v: number) => `${v > 0 ? "+" : ""}${v}`;

export function LoadView() {
  const year = useYear();
  const activities = useActivities();
  const [selected, setSelected] = useState<WeekWithActivities | null>(null);
  if (year.isPending || activities.isPending) return <p className="text-muted">Loading…</p>;
  if (year.isError || activities.isError) return <p className="text-danger">Could not load data.</p>;
  const y = year.data;
  const acts = activities.data;
  const weeks = weeksWithActivities(y.weekly, acts);
  const k = loadKpis(y.trainingLoad, weeks, y.totals.distanceKm, y.totals.days, y.totals.activeDays);
  const labels = y.trainingLoad.map((p) => p.date);
  const avg4 = rollingWeeks(weeks);
  const wdKm = weekdayAvgKm(acts);
  const weekRow = (w: WeekWithActivities) => ({ key: String(w.week), c: [fmtShortDate(w.weekStart), w.runs, w.distanceKm.toFixed(1), fmtDuration(w.durationSec), fmtPace(paceOf(w.activities)), fmtKm(w.longestKm)] });

  return (
    <div className="space-y-4" data-testid="load-view">
      <h1 className="text-lg font-semibold">Training Load</h1>
      <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6" data-testid="load-kpis">
        <KpiCard label="Fitness (CTL)" value={String(k.ctl)} unit="km/d" sub={`year end · peak ${k.peakCtl} on ${fmtShortDate(k.peakDate)}`} accent="accent" />
        <KpiCard label="Fatigue (ATL)" value={String(k.atl)} unit="km/d" sub="year end · 7-day average" accent="danger" />
        <KpiCard label="Form (TSB)" value={signed(k.tsb)} sub={`year end · ${k.tsb >= 0 ? "fresh" : "fatigued"}`} accent="accent2" />
        <KpiCard label="Biggest week" value={k.biggest.distanceKm.toFixed(1)} unit="km" sub={`week of ${fmtShortDate(k.biggest.weekStart)}`} accent="warn" />
        <KpiCard label="Avg week" value={k.avgWeekKm.toFixed(1)} unit="km" sub={k.avgFullWeekKm != null ? `${k.avgFullWeekKm.toFixed(1)} km over full weeks` : ""} accent="violet" />
        <KpiCard label="Streak" value={String(k.activeDays)} unit="days" sub={k.activeDays === k.days ? "no rest days" : "active days"} accent="accent" />
      </div>

      <Card title="Fitness, fatigue and form">
        <div className="h-64">
          <Line
            data={{ labels, datasets: [
              { label: "Fitness (CTL)", data: y.trainingLoad.map((p) => p.ctl), borderColor: COLORS.accent, backgroundColor: "rgba(79,142,247,.1)", fill: true, borderWidth: 2, pointRadius: 0, tension: 0.3, yAxisID: "y" },
              { label: "Fatigue (ATL)", data: y.trainingLoad.map((p) => p.atl), borderColor: COLORS.danger, borderWidth: 1.5, borderDash: [4, 2], pointRadius: 0, tension: 0.3, yAxisID: "y" },
              { label: "Form (TSB)", data: y.trainingLoad.map((p) => p.tsb), borderColor: COLORS.accent2, borderWidth: 1.5, pointRadius: 0, tension: 0.3, yAxisID: "y2" },
            ] }}
            options={{ ...BASE, interaction: { mode: "index", intersect: false }, plugins: { legend: { position: "top", labels: { boxWidth: 10, padding: 10 } }, tooltip: { callbacks: { title: (c) => fmtShortDate(labels[c[0].dataIndex]) } } }, scales: { x: dayAxis(labels), y: { grid: GRID, position: "left", ticks: { callback: (v) => `${v} km` } }, y2: { grid: NO_GRID, position: "right", ticks: { callback: (v) => signed(Number(v)) } } } }}
          />
        </div>
      </Card>

      <div className="grid gap-4 lg:grid-cols-[3fr_2fr]">
        <Card title="Weekly distance and 4-week average" action={<span className="text-[10px] text-muted">click a bar for that week</span>}>
          <div className="h-64">
            <Chart
              type="bar"
              data={{ labels: weeks.map((w) => fmtShortDate(w.weekStart)), datasets: [
                { type: "line", label: "4-week avg", data: avg4, borderColor: COLORS.warn, borderWidth: 2, pointRadius: 0, tension: 0.3, order: 0 },
                { type: "bar", label: "Week km", data: weeks.map((w) => w.distanceKm), backgroundColor: "rgba(79,142,247,.7)", borderRadius: 3, order: 1 },
              ] } as ChartData<"bar" | "line">}
              options={{ ...BASE, interaction: { mode: "index", intersect: false }, onClick: (_e, els) => { if (els.length) setSelected(weeks[els[0].index]); },
                plugins: { legend: { position: "top", labels: { boxWidth: 10, padding: 10 } }, tooltip: { callbacks: { title: (c) => `week of ${c[0].label}`, afterBody: (c) => { const i = c[0].dataIndex; const ch = weekChange(weeks, i); return ch == null ? `${weeks[i].runs} runs` : `${ch.toFixed(0)}% vs previous week · ${weeks[i].runs} runs`; } } } },
                scales: { x: { grid: NO_GRID, ticks: { maxTicksLimit: 12, maxRotation: 0 } }, y: { grid: GRID, ticks: { callback: (v) => `${v} km` } } } }}
            />
          </div>
        </Card>
        <Card title="Average distance by weekday">
          <div className="h-64"><Bar data={{ labels: WEEKDAYS, datasets: [{ data: wdKm, backgroundColor: "rgba(167,139,250,.7)", borderRadius: 5 }] }} options={{ ...BASE, plugins: { ...NO_LEGEND, tooltip: { callbacks: { label: (c) => `${c.parsed.y} km per run` } } }, scales: { x: { grid: NO_GRID }, y: { grid: GRID, ticks: { callback: (v) => `${v} km` } } } }} /></div>
        </Card>
      </div>

      <Card
        title={selected ? `Week of ${fmtShortDate(selected.weekStart)} — ${selected.distanceKm.toFixed(1)} km` : "Biggest weeks"}
        action={selected && <button type="button" className="text-xs text-accent hover:underline" onClick={() => setSelected(null)}>Back to biggest weeks</button>}
      >
        {selected ? (
          <DataTable testId="week-detail" cols={[{ h: "Date" }, { h: "km", num: true }, { h: "Time", num: true }, { h: "Pace", num: true }, { h: "Weather" }]} rows={selected.activities.map((a) => ({ id: a.id, c: [`${fmtShortDate(a.date)} ${a.startTime}`, fmtKm(a.distanceKm), fmtDuration(a.durationSec), fmtPace(a.paceSecPerKm), a.weather ? `${wxEmoji(a.weather.description)} ${actTemp(a) ?? ""}°C` : "–"] }))} />
        ) : (
          <DataTable testId="top-weeks" cols={[{ h: "Week of" }, { h: "Runs", num: true }, { h: "km", num: true }, { h: "Time", num: true }, { h: "Avg pace", num: true }, { h: "Longest", num: true }]} rows={topWeeks(weeks).map(weekRow)} />
        )}
      </Card>
    </div>
  );
}
