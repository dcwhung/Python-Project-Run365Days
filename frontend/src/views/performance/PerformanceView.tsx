import { useNavigate } from "react-router-dom";
import { Bar, Line, Scatter } from "react-chartjs-2";
import { useActivities, useYear } from "@/data/hooks";
import { MONTHS, fmtDuration, fmtKm, fmtPace, fmtShortDate } from "@/lib/format";
import { WEEKDAYS } from "@/lib/dates";
import { BASE, COLORS, GRID, NO_GRID, NO_LEGEND, dayAxis } from "@/components/charts/theme";
import { Card } from "@/components/Card";
import { KpiCard } from "@/components/KpiCard";
import { DataTable } from "@/components/DataTable";
import { distanceHistogram, monthlyTable, paceSeries, performanceKpis, timeOfDayPace, weekdayPace } from "./model";

const min = (sec: number | null) => (sec ? Math.round((sec / 60) * 100) / 100 : null);

export function PerformanceView() {
  const navigate = useNavigate();
  const year = useYear();
  const activities = useActivities();
  if (year.isPending || activities.isPending) return <p className="text-muted">Loading…</p>;
  if (year.isError || activities.isError) return <p className="text-danger">Could not load data.</p>;
  const acts = activities.data;
  const y = year.data;
  const k = performanceKpis(acts);
  const { points, trend, dayAct } = paceSeries(acts, y.dailyDistance);
  const labels = y.dailyDistance.map((d) => d.date);
  const hist = distanceHistogram(acts);
  const wd = weekdayPace(acts);
  const tod = timeOfDayPace(acts);
  const cp = acts.filter((a) => a.avgCadence && a.paceSecPerKm).map((a) => ({ x: a.avgCadence!, y: min(a.paceSecPerKm)!, id: a.id, date: a.date }));
  const rows = monthlyTable(acts);
  const paceScale = (extra = {}) => ({ grid: GRID, reverse: true, ticks: { callback: (v: unknown) => fmtPace(Number(v) * 60) }, ...extra });

  return (
    <div className="space-y-4" data-testid="perf-view">
      <h1 className="text-lg font-semibold">Performance <span className="text-sm font-normal text-muted">· {k.runs} runs</span></h1>
      <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6" data-testid="perf-kpis">
        <KpiCard label="Avg pace" value={fmtPace(k.avgPace)} unit="/km" sub="distance-weighted" accent="accent" />
        <KpiCard label="Best pace (≥5 km)" value={fmtPace(k.fastest?.paceSecPerKm)} unit="/km" sub={k.fastest ? fmtShortDate(k.fastest.date) : ""} accent="accent2" />
        <KpiCard label="Avg run" value={k.avgKm.toFixed(2)} unit="km" sub={`${fmtDuration(k.avgSec)} per run`} accent="warn" />
        <KpiCard label="Longest run" value={k.longest ? fmtKm(k.longest.distanceKm) : "–"} unit="km" sub={k.longest ? fmtShortDate(k.longest.date) : ""} accent="violet" />
        <KpiCard label="Avg cadence" value={k.avgCadence ? String(Math.round(k.avgCadence)) : "–"} unit="spm" sub={k.maxCadence ? `max ${k.maxCadence} spm` : ""} accent="danger" />
        <KpiCard label="Runs ≥ 10 km" value={String(k.longRuns)} sub={`${k.midRuns} runs ≥ 7 km`} accent="accent" />
      </div>

      <Card title="Pace per run and 30-day trend">
        <div className="h-64">
          <Line
            data={{ labels, datasets: [
              { label: "Run", data: points, showLine: false, pointRadius: 3, pointBackgroundColor: "rgba(79,142,247,.55)", pointBorderWidth: 0 },
              { label: "30-day trend", data: trend, borderColor: COLORS.accent2, borderWidth: 2, pointRadius: 0, tension: 0.3, spanGaps: true },
            ] }}
            options={{ ...BASE, interaction: { mode: "nearest", intersect: true },
              onClick: (_e, els) => { const el = els.find((x) => x.datasetIndex === 0); const a = el && dayAct[el.index]; if (a) navigate(`/activity/${a.id}`); },
              plugins: { legend: { position: "top", labels: { boxWidth: 10, padding: 10 } }, tooltip: { callbacks: { title: (c) => fmtShortDate(labels[c[0].dataIndex]), label: (c) => `${c.dataset.label}: ${fmtPace((c.parsed.y ?? 0) * 60)} /km` } } },
              scales: { x: dayAxis(labels), y: paceScale() } }}
          />
        </div>
      </Card>

      <div className="grid gap-4 lg:grid-cols-3">
        <Card title="Distance distribution">
          <div className="h-52"><Bar data={{ labels: hist.map((h) => h.label), datasets: [{ data: hist.map((h) => h.count), backgroundColor: "rgba(167,139,250,.7)", borderRadius: 5 }] }} options={{ ...BASE, plugins: NO_LEGEND, scales: { x: { grid: NO_GRID }, y: { grid: GRID, ticks: { callback: (v) => `${v} runs` } } } }} /></div>
        </Card>
        <Card title="Pace by weekday">
          <div className="h-52"><Bar data={{ labels: WEEKDAYS, datasets: [{ data: wd.map((w) => min(w.pace)), backgroundColor: "rgba(79,142,247,.7)", borderRadius: 5 }] }} options={{ ...BASE, plugins: { ...NO_LEGEND, tooltip: { callbacks: { label: (c) => `${fmtPace((c.parsed.y ?? 0) * 60)} /km · ${wd[c.dataIndex].runs} runs` } } }, scales: { x: { grid: NO_GRID }, y: paceScale({ min: Math.min(...wd.map((w) => min(w.pace) ?? 9)) - 0.2, max: Math.max(...wd.map((w) => min(w.pace) ?? 0)) + 0.2 }) } }} /></div>
        </Card>
        <Card title="Pace by time of day">
          <div className="h-52"><Bar data={{ labels: tod.map((t) => t.label.split(" ")[0]), datasets: [{ data: tod.map((t) => min(t.pace)), backgroundColor: "rgba(245,158,11,.7)", borderRadius: 5 }] }} options={{ ...BASE, plugins: { ...NO_LEGEND, tooltip: { callbacks: { title: (c) => tod[c[0].dataIndex].label, label: (c) => `${fmtPace((c.parsed.y ?? 0) * 60)} /km · ${tod[c.dataIndex].runs} runs` } } }, scales: { x: { grid: NO_GRID }, y: paceScale() } }} /></div>
        </Card>
      </div>

      <div className="grid gap-4 lg:grid-cols-[2fr_3fr]">
        <Card title="Cadence vs pace">
          <div className="h-64"><Scatter data={{ datasets: [{ data: cp, backgroundColor: "rgba(52,211,153,.5)", pointRadius: 4, pointHoverRadius: 6 }] }} options={{ ...BASE, onClick: (_e, els) => { if (els.length) navigate(`/activity/${cp[els[0].index].id}`); }, plugins: { ...NO_LEGEND, tooltip: { callbacks: { label: (c) => { const p = c.raw as (typeof cp)[number]; return `${fmtShortDate(p.date)} · ${p.x} spm · ${fmtPace(p.y * 60)}/km`; } } } }, scales: { x: { grid: GRID, title: { display: true, text: "Cadence (spm)" } }, y: paceScale({ title: { display: true, text: "Pace (min/km)" } }) } }} /></div>
        </Card>
        <Card title="Month by month">
          <DataTable
            testId="perf-table"
            cols={[{ h: "Month" }, { h: "Runs", num: true }, { h: "km", num: true }, { h: "Time", num: true }, { h: "Avg pace", num: true }, { h: "Best pace", num: true }, { h: "Cadence", num: true }, { h: "kcal", num: true }]}
            rows={[
              ...rows.map((r) => ({ key: String(r.month), c: [MONTHS[r.month - 1], r.runs, r.km.toFixed(1), fmtDuration(r.sec), fmtPace(r.pace), fmtPace(r.best), r.cadence ? Math.round(r.cadence) : "–", r.kcal.toLocaleString()] })),
              { key: "year", bold: true, c: ["Year", k.runs, y.totals.distanceKm.toFixed(1), fmtDuration(y.totals.durationSec), fmtPace(y.totals.avgPaceSecPerKm), fmtPace(k.fastest?.paceSecPerKm), k.avgCadence ? Math.round(k.avgCadence) : "–", y.totals.calories.toLocaleString()] },
            ]}
          />
        </Card>
      </div>
    </div>
  );
}
