import { useNavigate } from "react-router-dom";
import { Bar, Line, Scatter } from "react-chartjs-2";
import { useActivities, useWeather, useYear } from "@/data/hooks";
import { fmtKm, fmtPace, fmtShortDate } from "@/lib/format";
import { actTemp, wxEmoji } from "@/lib/weather";
import { BASE, COLORS, GRID, NO_GRID, NO_LEGEND, dayAxis } from "@/components/charts/theme";
import { Card } from "@/components/Card";
import { KpiCard } from "@/components/KpiCard";
import { DataTable } from "@/components/DataTable";
import { WarningIcons } from "@/components/WarningIcons";
import { conditions, extremes, humidityPoints, temperatureBands, temperatureRange, warningTable, weatherKpis } from "./model";

const COND_COLORS = ["#f59e0b", "#4f8ef7", "#a78bfa", "#60a5fa", "#f87171"];

export function WeatherView() {
  const navigate = useNavigate();
  const year = useYear();
  const activities = useActivities();
  const weather = useWeather();
  if (year.isPending || activities.isPending || weather.isPending) return <p className="text-muted">Loading…</p>;
  if (year.isError || activities.isError || weather.isError) return <p className="text-danger">Could not load data.</p>;
  const acts = activities.data;
  const wx = weather.data;
  const k = weatherKpis(acts, wx);
  const range = temperatureRange(year.data.dailyDistance, acts, wx);
  const labels = range.map((r) => r.date);
  const conds = conditions(acts);
  const bands = temperatureBands(acts);
  const hp = humidityPoints(acts);
  const ext = extremes(acts);
  const erow = (a: (typeof acts)[number]) => ({ id: a.id, c: [`${fmtShortDate(a.date)} ${a.startTime}`, `${actTemp(a)!.toFixed(1)} °C`, a.weather ? `${wxEmoji(a.weather.description)} ${a.weather.description}` : "–", fmtKm(a.distanceKm), fmtPace(a.paceSecPerKm)] });

  return (
    <div className="space-y-4" data-testid="weather-view">
      <h1 className="text-lg font-semibold">Weather Impact</h1>
      <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6" data-testid="weather-kpis">
        <KpiCard label="Avg run temp" value={k.avgTemp != null ? k.avgTemp.toFixed(1) : "–"} unit="°C" sub="Garmin sensor" accent="warn" />
        <KpiCard label="Hottest run" value={k.hottest ? actTemp(k.hottest)!.toFixed(1) : "–"} unit="°C" sub={k.hottest ? `${fmtShortDate(k.hottest.date)} · ${fmtPace(k.hottest.paceSecPerKm)}/km` : ""} accent="danger" />
        <KpiCard label="Coldest run" value={k.coldest ? actTemp(k.coldest)!.toFixed(1) : "–"} unit="°C" sub={k.coldest ? `${fmtShortDate(k.coldest.date)} · ${fmtPace(k.coldest.paceSecPerKm)}/km` : ""} accent="accent" />
        <KpiCard label="Rain / thunder at start" value={String(k.wetStart)} sub={`${k.rainyDays} runs on days with rainfall`} accent="accent" />
        <KpiCard label="Severe warnings" value={String(k.severe)} sub="T3+ or rainstorm signal that day" accent="danger" />
        <KpiCard label="Before sunrise" value={String(k.beforeSunrise)} sub={`${Math.round((k.beforeSunrise / (k.runs || 1)) * 100)}% of runs started in the dark`} accent="violet" />
      </div>

      <Card title="HKO daily range and temperature during the run">
        <div className="h-64">
          <Line
            data={{ labels, datasets: [
              { label: "HKO max", data: range.map((r) => r.max), borderColor: "rgba(248,113,113,.5)", backgroundColor: "rgba(248,113,113,.10)", borderWidth: 1, pointRadius: 0, fill: "+1", spanGaps: true },
              { label: "HKO min", data: range.map((r) => r.min), borderColor: "rgba(96,165,250,.5)", borderWidth: 1, pointRadius: 0, spanGaps: true },
              { label: "During run (Garmin)", data: range.map((r) => r.run), borderColor: COLORS.warn, borderWidth: 2, pointRadius: 0, tension: 0.2, spanGaps: true },
            ] }}
            options={{ ...BASE, interaction: { mode: "index", intersect: false }, plugins: { legend: { position: "top", labels: { boxWidth: 10, padding: 10 } }, tooltip: { callbacks: { title: (c) => fmtShortDate(labels[c[0].dataIndex]), label: (c) => `${c.dataset.label}: ${c.parsed.y} °C` } } }, scales: { x: dayAxis(labels), y: { grid: GRID, ticks: { callback: (v) => `${v} °C` } } } }}
          />
        </div>
      </Card>

      <div className="grid gap-4 lg:grid-cols-3">
        <Card title="Sky at the start">
          <div className="h-52"><Bar data={{ labels: conds.map((c) => `${wxEmoji(c.label)} ${c.label}`), datasets: [{ data: conds.map((c) => c.runs), backgroundColor: COND_COLORS, borderRadius: 5 }] }} options={{ ...BASE, indexAxis: "y", plugins: { ...NO_LEGEND, tooltip: { callbacks: { label: (c) => `${c.parsed.x} runs · avg ${fmtPace(conds[c.dataIndex].pace)}/km` } } }, scales: { x: { grid: GRID }, y: { grid: NO_GRID } } }} /></div>
        </Card>
        <Card title="Pace by temperature band">
          <div className="h-52"><Bar data={{ labels: bands.map((b) => b.label), datasets: [{ data: bands.map((b) => Math.round((b.pace / 60) * 100) / 100), backgroundColor: bands.map((_, i) => `hsl(${210 - i * 30},70%,60%)`), borderRadius: 5 }] }} options={{ ...BASE, plugins: { ...NO_LEGEND, tooltip: { callbacks: { label: (c) => `${fmtPace((c.parsed.y ?? 0) * 60)} /km · ${bands[c.dataIndex].runs} runs` } } }, scales: { x: { grid: NO_GRID }, y: { grid: GRID, reverse: true, ticks: { callback: (v) => fmtPace(Number(v) * 60) } } } }} /></div>
        </Card>
        <Card title="Humidity vs pace">
          <div className="h-52"><Scatter data={{ datasets: [{ data: hp, backgroundColor: "rgba(96,165,250,.5)", pointRadius: 4, pointHoverRadius: 6 }] }} options={{ ...BASE, onClick: (_e, els) => { if (els.length) navigate(`/activity/${hp[els[0].index].id}`); }, plugins: { ...NO_LEGEND, tooltip: { callbacks: { label: (c) => { const p = c.raw as (typeof hp)[number]; return `${fmtShortDate(p.date)} · ${p.x}% RH · ${fmtPace(p.y * 60)}/km`; } } } }, scales: { x: { grid: GRID, title: { display: true, text: "Relative humidity (%)" } }, y: { grid: GRID, reverse: true, ticks: { callback: (v) => fmtPace(Number(v) * 60) } } } }} /></div>
        </Card>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card title="Runs under each HKO signal">
          <DataTable testId="warning-table" cols={[{ h: "" }, { h: "Signal" }, { h: "Runs", num: true }, { h: "Avg km", num: true }, { h: "Avg pace", num: true }]} rows={warningTable(acts).map((w) => ({ key: w.signal, c: [<WarningIcons signals={[w.signal]} />, w.name, w.runs, w.avgKm.toFixed(2), fmtPace(w.pace)] }))} />
        </Card>
        <Card title="Hottest and coldest runs">
          <DataTable testId="extremes-table" cols={[{ h: "Run" }, { h: "Temp", num: true }, { h: "Sky" }, { h: "km", num: true }, { h: "Pace", num: true }]} rows={[...ext.hottest.map(erow), { key: "gap", c: ["…", "", "", "", ""] }, ...ext.coldest.map(erow)]} />
        </Card>
      </div>
    </div>
  );
}
