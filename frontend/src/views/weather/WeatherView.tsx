import { useNavigate } from "react-router-dom";
import { Bar, Line, Scatter } from "react-chartjs-2";
import { useActivities, useWeather, useYear } from "@/data/hooks";
import { fmtKm, fmtPace, fmtShortDate } from "@/lib/format";
import { minToSec, paceToPlotMin } from "@/lib/units";
import { actTemp, wxEmoji } from "@/lib/weather";
import { BASE, COLORS, GRID, NO_GRID, NO_LEGEND, alpha, dayAxis } from "@/components/charts/theme";
import { Card } from "@/components/ui/Card";
import { KpiCard } from "@/components/ui/KpiCard";
import { DataTable } from "@/components/ui/DataTable";
import { WarningIcons } from "@/components/weather/WarningIcons";
import {
  conditions,
  extremes,
  humidityPoints,
  temperatureBands,
  temperatureRange,
  warningTable,
  weatherKpis,
} from "./model";

/** One colour per sky condition, in the order `conditions()` returns them. */
const CONDITION_COLORS = [COLORS.warn, COLORS.accent, COLORS.violet, COLORS.blue, COLORS.danger];

export function WeatherView() {
  const navigate = useNavigate();
  const yearQuery = useYear();
  const activitiesQuery = useActivities();
  const weatherQuery = useWeather();
  if (yearQuery.isPending || activitiesQuery.isPending || weatherQuery.isPending)
    return <p className="text-muted">Loading…</p>;
  if (yearQuery.isError || activitiesQuery.isError || weatherQuery.isError)
    return <p className="text-danger">Could not load data.</p>;
  const activities = activitiesQuery.data;
  const dailyWeather = weatherQuery.data;
  const kpis = weatherKpis(activities, dailyWeather);
  const range = temperatureRange(yearQuery.data.dailyDistance, activities, dailyWeather);
  const labels = range.map((r) => r.date);
  const skyConditions = conditions(activities);
  const bands = temperatureBands(activities);
  const humidityVsPace = humidityPoints(activities);
  const extremeRuns = extremes(activities);
  const extremeRow = (a: (typeof activities)[number]) => ({
    id: a.id,
    c: [
      `${fmtShortDate(a.date)} ${a.startTime}`,
      `${actTemp(a)!.toFixed(1)} °C`,
      a.weather ? `${wxEmoji(a.weather.description)} ${a.weather.description}` : "–",
      fmtKm(a.distanceKm),
      fmtPace(a.paceSecPerKm),
    ],
  });

  return (
    <div className="space-y-4" data-testid="weather-view">
      <h1 className="text-lg font-semibold">Weather Impact</h1>
      <div
        className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6"
        data-testid="weather-kpis"
      >
        <KpiCard
          label="Avg run temp"
          value={kpis.avgTemp != null ? kpis.avgTemp.toFixed(1) : "–"}
          unit="°C"
          sub="Garmin sensor"
          accent="warn"
        />
        <KpiCard
          label="Hottest run"
          value={kpis.hottest ? actTemp(kpis.hottest)!.toFixed(1) : "–"}
          unit="°C"
          sub={
            kpis.hottest
              ? `${fmtShortDate(kpis.hottest.date)} · ${fmtPace(kpis.hottest.paceSecPerKm)}/km`
              : ""
          }
          accent="danger"
        />
        <KpiCard
          label="Coldest run"
          value={kpis.coldest ? actTemp(kpis.coldest)!.toFixed(1) : "–"}
          unit="°C"
          sub={
            kpis.coldest
              ? `${fmtShortDate(kpis.coldest.date)} · ${fmtPace(kpis.coldest.paceSecPerKm)}/km`
              : ""
          }
          accent="accent"
        />
        <KpiCard
          label="Rain / thunder at start"
          value={String(kpis.wetStart)}
          sub={`${kpis.rainyDays} runs on days with rainfall`}
          accent="accent"
        />
        <KpiCard
          label="Severe warnings"
          value={String(kpis.severe)}
          sub="T3+ or rainstorm signal that day"
          accent="danger"
        />
        <KpiCard
          label="Before sunrise"
          value={String(kpis.beforeSunrise)}
          sub={`${Math.round((kpis.beforeSunrise / (kpis.runs || 1)) * 100)}% of runs started in the dark`}
          accent="violet"
        />
      </div>

      <Card title="HKO daily range and temperature during the run">
        <div className="h-64">
          <Line
            data={{
              labels,
              datasets: [
                {
                  label: "HKO max",
                  data: range.map((r) => r.max),
                  borderColor: alpha(COLORS.danger, 0.5),
                  backgroundColor: alpha(COLORS.danger, 0.1),
                  borderWidth: 1,
                  pointRadius: 0,
                  fill: "+1",
                  spanGaps: true,
                },
                {
                  label: "HKO min",
                  data: range.map((r) => r.min),
                  borderColor: alpha(COLORS.blue, 0.5),
                  borderWidth: 1,
                  pointRadius: 0,
                  spanGaps: true,
                },
                {
                  label: "During run (Garmin)",
                  data: range.map((r) => r.run),
                  borderColor: COLORS.warn,
                  borderWidth: 2,
                  pointRadius: 0,
                  tension: 0.2,
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
                    label: (c) => `${c.dataset.label}: ${c.parsed.y} °C`,
                  },
                },
              },
              scales: {
                x: dayAxis(labels),
                y: { grid: GRID, ticks: { callback: (v) => `${v} °C` } },
              },
            }}
          />
        </div>
      </Card>

      <div className="grid gap-4 lg:grid-cols-3">
        <Card title="Sky at the start">
          <div className="h-52">
            <Bar
              data={{
                labels: skyConditions.map((c) => `${wxEmoji(c.label)} ${c.label}`),
                datasets: [
                  {
                    data: skyConditions.map((c) => c.runs),
                    backgroundColor: CONDITION_COLORS,
                    borderRadius: 5,
                  },
                ],
              }}
              options={{
                ...BASE,
                indexAxis: "y",
                plugins: {
                  ...NO_LEGEND,
                  tooltip: {
                    callbacks: {
                      label: (c) =>
                        `${c.parsed.x} runs · avg ${fmtPace(skyConditions[c.dataIndex].pace)}/km`,
                    },
                  },
                },
                scales: { x: { grid: GRID }, y: { grid: NO_GRID } },
              }}
            />
          </div>
        </Card>
        <Card title="Pace by temperature band">
          <div className="h-52">
            <Bar
              data={{
                labels: bands.map((b) => b.label),
                datasets: [
                  {
                    data: bands.map((b) => paceToPlotMin(b.pace)),
                    backgroundColor: bands.map((_, i) => `hsl(${210 - i * 30},70%,60%)`),
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
                      label: (c) =>
                        `${fmtPace(minToSec(c.parsed.y ?? 0))} /km · ${bands[c.dataIndex].runs} runs`,
                    },
                  },
                },
                scales: {
                  x: { grid: NO_GRID },
                  y: {
                    grid: GRID,
                    reverse: true,
                    ticks: { callback: (v) => fmtPace(minToSec(Number(v))) },
                  },
                },
              }}
            />
          </div>
        </Card>
        <Card title="Humidity vs pace">
          <div className="h-52">
            <Scatter
              data={{
                datasets: [
                  {
                    data: humidityVsPace,
                    backgroundColor: alpha(COLORS.blue, 0.5),
                    pointRadius: 4,
                    pointHoverRadius: 6,
                  },
                ],
              }}
              options={{
                ...BASE,
                onClick: (_e, els) => {
                  if (els.length) navigate(`/activity/${humidityVsPace[els[0].index].id}`);
                },
                plugins: {
                  ...NO_LEGEND,
                  tooltip: {
                    callbacks: {
                      label: (c) => {
                        const p = c.raw as (typeof humidityVsPace)[number];
                        return `${fmtShortDate(p.date)} · ${p.x}% RH · ${fmtPace(minToSec(p.y))}/km`;
                      },
                    },
                  },
                },
                scales: {
                  x: { grid: GRID, title: { display: true, text: "Relative humidity (%)" } },
                  y: {
                    grid: GRID,
                    reverse: true,
                    ticks: { callback: (v) => fmtPace(minToSec(Number(v))) },
                  },
                },
              }}
            />
          </div>
        </Card>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card title="Runs under each HKO signal">
          <DataTable
            testId="warning-table"
            cols={[
              { h: "" },
              { h: "Signal" },
              { h: "Runs", num: true },
              { h: "Avg km", num: true },
              { h: "Avg pace", num: true },
            ]}
            rows={warningTable(activities).map((w) => ({
              key: w.signal,
              c: [
                <WarningIcons signals={[w.signal]} />,
                w.name,
                w.runs,
                w.avgKm.toFixed(2),
                fmtPace(w.pace),
              ],
            }))}
          />
        </Card>
        <Card title="Hottest and coldest runs">
          <DataTable
            testId="extremes-table"
            cols={[
              { h: "Run" },
              { h: "Temp", num: true },
              { h: "Sky" },
              { h: "km", num: true },
              { h: "Pace", num: true },
            ]}
            rows={[
              ...extremeRuns.hottest.map(extremeRow),
              { key: "gap", c: ["…", "", "", "", ""] },
              ...extremeRuns.coldest.map(extremeRow),
            ]}
          />
        </Card>
      </div>
    </div>
  );
}
