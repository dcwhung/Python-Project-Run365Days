import { useNavigate } from "react-router-dom";
import { Bar, Line, Scatter } from "react-chartjs-2";
import { useActivities, useYear } from "@/data/hooks";
import { MONTHS, fmtDuration, fmtKm, fmtPace, fmtShortDate } from "@/lib/format";
import { WEEKDAYS } from "@/lib/dates";
import { minToSec, paceToPlotMin } from "@/lib/units";
import { BASE, COLORS, GRID, NO_GRID, NO_LEGEND, alpha, dayAxis } from "@/components/charts/theme";
import { Card } from "@/components/ui/Card";
import { KpiCard } from "@/components/ui/KpiCard";
import { DataTable } from "@/components/ui/DataTable";
import {
  distanceHistogram,
  monthlyTable,
  paceSeries,
  performanceKpis,
  timeOfDayPace,
  weekdayPace,
} from "./model";

/** Pace in seconds/km as the minutes value the pace axes are plotted in. */
const toPlotMinutes = (sec: number | null) => (sec ? paceToPlotMin(sec) : null);

export function PerformanceView() {
  const navigate = useNavigate();
  const yearQuery = useYear();
  const activitiesQuery = useActivities();
  if (yearQuery.isPending || activitiesQuery.isPending)
    return <p className="text-muted">Loading…</p>;
  if (yearQuery.isError || activitiesQuery.isError)
    return <p className="text-danger">Could not load data.</p>;
  const activities = activitiesQuery.data;
  const year = yearQuery.data;
  const kpis = performanceKpis(activities);
  const { points, trend, dayAct } = paceSeries(activities, year.dailyDistance);
  const labels = year.dailyDistance.map((d) => d.date);
  const histogram = distanceHistogram(activities);
  const weekdayPaces = weekdayPace(activities);
  const timeOfDayPaces = timeOfDayPace(activities);
  const cadenceVsPace = activities
    .filter((a) => a.avgCadence && a.paceSecPerKm)
    .map((a) => ({ x: a.avgCadence!, y: toPlotMinutes(a.paceSecPerKm)!, id: a.id, date: a.date }));
  const rows = monthlyTable(activities);
  const paceScale = (extra = {}) => ({
    grid: GRID,
    reverse: true,
    ticks: { callback: (v: unknown) => fmtPace(minToSec(Number(v))) },
    ...extra,
  });

  return (
    <div className="space-y-4" data-testid="perf-view">
      <h1 className="text-lg font-semibold">
        Performance <span className="text-sm font-normal text-muted">· {kpis.runs} runs</span>
      </h1>
      <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6" data-testid="perf-kpis">
        <KpiCard
          label="Avg pace"
          value={fmtPace(kpis.avgPace)}
          unit="/km"
          sub="distance-weighted"
          accent="accent"
        />
        <KpiCard
          label="Best pace (≥5 km)"
          value={fmtPace(kpis.fastest?.paceSecPerKm)}
          unit="/km"
          sub={kpis.fastest ? fmtShortDate(kpis.fastest.date) : ""}
          accent="accent2"
        />
        <KpiCard
          label="Avg run"
          value={kpis.avgKm.toFixed(2)}
          unit="km"
          sub={`${fmtDuration(kpis.avgSec)} per run`}
          accent="warn"
        />
        <KpiCard
          label="Longest run"
          value={kpis.longest ? fmtKm(kpis.longest.distanceKm) : "–"}
          unit="km"
          sub={kpis.longest ? fmtShortDate(kpis.longest.date) : ""}
          accent="violet"
        />
        <KpiCard
          label="Avg cadence"
          value={kpis.avgCadence ? String(Math.round(kpis.avgCadence)) : "–"}
          unit="spm"
          sub={kpis.maxCadence ? `max ${kpis.maxCadence} spm` : ""}
          accent="danger"
        />
        <KpiCard
          label="Runs ≥ 10 km"
          value={String(kpis.longRuns)}
          sub={`${kpis.midRuns} runs ≥ 7 km`}
          accent="accent"
        />
      </div>

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
                const a = el && dayAct[el.index];
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

      <div className="grid gap-4 lg:grid-cols-3">
        <Card title="Distance distribution">
          <div className="h-52">
            <Bar
              data={{
                labels: histogram.map((h) => h.label),
                datasets: [
                  {
                    data: histogram.map((h) => h.count),
                    backgroundColor: alpha(COLORS.violet, 0.7),
                    borderRadius: 5,
                  },
                ],
              }}
              options={{
                ...BASE,
                plugins: NO_LEGEND,
                scales: {
                  x: { grid: NO_GRID },
                  y: { grid: GRID, ticks: { callback: (v) => `${v} runs` } },
                },
              }}
            />
          </div>
        </Card>
        <Card title="Pace by weekday">
          <div className="h-52">
            <Bar
              data={{
                labels: WEEKDAYS,
                datasets: [
                  {
                    data: weekdayPaces.map((w) => toPlotMinutes(w.pace)),
                    backgroundColor: alpha(COLORS.accent, 0.7),
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
                        `${fmtPace(minToSec(c.parsed.y ?? 0))} /km · ${weekdayPaces[c.dataIndex].runs} runs`,
                    },
                  },
                },
                scales: {
                  x: { grid: NO_GRID },
                  y: paceScale({
                    min: Math.min(...weekdayPaces.map((w) => toPlotMinutes(w.pace) ?? 9)) - 0.2,
                    max: Math.max(...weekdayPaces.map((w) => toPlotMinutes(w.pace) ?? 0)) + 0.2,
                  }),
                },
              }}
            />
          </div>
        </Card>
        <Card title="Pace by time of day">
          <div className="h-52">
            <Bar
              data={{
                labels: timeOfDayPaces.map((t) => t.label.split(" ")[0]),
                datasets: [
                  {
                    data: timeOfDayPaces.map((t) => toPlotMinutes(t.pace)),
                    backgroundColor: alpha(COLORS.warn, 0.7),
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
                      title: (c) => timeOfDayPaces[c[0].dataIndex].label,
                      label: (c) =>
                        `${fmtPace(minToSec(c.parsed.y ?? 0))} /km · ${timeOfDayPaces[c.dataIndex].runs} runs`,
                    },
                  },
                },
                scales: { x: { grid: NO_GRID }, y: paceScale() },
              }}
            />
          </div>
        </Card>
      </div>

      <div className="grid gap-4 lg:grid-cols-[2fr_3fr]">
        <Card title="Cadence vs pace">
          <div className="h-64">
            <Scatter
              data={{
                datasets: [
                  {
                    data: cadenceVsPace,
                    backgroundColor: alpha(COLORS.accent2, 0.5),
                    pointRadius: 4,
                    pointHoverRadius: 6,
                  },
                ],
              }}
              options={{
                ...BASE,
                onClick: (_e, els) => {
                  if (els.length) navigate(`/activity/${cadenceVsPace[els[0].index].id}`);
                },
                plugins: {
                  ...NO_LEGEND,
                  tooltip: {
                    callbacks: {
                      label: (c) => {
                        const p = c.raw as (typeof cadenceVsPace)[number];
                        return `${fmtShortDate(p.date)} · ${p.x} spm · ${fmtPace(minToSec(p.y))}/km`;
                      },
                    },
                  },
                },
                scales: {
                  x: { grid: GRID, title: { display: true, text: "Cadence (spm)" } },
                  y: paceScale({ title: { display: true, text: "Pace (min/km)" } }),
                },
              }}
            />
          </div>
        </Card>
        <Card title="Month by month">
          <DataTable
            testId="perf-table"
            cols={[
              { h: "Month" },
              { h: "Runs", num: true },
              { h: "km", num: true },
              { h: "Time", num: true },
              { h: "Avg pace", num: true },
              { h: "Best pace", num: true },
              { h: "Cadence", num: true },
              { h: "kcal", num: true },
            ]}
            rows={[
              ...rows.map((r) => ({
                key: String(r.month),
                c: [
                  MONTHS[r.month - 1],
                  r.runs,
                  r.km.toFixed(1),
                  fmtDuration(r.sec),
                  fmtPace(r.pace),
                  fmtPace(r.best),
                  r.cadence ? Math.round(r.cadence) : "–",
                  r.kcal.toLocaleString(),
                ],
              })),
              {
                key: "year",
                bold: true,
                c: [
                  "Year",
                  kpis.runs,
                  year.totals.distanceKm.toFixed(1),
                  fmtDuration(year.totals.durationSec),
                  fmtPace(year.totals.avgPaceSecPerKm),
                  fmtPace(kpis.fastest?.paceSecPerKm),
                  kpis.avgCadence ? Math.round(kpis.avgCadence) : "–",
                  year.totals.calories.toLocaleString(),
                ],
              },
            ]}
          />
        </Card>
      </div>
    </div>
  );
}
