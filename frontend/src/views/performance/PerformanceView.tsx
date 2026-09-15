import { useActivities, useYear } from "@/data/hooks";
import { PerformanceKpis } from "./PerformanceKpis";
import { PaceTrendChart } from "./PaceTrendChart";
import { PaceBreakdownCharts } from "./PaceBreakdownCharts";
import { CadenceAndMonthlyTable } from "./CadenceAndMonthlyTable";
import { toPlotMinutes } from "./paceAxis";
import {
  distanceHistogram,
  monthlyTable,
  paceSeries,
  performanceKpis,
  timeOfDayPace,
  weekdayPace,
} from "./model";

export function PerformanceView() {
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
  const cadenceVsPace = activities
    .filter((a) => a.avgCadence && a.paceSecPerKm)
    .map((a) => ({ x: a.avgCadence!, y: toPlotMinutes(a.paceSecPerKm)!, id: a.id, date: a.date }));

  return (
    <div className="space-y-4" data-testid="perf-view">
      <h1 className="text-lg font-semibold">
        Performance <span className="text-sm font-normal text-muted">· {kpis.runs} runs</span>
      </h1>
      <PerformanceKpis kpis={kpis} />
      <PaceTrendChart
        labels={year.dailyDistance.map((d) => d.date)}
        points={points}
        trend={trend}
        dayActivities={dayAct}
      />
      <PaceBreakdownCharts
        histogram={distanceHistogram(activities)}
        weekdayPaces={weekdayPace(activities)}
        timeOfDayPaces={timeOfDayPace(activities)}
      />
      <CadenceAndMonthlyTable
        cadenceVsPace={cadenceVsPace}
        monthlyRows={monthlyTable(activities)}
        kpis={kpis}
        totals={year.totals}
      />
    </div>
  );
}
