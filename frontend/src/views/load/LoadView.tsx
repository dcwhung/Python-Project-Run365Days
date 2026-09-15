import { useState } from "react";
import { useActivities, useYear } from "@/data/hooks";
import { weeksWithActivities, type WeekWithActivities } from "@/lib/analytics";
import { LoadKpis } from "./LoadKpis";
import { FitnessFatigueChart } from "./FitnessFatigueChart";
import { WeeklyVolumeCharts } from "./WeeklyVolumeCharts";
import { WeekTables } from "./WeekTables";
import { loadKpis, rollingWeeks, weekdayAvgKm } from "./model";

export function LoadView() {
  const yearQuery = useYear();
  const activitiesQuery = useActivities();
  const [selected, setSelected] = useState<WeekWithActivities | null>(null);
  if (yearQuery.isPending || activitiesQuery.isPending)
    return <p className="text-muted">Loading…</p>;
  if (yearQuery.isError || activitiesQuery.isError)
    return <p className="text-danger">Could not load data.</p>;
  const year = yearQuery.data;
  const activities = activitiesQuery.data;
  const weeks = weeksWithActivities(year.weekly, activities);
  const kpis = loadKpis(
    year.trainingLoad,
    weeks,
    year.totals.distanceKm,
    year.totals.days,
    year.totals.activeDays,
  );

  return (
    <div className="space-y-4" data-testid="load-view">
      <h1 className="text-lg font-semibold">Training Load</h1>
      <LoadKpis kpis={kpis} />
      <FitnessFatigueChart
        trainingLoad={year.trainingLoad}
        labels={year.trainingLoad.map((p) => p.date)}
      />
      <WeeklyVolumeCharts
        weeks={weeks}
        fourWeekAvg={rollingWeeks(weeks)}
        weekdayKm={weekdayAvgKm(activities)}
        onSelectWeek={setSelected}
      />
      <WeekTables weeks={weeks} selected={selected} onClearSelection={() => setSelected(null)} />
    </div>
  );
}
