import { useActivities, useWeather, useWeight, useYear } from "@/data/hooks";
import { Card } from "@/components/ui/Card";
import { KpiRow } from "./KpiRow";
import { Heatmap } from "./Heatmap";
import { MonthlyDistanceChart, MonthlyPaceChart } from "./MonthlyCharts";
import { RecentActivities } from "./RecentActivities";
import { PersonalBestList } from "./PersonalBestList";
import { TrainingLoadChart } from "./TrainingLoadChart";
import { WeightChart } from "./WeightChart";
import { TempPaceChart } from "./TempPaceChart";
import { WeatherStrip } from "./WeatherStrip";
import "@/components/charts/theme";

export function OverviewView() {
  const yearQuery = useYear();
  const activitiesQuery = useActivities();
  const weightQuery = useWeight();
  const weatherQuery = useWeather();
  if (yearQuery.isPending || activitiesQuery.isPending)
    return <p className="text-muted">Loading…</p>;
  if (yearQuery.isError)
    return <p className="text-danger">Could not load data: {yearQuery.error.message}</p>;
  if (activitiesQuery.isError)
    return (
      <p className="text-danger">Could not load activities: {activitiesQuery.error.message}</p>
    );
  const year = yearQuery.data;
  const activities = activitiesQuery.data;
  return (
    <div className="space-y-4">
      <h1 className="sr-only">Overview</h1>
      <KpiRow totals={year.totals} personalBests={year.personalBests} />
      <Card title="Daily Distance">
        <Heatmap daily={year.dailyDistance} year={year.year} />
      </Card>
      <div className="grid gap-4 lg:grid-cols-2">
        <MonthlyDistanceChart monthly={year.monthly} />
        <MonthlyPaceChart monthly={year.monthly} />
      </div>
      <div className="grid gap-4 lg:grid-cols-[3fr_2fr]">
        <RecentActivities activities={activities} />
        <PersonalBestList personalBests={year.personalBests} totals={year.totals} />
      </div>
      <div className="grid gap-4 lg:grid-cols-2">
        <TrainingLoadChart load={year.trainingLoad} />
        <WeightChart weight={weightQuery.data ?? []} daily={year.dailyDistance} />
      </div>
      <div className="grid gap-4 lg:grid-cols-2">
        <TempPaceChart activities={activities} />
        <div>
          <WeatherStrip activities={activities} weather={weatherQuery.data ?? []} />
        </div>
      </div>
    </div>
  );
}
