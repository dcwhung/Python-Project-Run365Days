import { useActivities, useWeather, useWeight, useYear } from "@/data/hooks";
import { Card } from "@/components/Card";
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
  const year = useYear();
  const activities = useActivities();
  const weight = useWeight();
  const weather = useWeather();
  if (year.isPending || activities.isPending) return <p className="text-muted">Loading…</p>;
  if (year.isError) return <p className="text-danger">Could not load data: {year.error.message}</p>;
  if (activities.isError) return <p className="text-danger">Could not load activities: {activities.error.message}</p>;
  const y = year.data;
  const acts = activities.data;
  return (
    <div className="space-y-4">
      <h1 className="sr-only">Overview</h1>
      <KpiRow totals={y.totals} personalBests={y.personalBests} />
      <Card title="Daily Distance">
        <Heatmap daily={y.dailyDistance} year={y.year} />
      </Card>
      <div className="grid gap-4 lg:grid-cols-2">
        <MonthlyDistanceChart monthly={y.monthly} />
        <MonthlyPaceChart monthly={y.monthly} />
      </div>
      <div className="grid gap-4 lg:grid-cols-[3fr_2fr]">
        <RecentActivities activities={acts} />
        <PersonalBestList personalBests={y.personalBests} totals={y.totals} />
      </div>
      <div className="grid gap-4 lg:grid-cols-2">
        <TrainingLoadChart load={y.trainingLoad} />
        <WeightChart weight={weight.data ?? []} daily={y.dailyDistance} />
      </div>
      <div className="grid gap-4 lg:grid-cols-2">
        <TempPaceChart activities={acts} />
        <div>
          <WeatherStrip activities={acts} weather={weather.data ?? []} />
        </div>
      </div>
    </div>
  );
}
