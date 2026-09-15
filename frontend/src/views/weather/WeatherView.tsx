import { useActivities, useWeather, useYear } from "@/data/hooks";
import { WeatherKpis } from "./WeatherKpis";
import { TemperatureRangeChart } from "./TemperatureRangeChart";
import { WeatherBreakdownCharts } from "./WeatherBreakdownCharts";
import { WeatherTables } from "./WeatherTables";
import {
  conditions,
  humidityPoints,
  temperatureBands,
  temperatureRange,
  weatherKpis,
} from "./model";

export function WeatherView() {
  const yearQuery = useYear();
  const activitiesQuery = useActivities();
  const weatherQuery = useWeather();
  if (yearQuery.isPending || activitiesQuery.isPending || weatherQuery.isPending)
    return <p className="text-muted">Loading…</p>;
  if (yearQuery.isError || activitiesQuery.isError || weatherQuery.isError)
    return <p className="text-danger">Could not load data.</p>;
  const activities = activitiesQuery.data;
  const dailyWeather = weatherQuery.data;
  const range = temperatureRange(yearQuery.data.dailyDistance, activities, dailyWeather);

  return (
    <div className="space-y-4" data-testid="weather-view">
      <h1 className="text-lg font-semibold">Weather Impact</h1>
      <WeatherKpis kpis={weatherKpis(activities, dailyWeather)} />
      <TemperatureRangeChart range={range} labels={range.map((r) => r.date)} />
      <WeatherBreakdownCharts
        skyConditions={conditions(activities)}
        bands={temperatureBands(activities)}
        humidityVsPace={humidityPoints(activities)}
      />
      <WeatherTables activities={activities} />
    </div>
  );
}
