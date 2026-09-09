import { createBrowserRouter, Navigate } from "react-router-dom";
import { Layout } from "./Layout";
import { OverviewView } from "@/views/overview/OverviewView";
import { YearView } from "@/views/year/YearView";
import { ActivityView } from "@/views/activity/ActivityView";
import { PerformanceView } from "@/views/performance/PerformanceView";
import { WeightView } from "@/views/weight/WeightView";
import { WeatherView } from "@/views/weather/WeatherView";
import { LoadView } from "@/views/load/LoadView";
import { ActivitiesView } from "@/views/activities/ActivitiesView";
import { SettingsView } from "@/views/settings/SettingsView";
import { getPrefs } from "@/lib/prefs";
import { VIEWS } from "./views";

/** Start on the preferred view (Settings), falling back to the overview. */
export function landingPath(): string {
  const landing = getPrefs().landing;
  return VIEWS.some((v) => v.path === landing) ? `/${landing}` : "/overview";
}

export const router = createBrowserRouter([
  {
    path: "/",
    element: <Layout />,
    children: [
      { index: true, element: <Navigate to={landingPath()} replace /> },
      { path: "overview", element: <OverviewView /> },
      { path: "year", element: <YearView /> },
      { path: "activity/:id?", element: <ActivityView /> },
      { path: "performance", element: <PerformanceView /> },
      { path: "weight", element: <WeightView /> },
      { path: "weather", element: <WeatherView /> },
      { path: "load", element: <LoadView /> },
      { path: "activities", element: <ActivitiesView /> },
      { path: "settings", element: <SettingsView /> },
      { path: "*", element: <Navigate to="/overview" replace /> },
    ],
  },
]);
