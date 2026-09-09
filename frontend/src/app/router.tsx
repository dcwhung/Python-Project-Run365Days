import { createBrowserRouter, Navigate } from "react-router-dom";
import { Layout } from "./Layout";
import { OverviewView } from "@/views/overview/OverviewView";
import { ActivityView } from "@/views/activity/ActivityView";
import { ActivitiesView } from "@/views/activities/ActivitiesView";
import { PlaceholderView } from "@/views/PlaceholderView";
import { VIEWS } from "./views";

const BUILT = new Set(["overview", "activity", "activities"]);

export const router = createBrowserRouter([
  {
    path: "/",
    element: <Layout />,
    children: [
      { index: true, element: <Navigate to="/overview" replace /> },
      { path: "overview", element: <OverviewView /> },
      { path: "activity/:id?", element: <ActivityView /> },
      { path: "activities", element: <ActivitiesView /> },
      ...VIEWS.filter((v) => !BUILT.has(v.path)).map((v) => ({
        path: v.path,
        element: <PlaceholderView name={v.label} />,
      })),
      { path: "*", element: <Navigate to="/overview" replace /> },
    ],
  },
]);
