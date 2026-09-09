import { createBrowserRouter, Navigate } from "react-router-dom";
import { Layout } from "./Layout";
import { OverviewView } from "@/views/OverviewView";
import { PlaceholderView } from "@/views/PlaceholderView";
import { VIEWS } from "./views";

export const router = createBrowserRouter([
  {
    path: "/",
    element: <Layout />,
    children: [
      { index: true, element: <Navigate to="/overview" replace /> },
      { path: "overview", element: <OverviewView /> },
      { path: "activity/:id?", element: <PlaceholderView name="Activity" /> },
      ...VIEWS.filter((v) => v.path !== "overview" && v.path !== "activity").map((v) => ({
        path: v.path,
        element: <PlaceholderView name={v.label} />,
      })),
      { path: "*", element: <Navigate to="/overview" replace /> },
    ],
  },
]);
