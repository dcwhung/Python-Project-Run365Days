/** The nine dashboard views, in nav order. Paths double as ids. */
export const VIEWS = [
  { path: "overview", label: "Overview" },
  { path: "year", label: "Year in Review" },
  { path: "activity", label: "Activity" },
  { path: "performance", label: "Performance" },
  { path: "weight", label: "Weight" },
  { path: "weather", label: "Weather Impact" },
  { path: "load", label: "Training Load" },
  { path: "activities", label: "Activities" },
  { path: "settings", label: "Settings" },
] as const;

export type ViewPath = (typeof VIEWS)[number]["path"];
