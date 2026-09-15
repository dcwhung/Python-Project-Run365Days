import type { Activity } from "@/data/types";
import { fmtKm, fmtPace, fmtShortDate } from "@/lib/format";
import { actTemp, wxEmoji } from "@/lib/weather";
import { Card } from "@/components/ui/Card";
import { DataTable } from "@/components/ui/DataTable";
import { WarningIcons } from "@/components/weather/WarningIcons";
import { extremes, warningTable } from "./model";

function extremeRow(activity: Activity) {
  return {
    id: activity.id,
    c: [
      `${fmtShortDate(activity.date)} ${activity.startTime}`,
      `${actTemp(activity)!.toFixed(1)} °C`,
      activity.weather
        ? `${wxEmoji(activity.weather.description)} ${activity.weather.description}`
        : "–",
      fmtKm(activity.distanceKm),
      fmtPace(activity.paceSecPerKm),
    ],
  };
}

/** The two tables at the foot of the view: runs per HKO signal, and the extremes. */
export function WeatherTables({ activities }: { activities: Activity[] }) {
  const extremeRuns = extremes(activities);
  return (
    <div className="grid gap-4 lg:grid-cols-2">
      <Card title="Runs under each HKO signal">
        <DataTable
          testId="warning-table"
          cols={[
            { h: "" },
            { h: "Signal" },
            { h: "Runs", num: true },
            { h: "Avg km", num: true },
            { h: "Avg pace", num: true },
          ]}
          rows={warningTable(activities).map((w) => ({
            key: w.signal,
            c: [
              <WarningIcons signals={[w.signal]} />,
              w.name,
              w.runs,
              w.avgKm.toFixed(2),
              fmtPace(w.pace),
            ],
          }))}
        />
      </Card>
      <Card title="Hottest and coldest runs">
        <DataTable
          testId="extremes-table"
          cols={[
            { h: "Run" },
            { h: "Temp", num: true },
            { h: "Sky" },
            { h: "km", num: true },
            { h: "Pace", num: true },
          ]}
          rows={[
            ...extremeRuns.hottest.map(extremeRow),
            { key: "gap", c: ["…", "", "", "", ""] },
            ...extremeRuns.coldest.map(extremeRow),
          ]}
        />
      </Card>
    </div>
  );
}
