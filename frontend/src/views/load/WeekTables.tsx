import { fmtDuration, fmtKm, fmtPace, fmtShortDate } from "@/lib/format";
import { paceOf, type WeekWithActivities } from "@/lib/analytics";
import { actTemp, wxEmoji } from "@/lib/weather";
import { Card } from "@/components/ui/Card";
import { DataTable } from "@/components/ui/DataTable";
import { topWeeks } from "./model";

function weekRow(week: WeekWithActivities) {
  return {
    key: String(week.week),
    c: [
      fmtShortDate(week.weekStart),
      week.runs,
      week.distanceKm.toFixed(1),
      fmtDuration(week.durationSec),
      fmtPace(paceOf(week.activities)),
      fmtKm(week.longestKm),
    ],
  };
}

/**
 * One card in two states: the biggest weeks of the year, or the runs inside
 * whichever week the volume chart last had clicked.
 */
export function WeekTables({
  weeks,
  selected,
  onClearSelection,
}: {
  weeks: WeekWithActivities[];
  selected: WeekWithActivities | null;
  onClearSelection: () => void;
}) {
  return (
    <Card
      title={
        selected
          ? `Week of ${fmtShortDate(selected.weekStart)} — ${selected.distanceKm.toFixed(1)} km`
          : "Biggest weeks"
      }
      action={
        selected && (
          <button
            type="button"
            className="text-xs text-accent hover:underline"
            onClick={onClearSelection}
          >
            Back to biggest weeks
          </button>
        )
      }
    >
      {selected ? (
        <DataTable
          testId="week-detail"
          cols={[
            { h: "Date" },
            { h: "km", num: true },
            { h: "Time", num: true },
            { h: "Pace", num: true },
            { h: "Weather" },
          ]}
          rows={selected.activities.map((a) => ({
            id: a.id,
            c: [
              `${fmtShortDate(a.date)} ${a.startTime}`,
              fmtKm(a.distanceKm),
              fmtDuration(a.durationSec),
              fmtPace(a.paceSecPerKm),
              a.weather ? `${wxEmoji(a.weather.description)} ${actTemp(a) ?? ""}°C` : "–",
            ],
          }))}
        />
      ) : (
        <DataTable
          testId="top-weeks"
          cols={[
            { h: "Week of" },
            { h: "Runs", num: true },
            { h: "km", num: true },
            { h: "Time", num: true },
            { h: "Avg pace", num: true },
            { h: "Longest", num: true },
          ]}
          rows={topWeeks(weeks).map(weekRow)}
        />
      )}
    </Card>
  );
}
