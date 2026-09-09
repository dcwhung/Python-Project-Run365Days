import { useNavigate } from "react-router-dom";
import type { Activity } from "@/data/types";
import { fmtDuration, fmtKm, fmtPace, fmtShortDate } from "@/lib/format";
import { Card } from "@/components/Card";
import { WarningIcons } from "@/components/WarningIcons";
import { WeatherTag } from "@/components/WeatherTag";

const RECENT_COUNT = 8;

export function RecentActivities({ activities }: { activities: Activity[] }) {
  const navigate = useNavigate();
  const recent = activities.slice(-RECENT_COUNT).reverse();
  return (
    <Card
      title="Recent Runs"
      action={
        <button
          type="button"
          className="text-xs text-accent hover:underline"
          onClick={() => navigate("/activities")}
        >
          See all
        </button>
      }
    >
      <table className="w-full text-xs" data-testid="recent">
        <thead className="text-left text-muted">
          <tr>
            <th className="py-1 font-medium">Date</th>
            <th className="py-1 font-medium">Distance</th>
            <th className="py-1 font-medium">Time</th>
            <th className="py-1 font-medium">Pace</th>
            <th className="py-1 font-medium">Weather</th>
            <th className="py-1 font-medium">Warnings</th>
          </tr>
        </thead>
        <tbody>
          {recent.map((a) => (
            <tr
              key={a.id}
              className="cursor-pointer border-t border-border hover:bg-surface2"
              onClick={() => navigate(`/activity/${a.id}`)}
            >
              <td className="py-1.5">{fmtShortDate(a.date)}</td>
              <td className="py-1.5">{fmtKm(a.distanceKm)} km</td>
              <td className="py-1.5">{fmtDuration(a.durationSec)}</td>
              <td className="py-1.5 text-accent2">{fmtPace(a.paceSecPerKm)}/km</td>
              <td className="py-1.5">
                <WeatherTag activity={a} />
              </td>
              <td className="py-1.5">
                <WarningIcons signals={a.warnings} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </Card>
  );
}
