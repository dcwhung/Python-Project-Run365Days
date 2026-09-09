import type { PersonalBests, Totals } from "@/data/types";
import { fmtPace, fmtShortDate } from "@/lib/format";
import { KpiCard } from "@/components/KpiCard";

export function KpiRow({ totals, personalBests }: { totals: Totals; personalBests: PersonalBests }) {
  const fastest = personalBests.fastest;
  return (
    <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-5" data-testid="kpis">
      <KpiCard
        label="Total Distance"
        value={totals.distanceKm.toLocaleString(undefined, { maximumFractionDigits: 1 })}
        unit="km"
        sub={`avg ${(totals.distanceKm / totals.days).toFixed(1)} km/day · ${totals.avgDistanceKm.toFixed(1)} km/run`}
        accent="accent"
      />
      <KpiCard
        label="Total Time"
        value={(totals.durationSec / 3600).toFixed(1)}
        unit="hrs"
        sub={`avg ${Math.round(totals.durationSec / 60 / totals.days)} min/day`}
        accent="accent2"
      />
      <KpiCard
        label="Avg. Pace"
        value={fmtPace(totals.avgPaceSecPerKm)}
        unit="/km"
        sub={fastest ? `best ${fmtPace(fastest.paceSecPerKm)} · ${fmtShortDate(fastest.date)}` : ""}
        accent="warn"
      />
      <KpiCard
        label="Total Calories"
        value={`${Math.round(totals.calories / 1000)}k`}
        unit="kcal"
        sub={`avg ${totals.runs ? Math.round(totals.calories / totals.runs) : 0}/run`}
        accent="danger"
      />
      <KpiCard
        label="Running Streak"
        value={String(totals.activeDays)}
        unit={`/ ${totals.days} days`}
        sub={totals.activeDays === totals.days ? "Full year" : `${totals.days - totals.activeDays} rest days`}
        accent="violet"
      />
    </div>
  );
}
