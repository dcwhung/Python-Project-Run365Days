import { fmtShortDate, fmtSigned } from "@/lib/format";
import { KpiCard } from "@/components/ui/KpiCard";
import type { loadKpis } from "./model";

/** The six headline numbers at the top of the Training Load view. */
export function LoadKpis({ kpis }: { kpis: ReturnType<typeof loadKpis> }) {
  return (
    <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6" data-testid="load-kpis">
      <KpiCard
        label="Fitness (CTL)"
        value={String(kpis.ctl)}
        unit="km/d"
        sub={`year end · peak ${kpis.peakCtl} on ${fmtShortDate(kpis.peakDate)}`}
        accent="accent"
      />
      <KpiCard
        label="Fatigue (ATL)"
        value={String(kpis.atl)}
        unit="km/d"
        sub="year end · 7-day average"
        accent="danger"
      />
      <KpiCard
        label="Form (TSB)"
        value={fmtSigned(kpis.tsb)}
        sub={`year end · ${kpis.tsb >= 0 ? "fresh" : "fatigued"}`}
        accent="accent2"
      />
      <KpiCard
        label="Biggest week"
        value={kpis.biggest.distanceKm.toFixed(1)}
        unit="km"
        sub={`week of ${fmtShortDate(kpis.biggest.weekStart)}`}
        accent="warn"
      />
      <KpiCard
        label="Avg week"
        value={kpis.avgWeekKm.toFixed(1)}
        unit="km"
        sub={
          kpis.avgFullWeekKm != null ? `${kpis.avgFullWeekKm.toFixed(1)} km over full weeks` : ""
        }
        accent="violet"
      />
      <KpiCard
        label="Streak"
        value={String(kpis.activeDays)}
        unit="days"
        sub={kpis.activeDays === kpis.days ? "no rest days" : "active days"}
        accent="accent"
      />
    </div>
  );
}
