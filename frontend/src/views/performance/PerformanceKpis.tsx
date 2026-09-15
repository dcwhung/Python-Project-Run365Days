import { fmtDuration, fmtKm, fmtPace, fmtShortDate } from "@/lib/format";
import { KpiCard } from "@/components/ui/KpiCard";
import type { performanceKpis } from "./model";

/** The six headline numbers at the top of the Performance view. */
export function PerformanceKpis({ kpis }: { kpis: ReturnType<typeof performanceKpis> }) {
  return (
    <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6" data-testid="perf-kpis">
      <KpiCard
        label="Avg pace"
        value={fmtPace(kpis.avgPace)}
        unit="/km"
        sub="distance-weighted"
        accent="accent"
      />
      <KpiCard
        label="Best pace (≥5 km)"
        value={fmtPace(kpis.fastest?.paceSecPerKm)}
        unit="/km"
        sub={kpis.fastest ? fmtShortDate(kpis.fastest.date) : ""}
        accent="accent2"
      />
      <KpiCard
        label="Avg run"
        value={kpis.avgKm.toFixed(2)}
        unit="km"
        sub={`${fmtDuration(kpis.avgSec)} per run`}
        accent="warn"
      />
      <KpiCard
        label="Longest run"
        value={kpis.longest ? fmtKm(kpis.longest.distanceKm) : "–"}
        unit="km"
        sub={kpis.longest ? fmtShortDate(kpis.longest.date) : ""}
        accent="violet"
      />
      <KpiCard
        label="Avg cadence"
        value={kpis.avgCadence ? String(Math.round(kpis.avgCadence)) : "–"}
        unit="spm"
        sub={kpis.maxCadence ? `max ${kpis.maxCadence} spm` : ""}
        accent="danger"
      />
      <KpiCard
        label="Runs ≥ 10 km"
        value={String(kpis.longRuns)}
        sub={`${kpis.midRuns} runs ≥ 7 km`}
        accent="accent"
      />
    </div>
  );
}
