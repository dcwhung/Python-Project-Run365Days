import { fmtShortDate } from "@/lib/format";
import { toWeightUnit, type Prefs } from "@/lib/prefs";
import { KpiCard } from "@/components/ui/KpiCard";
import type { weightKpis } from "./model";

/** The six headline numbers at the top of the Weight view. */
export function WeightKpis({
  kpis,
  weightUnit,
}: {
  kpis: NonNullable<ReturnType<typeof weightKpis>>;
  weightUnit: Prefs["weightUnit"];
}) {
  const toDisplayUnit = (v: number | null) => toWeightUnit(v, weightUnit);
  return (
    <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6" data-testid="weight-kpis">
      <KpiCard
        label="Start"
        value={`${toDisplayUnit(kpis.first.weightLbs)}`}
        unit={weightUnit}
        sub={fmtShortDate(kpis.first.date)}
        accent="accent"
      />
      <KpiCard
        label="End"
        value={`${toDisplayUnit(kpis.last.weightLbs)}`}
        unit={weightUnit}
        sub={fmtShortDate(kpis.last.date)}
        accent="warn"
      />
      <KpiCard
        label="Lowest"
        value={`${toDisplayUnit(kpis.min.weightLbs)}`}
        unit={weightUnit}
        sub={fmtShortDate(kpis.min.date)}
        accent="accent2"
      />
      <KpiCard
        label="Total loss"
        value={`${toDisplayUnit(kpis.loss)}`}
        unit={weightUnit}
        sub={`${kpis.lossPct.toFixed(1)}% · ${(toDisplayUnit(kpis.lossPerWeek) ?? 0).toFixed(2)} ${weightUnit}/week`}
        accent="accent"
      />
      <KpiCard
        label="BMI"
        value={kpis.bmiEnd.toFixed(1)}
        sub={`from ${kpis.bmiStart.toFixed(1)} at start`}
        accent="violet"
      />
      <KpiCard
        label="Weigh-ins"
        value={String(kpis.weighIns)}
        sub={kpis.missing === 0 ? "every day" : `${kpis.missing} days missing`}
        accent="danger"
      />
    </div>
  );
}
