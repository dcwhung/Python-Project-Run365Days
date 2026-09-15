import { useWeight, useYear } from "@/data/hooks";
import { toWeightUnit, usePrefs } from "@/lib/prefs";
import { Card } from "@/components/ui/Card";
import { WeightKpis } from "./WeightKpis";
import { WeightTrendChart } from "./WeightTrendChart";
import { WeightBreakdownCharts } from "./WeightBreakdownCharts";
import { MonthlyWeightTable } from "./MonthlyWeightTable";
import {
  dailyDeltas,
  dailySeries,
  lastOfMonth,
  monthlyChange,
  weekdayDelta,
  weeklyKmVsChange,
  weightKpis,
} from "./model";

export function WeightView() {
  const yearQuery = useYear();
  const weightQuery = useWeight();
  const { weightUnit, heightCm } = usePrefs();
  if (yearQuery.isPending || weightQuery.isPending) return <p className="text-muted">Loading…</p>;
  if (yearQuery.isError || weightQuery.isError)
    return <p className="text-danger">Could not load data.</p>;
  const year = yearQuery.data;
  const weight = weightQuery.data;
  const kpis = weightKpis(weight, heightCm, year.totals.days);
  if (!kpis)
    return (
      <Card title="Weight">
        <p className="text-muted">No weight data.</p>
      </Card>
    );
  const labels = year.dailyDistance.map((d) => d.date);
  const { series, ma7: movingAvg7 } = dailySeries(weight, labels);
  const toDisplayUnit = (v: number | null) => toWeightUnit(v, weightUnit);
  const deltas = dailyDeltas(weight);

  return (
    <div className="space-y-4" data-testid="weight-view">
      <h1 className="text-lg font-semibold">
        Weight{" "}
        <span className="text-sm font-normal text-muted">· height {heightCm} cm (Settings)</span>
      </h1>
      <WeightKpis kpis={kpis} weightUnit={weightUnit} />
      <WeightTrendChart
        labels={labels}
        daily={series.map(toDisplayUnit)}
        movingAvg7={movingAvg7.map(toDisplayUnit)}
        weightUnit={weightUnit}
      />
      <WeightBreakdownCharts
        monthlyChanges={monthlyChange(weight).map((v) => (v == null ? null : toDisplayUnit(v)))}
        weekdayDeltas={weekdayDelta(deltas).map((v) =>
          v == null ? null : Math.round(toDisplayUnit(v)! * 100) / 100,
        )}
        weeklyPoints={weeklyKmVsChange(year.weekly, weight, year.dailyDistance, year.year).map(
          (p) => ({ ...p, y: toDisplayUnit(p.y)! }),
        )}
        weightUnit={weightUnit}
      />
      <MonthlyWeightTable
        deltas={deltas}
        entries={weight}
        monthEndWeights={lastOfMonth(weight)}
        weightUnit={weightUnit}
      />
    </div>
  );
}
