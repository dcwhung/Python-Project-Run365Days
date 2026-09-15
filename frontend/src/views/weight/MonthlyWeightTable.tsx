import type { WeightEntry } from "@/data/types";
import { MONTHS, fmtSigned } from "@/lib/format";
import { toWeightUnit, type Prefs } from "@/lib/prefs";
import { Card } from "@/components/ui/Card";
import { DataTable } from "@/components/ui/DataTable";
import { monthlyUpDown, type Delta } from "./model";

/** How many days went up, down or nowhere in each month, and where it ended. */
export function MonthlyWeightTable({
  deltas,
  entries,
  monthEndWeights,
  weightUnit,
}: {
  deltas: Delta[];
  entries: WeightEntry[];
  monthEndWeights: (number | null)[];
  weightUnit: Prefs["weightUnit"];
}) {
  const toDisplayUnit = (v: number | null) => toWeightUnit(v, weightUnit);
  return (
    <Card title="Up, down or unchanged days per month">
      <DataTable
        testId="weight-table"
        cols={[
          { h: "Month" },
          { h: "Up", num: true },
          { h: "Down", num: true },
          { h: "Same", num: true },
          { h: `Net (${weightUnit})`, num: true },
          { h: "Month end", num: true },
        ]}
        rows={monthlyUpDown(deltas, entries).map((r) => ({
          key: String(r.month),
          c: [
            MONTHS[r.month - 1],
            <span className="text-danger">{r.up}</span>,
            <span className="text-accent2">{r.down}</span>,
            r.same,
            fmtSigned(toDisplayUnit(r.net)!),
            monthEndWeights[r.month - 1] != null
              ? toDisplayUnit(monthEndWeights[r.month - 1])
              : "–",
          ],
        }))}
      />
    </Card>
  );
}
