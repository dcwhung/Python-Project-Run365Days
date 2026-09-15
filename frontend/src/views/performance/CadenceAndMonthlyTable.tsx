import { useNavigate } from "react-router-dom";
import { Scatter } from "react-chartjs-2";
import type { Totals } from "@/data/types";
import { MONTHS, fmtDuration, fmtPace, fmtShortDate } from "@/lib/format";
import { minToSec } from "@/lib/units";
import { BASE, COLORS, GRID, NO_LEGEND, alpha } from "@/components/charts/theme";
import { Card } from "@/components/ui/Card";
import { DataTable } from "@/components/ui/DataTable";
import { paceScale } from "./paceAxis";
import type { monthlyTable, performanceKpis } from "./model";

/** One dot per run that recorded cadence: steps per minute against pace. */
export interface CadencePoint {
  x: number;
  y: number;
  id: string;
  date: string;
}

function CadenceVsPaceChart({ cadenceVsPace }: { cadenceVsPace: CadencePoint[] }) {
  const navigate = useNavigate();
  return (
    <Card title="Cadence vs pace">
      <div className="h-64">
        <Scatter
          data={{
            datasets: [
              {
                data: cadenceVsPace,
                backgroundColor: alpha(COLORS.accent2, 0.5),
                pointRadius: 4,
                pointHoverRadius: 6,
              },
            ],
          }}
          options={{
            ...BASE,
            onClick: (_e, els) => {
              if (els.length) navigate(`/activity/${cadenceVsPace[els[0].index].id}`);
            },
            plugins: {
              ...NO_LEGEND,
              tooltip: {
                callbacks: {
                  label: (c) => {
                    const p = c.raw as CadencePoint;
                    return `${fmtShortDate(p.date)} · ${p.x} spm · ${fmtPace(minToSec(p.y))}/km`;
                  },
                },
              },
            },
            scales: {
              x: { grid: GRID, title: { display: true, text: "Cadence (spm)" } },
              y: paceScale({ title: { display: true, text: "Pace (min/km)" } }),
            },
          }}
        />
      </div>
    </Card>
  );
}

/** Twelve month rows plus a bold year row underneath them. */
function MonthlyTable({
  monthlyRows,
  kpis,
  totals,
}: {
  monthlyRows: ReturnType<typeof monthlyTable>;
  kpis: ReturnType<typeof performanceKpis>;
  totals: Totals;
}) {
  return (
    <Card title="Month by month">
      <DataTable
        testId="perf-table"
        cols={[
          { h: "Month" },
          { h: "Runs", num: true },
          { h: "km", num: true },
          { h: "Time", num: true },
          { h: "Avg pace", num: true },
          { h: "Best pace", num: true },
          { h: "Cadence", num: true },
          { h: "kcal", num: true },
        ]}
        rows={[
          ...monthlyRows.map((r) => ({
            key: String(r.month),
            c: [
              MONTHS[r.month - 1],
              r.runs,
              r.km.toFixed(1),
              fmtDuration(r.sec),
              fmtPace(r.pace),
              fmtPace(r.best),
              r.cadence ? Math.round(r.cadence) : "–",
              r.kcal.toLocaleString(),
            ],
          })),
          {
            key: "year",
            bold: true,
            c: [
              "Year",
              kpis.runs,
              totals.distanceKm.toFixed(1),
              fmtDuration(totals.durationSec),
              fmtPace(totals.avgPaceSecPerKm),
              fmtPace(kpis.fastest?.paceSecPerKm),
              kpis.avgCadence ? Math.round(kpis.avgCadence) : "–",
              totals.calories.toLocaleString(),
            ],
          },
        ]}
      />
    </Card>
  );
}

/** The bottom row of the Performance view: the cadence scatter beside the table. */
export function CadenceAndMonthlyTable({
  cadenceVsPace,
  monthlyRows,
  kpis,
  totals,
}: {
  cadenceVsPace: CadencePoint[];
  monthlyRows: ReturnType<typeof monthlyTable>;
  kpis: ReturnType<typeof performanceKpis>;
  totals: Totals;
}) {
  return (
    <div className="grid gap-4 lg:grid-cols-[2fr_3fr]">
      <CadenceVsPaceChart cadenceVsPace={cadenceVsPace} />
      <MonthlyTable monthlyRows={monthlyRows} kpis={kpis} totals={totals} />
    </div>
  );
}
