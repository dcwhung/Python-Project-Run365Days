import { useNavigate } from "react-router-dom";
import { Bar, Scatter } from "react-chartjs-2";
import { fmtPace, fmtShortDate } from "@/lib/format";
import { minToSec, paceToPlotMin } from "@/lib/units";
import { wxEmoji } from "@/lib/weather";
import { BASE, COLORS, GRID, NO_GRID, NO_LEGEND, alpha } from "@/components/charts/theme";
import { Card } from "@/components/ui/Card";
import type { conditions, humidityPoints, temperatureBands } from "./model";

/** One colour per sky condition, in the order `conditions()` returns them. */
const CONDITION_COLORS = [COLORS.warn, COLORS.accent, COLORS.violet, COLORS.blue, COLORS.danger];

/** Hue walk for the temperature bands, coolest first. */
const BAND_HUE_START = 210;
const BAND_HUE_STEP = 30;

type SkyCondition = ReturnType<typeof conditions>[number];
type TemperatureBand = ReturnType<typeof temperatureBands>[number];
type HumidityPoint = ReturnType<typeof humidityPoints>[number];

/** Runs counted by the sky at the start, as a horizontal bar chart. */
function SkyConditionChart({ skyConditions }: { skyConditions: SkyCondition[] }) {
  return (
    <Card title="Sky at the start">
      <div className="h-52">
        <Bar
          data={{
            labels: skyConditions.map((c) => `${wxEmoji(c.label)} ${c.label}`),
            datasets: [
              {
                data: skyConditions.map((c) => c.runs),
                backgroundColor: CONDITION_COLORS,
                borderRadius: 5,
              },
            ],
          }}
          options={{
            ...BASE,
            indexAxis: "y",
            plugins: {
              ...NO_LEGEND,
              tooltip: {
                callbacks: {
                  label: (c) =>
                    `${c.parsed.x} runs · avg ${fmtPace(skyConditions[c.dataIndex].pace)}/km`,
                },
              },
            },
            scales: { x: { grid: GRID }, y: { grid: NO_GRID } },
          }}
        />
      </div>
    </Card>
  );
}

/** Average pace within each temperature band. */
function TemperatureBandChart({ bands }: { bands: TemperatureBand[] }) {
  return (
    <Card title="Pace by temperature band">
      <div className="h-52">
        <Bar
          data={{
            labels: bands.map((b) => b.label),
            datasets: [
              {
                data: bands.map((b) => paceToPlotMin(b.pace)),
                backgroundColor: bands.map(
                  (_, i) => `hsl(${BAND_HUE_START - i * BAND_HUE_STEP},70%,60%)`,
                ),
                borderRadius: 5,
              },
            ],
          }}
          options={{
            ...BASE,
            plugins: {
              ...NO_LEGEND,
              tooltip: {
                callbacks: {
                  label: (c) =>
                    `${fmtPace(minToSec(c.parsed.y ?? 0))} /km · ${bands[c.dataIndex].runs} runs`,
                },
              },
            },
            scales: {
              x: { grid: NO_GRID },
              y: {
                grid: GRID,
                reverse: true,
                ticks: { callback: (v) => fmtPace(minToSec(Number(v))) },
              },
            },
          }}
        />
      </div>
    </Card>
  );
}

/** One dot per run: relative humidity against pace; a dot opens that run. */
function HumidityVsPaceChart({ humidityVsPace }: { humidityVsPace: HumidityPoint[] }) {
  const navigate = useNavigate();
  return (
    <Card title="Humidity vs pace">
      <div className="h-52">
        <Scatter
          data={{
            datasets: [
              {
                data: humidityVsPace,
                backgroundColor: alpha(COLORS.blue, 0.5),
                pointRadius: 4,
                pointHoverRadius: 6,
              },
            ],
          }}
          options={{
            ...BASE,
            onClick: (_e, els) => {
              if (els.length) navigate(`/activity/${humidityVsPace[els[0].index].id}`);
            },
            plugins: {
              ...NO_LEGEND,
              tooltip: {
                callbacks: {
                  label: (c) => {
                    const p = c.raw as HumidityPoint;
                    return `${fmtShortDate(p.date)} · ${p.x}% RH · ${fmtPace(minToSec(p.y))}/km`;
                  },
                },
              },
            },
            scales: {
              x: { grid: GRID, title: { display: true, text: "Relative humidity (%)" } },
              y: {
                grid: GRID,
                reverse: true,
                ticks: { callback: (v) => fmtPace(minToSec(Number(v))) },
              },
            },
          }}
        />
      </div>
    </Card>
  );
}

/** The three small charts that share a row under the temperature curve. */
export function WeatherBreakdownCharts({
  skyConditions,
  bands,
  humidityVsPace,
}: {
  skyConditions: SkyCondition[];
  bands: TemperatureBand[];
  humidityVsPace: HumidityPoint[];
}) {
  return (
    <div className="grid gap-4 lg:grid-cols-3">
      <SkyConditionChart skyConditions={skyConditions} />
      <TemperatureBandChart bands={bands} />
      <HumidityVsPaceChart humidityVsPace={humidityVsPace} />
    </div>
  );
}
