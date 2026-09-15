import { fmtPace, fmtShortDate } from "@/lib/format";
import { actTemp } from "@/lib/weather";
import { KpiCard } from "@/components/ui/KpiCard";
import type { weatherKpis } from "./model";

/** The six headline numbers at the top of the Weather Impact view. */
export function WeatherKpis({ kpis }: { kpis: ReturnType<typeof weatherKpis> }) {
  return (
    <div
      className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6"
      data-testid="weather-kpis"
    >
      <KpiCard
        label="Avg run temp"
        value={kpis.avgTemp != null ? kpis.avgTemp.toFixed(1) : "–"}
        unit="°C"
        sub="Garmin sensor"
        accent="warn"
      />
      <KpiCard
        label="Hottest run"
        value={kpis.hottest ? actTemp(kpis.hottest)!.toFixed(1) : "–"}
        unit="°C"
        sub={
          kpis.hottest
            ? `${fmtShortDate(kpis.hottest.date)} · ${fmtPace(kpis.hottest.paceSecPerKm)}/km`
            : ""
        }
        accent="danger"
      />
      <KpiCard
        label="Coldest run"
        value={kpis.coldest ? actTemp(kpis.coldest)!.toFixed(1) : "–"}
        unit="°C"
        sub={
          kpis.coldest
            ? `${fmtShortDate(kpis.coldest.date)} · ${fmtPace(kpis.coldest.paceSecPerKm)}/km`
            : ""
        }
        accent="accent"
      />
      <KpiCard
        label="Rain / thunder at start"
        value={String(kpis.wetStart)}
        sub={`${kpis.rainyDays} runs on days with rainfall`}
        accent="accent"
      />
      <KpiCard
        label="Severe warnings"
        value={String(kpis.severe)}
        sub="T3+ or rainstorm signal that day"
        accent="danger"
      />
      <KpiCard
        label="Before sunrise"
        value={String(kpis.beforeSunrise)}
        sub={`${Math.round((kpis.beforeSunrise / (kpis.runs || 1)) * 100)}% of runs started in the dark`}
        accent="violet"
      />
    </div>
  );
}
