import type { Activity, DailyWeather } from "@/data/types";
import { fmtPace } from "@/lib/format";
import { weatherStrip } from "./model";

function Mini({ title, value, sub, color }: { title: string; value: string; sub: string; color: string }) {
  return (
    <div className="rounded-card border border-border bg-surface p-3">
      <div className="text-[10px] uppercase tracking-wide text-muted">{title}</div>
      <div className={`text-xl font-semibold ${color}`}>{value}</div>
      <div className="text-[11px] text-muted">{sub}</div>
    </div>
  );
}

export function WeatherStrip({ activities, weather }: { activities: Activity[]; weather: DailyWeather[] }) {
  const s = weatherStrip(activities, weather);
  const n = activities.length || 1;
  return (
    <div className="grid grid-cols-2 gap-3 md:grid-cols-4" data-testid="weather-strip">
      <Mini
        title="Avg. Run Temp"
        value={s.avgTemp != null ? `${s.avgTemp.toFixed(1)}°C` : "–"}
        sub={s.minTemp != null ? `Range: ${s.minTemp.toFixed(0)}°C – ${s.maxTemp!.toFixed(0)}°C (Garmin sensor)` : ""}
        color="text-warn"
      />
      <Mini title="Rainy Day Runs" value={String(s.rainyRuns)} sub={`${Math.round((s.rainyRuns / n) * 100)}% of runs on days with rainfall`} color="text-accent" />
      <Mini title="Severe Warning Runs" value={String(s.severeRuns)} sub="Typhoon T3+ or rainstorm signal in force that day" color="text-danger" />
      <Mini
        title="Best Pace Temp"
        value={s.bestBand ? `${s.bestBand.from}–${s.bestBand.to}°C` : "–"}
        sub={s.bestBand ? `avg ${fmtPace(s.bestBand.pace)}/km over ${s.bestBand.runs} runs` : ""}
        color="text-accent2"
      />
    </div>
  );
}
