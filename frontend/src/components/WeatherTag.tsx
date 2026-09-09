import type { Activity } from "@/data/types";
import { actTemp, wxEmoji } from "@/lib/weather";

export function WeatherTag({ activity }: { activity: Activity }) {
  const t = actTemp(activity);
  return (
    <span className="inline-flex items-center gap-1 rounded bg-surface2 px-2 py-0.5 text-xs">
      {wxEmoji(activity.weather?.description)} {t != null ? `${t.toFixed(0)}°C` : "–"}
    </span>
  );
}
