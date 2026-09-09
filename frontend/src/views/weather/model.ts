import type { Activity, DailyWeather } from "@/data/types";
import { paceOf } from "@/lib/analytics";
import { actTemp, isSevere, warnInfo, WX_EMOJI } from "@/lib/weather";
import { maxBy, mean, minBy, sum } from "@/lib/stats-helpers";

export const TEMP_BAND = 4;
export const TEMP_BAND_MIN = 4;
export const TEMP_BAND_MAX = 36;
export const EXTREMES_COUNT = 5;
const WET_START = /Rain|Thunder/;

export function weatherKpis(activities: Activity[], weather: DailyWeather[]) {
  const byDate = new Map(weather.map((w) => [w.date, w]));
  const withTemp = activities.filter((a) => actTemp(a) != null);
  return {
    avgTemp: mean(withTemp.map(actTemp)),
    hottest: maxBy(withTemp, actTemp),
    coldest: minBy(withTemp, actTemp),
    wetStart: activities.filter((a) => a.weather?.description && WET_START.test(a.weather.description)).length,
    rainyDays: activities.filter((a) => (byDate.get(a.date)?.rainfallMm ?? 0) > 0).length,
    severe: activities.filter((a) => a.warnings.some(isSevere)).length,
    beforeSunrise: activities.filter((a) => {
      const w = byDate.get(a.date);
      return w?.sunrise && a.startTime < w.sunrise;
    }).length,
    runs: activities.length,
  };
}

/** HKO max/min per day plus the Garmin temperature of the first run that day. */
export function temperatureRange(daily: { date: string; activityId: string | null }[], activities: Activity[], weather: DailyWeather[]) {
  const byDate = new Map(weather.map((w) => [w.date, w]));
  const byId = new Map(activities.map((a) => [a.id, a]));
  return daily.map((d) => {
    const w = byDate.get(d.date);
    const a = d.activityId ? byId.get(d.activityId) : null;
    return { date: d.date, max: w?.maxTempC ?? null, min: w?.minTempC ?? null, run: a ? actTemp(a) : null };
  });
}

export function conditions(activities: Activity[]) {
  return Object.keys(WX_EMOJI)
    .map((k) => ({ label: k, group: activities.filter((a) => a.weather?.description === k) }))
    .filter((c) => c.group.length)
    .map((c) => ({ label: c.label, runs: c.group.length, pace: paceOf(c.group) }));
}

export function temperatureBands(activities: Activity[]) {
  const out: { label: string; pace: number; runs: number }[] = [];
  for (let t = TEMP_BAND_MIN; t < TEMP_BAND_MAX; t += TEMP_BAND) {
    const g = activities.filter((a) => {
      const x = actTemp(a);
      return x != null && x >= t && x < t + TEMP_BAND && a.paceSecPerKm;
    });
    if (g.length) out.push({ label: `${t}–${t + TEMP_BAND}°`, pace: paceOf(g)!, runs: g.length });
  }
  return out;
}

export function humidityPoints(activities: Activity[]) {
  return activities
    .filter((a) => a.weather?.humidityPct != null && a.paceSecPerKm)
    .map((a) => ({ x: a.weather!.humidityPct!, y: Math.round((a.paceSecPerKm! / 60) * 100) / 100, id: a.id, date: a.date }));
}

export function warningTable(activities: Activity[]) {
  const groups = new Map<string, Activity[]>();
  for (const a of activities) for (const s of a.warnings) groups.set(s, [...(groups.get(s) ?? []), a]);
  return [...groups.entries()]
    .sort((x, y) => y[1].length - x[1].length)
    .map(([signal, g]) => ({
      signal,
      name: warnInfo(signal)?.name ?? signal.toLowerCase().replace(/\b\w/g, (ch) => ch.toUpperCase()),
      runs: g.length,
      avgKm: sum(g.map((a) => a.distanceKm)) / g.length,
      pace: paceOf(g),
    }));
}

/** Hottest and coldest runs. */
export function extremes(activities: Activity[]) {
  const sorted = activities.filter((a) => actTemp(a) != null).sort((x, y) => actTemp(y)! - actTemp(x)!);
  return { hottest: sorted.slice(0, EXTREMES_COUNT), coldest: sorted.slice(-EXTREMES_COUNT) };
}
