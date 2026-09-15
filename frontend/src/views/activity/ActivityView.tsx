import { useEffect, useMemo } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { useActivities, useActivity, useTrack } from "@/data/hooks";
import { KpiCard } from "@/components/ui/KpiCard";
import { WarningIcons } from "@/components/weather/WarningIcons";
import { fmtDuration, fmtKm, fmtPace, fmtShortDate } from "@/lib/format";
import { longDate } from "@/lib/dates";
import { mean } from "@/lib/stats-helpers";
import { wxEmoji } from "@/lib/weather";
import { usePrefs } from "@/lib/prefs";
import { SERIES_SPECS, buildSeries } from "./series";
import { usePlayback } from "./usePlayback";
import { RouteMap } from "./RouteMap";
import { SeriesChart } from "./SeriesChart";
import { LiveCard } from "./LiveCard";

export function ActivityView() {
  const { id } = useParams();
  const navigate = useNavigate();
  const activitiesQuery = useActivities();
  const activities = activitiesQuery.data ?? [];
  const current = id ?? activities[activities.length - 1]?.id;
  const activityQuery = useActivity(current);
  // No count named: the data layer owns how many samples a track is worth,
  // and both sources answer with everything the export stored (CUI-0024).
  const trackQuery = useTrack(current);
  const prefs = usePrefs();

  const series = useMemo(
    () =>
      activityQuery.data && trackQuery.data
        ? buildSeries(activityQuery.data, trackQuery.data)
        : null,
    [activityQuery.data, trackQuery.data],
  );
  const times = series?.t ?? EMPTY;
  const playback = usePlayback(times, prefs.speed);

  const index = activities.findIndex((item) => item.id === current);
  const step = (n: number) => {
    const next = activities[index + n];
    if (next) navigate(`/activity/${next.id}`);
  };

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const tag = (e.target as HTMLElement).tagName;
      if (tag === "INPUT" || tag === "SELECT" || tag === "TEXTAREA") return;
      if (e.key === " ") {
        e.preventDefault();
        playback.toggle();
      } else if (e.key === "ArrowRight") {
        playback.stop();
        playback.goTo(playback.idx + 1);
      } else if (e.key === "ArrowLeft") {
        playback.stop();
        playback.goTo(playback.idx - 1);
      } else if (e.key === "PageDown") step(1);
      else if (e.key === "PageUp") step(-1);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });

  if (activitiesQuery.isPending || activityQuery.isPending || trackQuery.isPending)
    return <p className="text-muted">Loading…</p>;
  if (activitiesQuery.isError)
    return (
      <p className="text-danger">Could not load activities: {activitiesQuery.error.message}</p>
    );
  if (!activityQuery.data || !series) return <p className="text-danger">Activity not found.</p>;
  const activity = activityQuery.data;
  const hover = (i: number) => {
    playback.stop();
    playback.goTo(i);
  };
  const avg = {
    ele: Number.isFinite(mean(series.ele) ?? NaN)
      ? `avg ${Math.round(mean(series.ele)!)} m`
      : undefined,
    pace: `avg ${fmtPace(activity.paceSecPerKm)} /km`,
    cad: activity.avgCadence ? `avg ${Math.round(activity.avgCadence)} spm` : undefined,
    temp: activity.avgTempC != null ? `avg ${activity.avgTempC.toFixed(1)} °C` : undefined,
  };

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold">
            {series.hasGps ? "Outdoor" : "Indoor"} Run · Day {activity.dayOfYear}
          </h1>
          <div className="text-xs text-muted">
            {longDate(activity.date)} <b className="text-text">{activity.startTime}</b> · activity{" "}
            <b className="text-text">{activity.id}</b>
            {activity.warnings.length > 0 && (
              <>
                {" "}
                · <WarningIcons signals={activity.warnings} dash={false} />
              </>
            )}
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <span className="rounded bg-surface2 px-2 py-1 text-xs" data-testid="act-wx">
            {activity.weather
              ? `${wxEmoji(activity.weather.description)} ${activity.weather.description} · ${activity.weather.tempC}°C · ${activity.weather.humidityPct}% RH`
              : "no weather record"}
          </span>
          <div className="flex items-center gap-1">
            <button
              type="button"
              aria-label="Previous day"
              className="rounded border border-border px-2 py-1 hover:bg-surface2"
              onClick={() => step(-1)}
              disabled={index <= 0}
            >
              ‹
            </button>
            <select
              aria-label="Choose activity"
              className="rounded border border-border bg-surface px-2 py-1 text-xs"
              value={current}
              onChange={(e) => navigate(`/activity/${e.target.value}`)}
            >
              {activities.map((x) => (
                <option key={x.id} value={x.id}>
                  {fmtShortDate(x.date)} · {fmtKm(x.distanceKm)} km
                </option>
              ))}
            </select>
            <button
              type="button"
              aria-label="Next day"
              className="rounded border border-border px-2 py-1 hover:bg-surface2"
              onClick={() => step(1)}
              disabled={index >= activities.length - 1}
            >
              ›
            </button>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3 md:grid-cols-5" data-testid="act-kpis">
        <KpiCard label="Distance" value={fmtKm(activity.distanceKm)} unit="km" accent="accent" />
        <KpiCard label="Time" value={fmtDuration(activity.durationSec)} accent="accent2" />
        <KpiCard label="Avg Pace" value={fmtPace(activity.paceSecPerKm)} unit="/km" accent="warn" />
        <KpiCard
          label="Total Ascent"
          value={activity.ascentM != null ? String(Math.round(activity.ascentM)) : "–"}
          unit="m"
          accent="violet"
        />
        <KpiCard
          label="Calories"
          value={activity.calories != null ? String(activity.calories) : "–"}
          unit="kcal"
          accent="danger"
        />
      </div>

      <div className="grid gap-4 lg:grid-cols-[1fr_300px]">
        <RouteMap series={series} idx={playback.idx} onHover={hover} />
        <LiveCard
          series={series}
          idx={playback.idx}
          playing={playback.playing}
          speed={playback.speed}
          onToggle={playback.toggle}
          onSpeed={playback.setSpeed}
          onScrub={hover}
        />
      </div>

      <div className="grid gap-4 md:grid-cols-2">
        {SERIES_SPECS.map((spec) => (
          <SeriesChart
            key={spec.key}
            series={series}
            spec={spec}
            idx={playback.idx}
            average={avg[spec.key]}
            onHover={hover}
          />
        ))}
      </div>
    </div>
  );
}

const EMPTY: number[] = [];
