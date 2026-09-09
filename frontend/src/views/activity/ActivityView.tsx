import { useEffect, useMemo } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { useActivities, useActivity, useTrack } from "@/data/hooks";
import { KpiCard } from "@/components/KpiCard";
import { WarningIcons } from "@/components/WarningIcons";
import { fmtDuration, fmtKm, fmtPace, fmtShortDate } from "@/lib/format";
import { longDate } from "@/lib/dates";
import { mean } from "@/lib/stats-helpers";
import { wxEmoji } from "@/lib/weather";
import { usePrefs } from "@/lib/prefs";
import { buildSeries, type SeriesSpec } from "./series";
import { usePlayback } from "./usePlayback";
import { RouteMap } from "./RouteMap";
import { SeriesChart } from "./SeriesChart";
import { LiveCard } from "./LiveCard";

export const TRACK_POINTS = 600;

const SPECS: SeriesSpec[] = [
  { key: "ele", title: "Elevation (m)", color: "#34d399", area: true, invert: false, pad: 4, format: (v) => String(Math.round(v)), unit: "m" },
  { key: "pace", title: "Pace (min/km)", color: "#4f8ef7", area: true, invert: true, pad: 0.3, format: (v) => fmtPace(v * 60), unit: "/km" },
  { key: "cad", title: "Run Cadence (spm)", color: "#a78bfa", area: false, invert: false, pad: 8, format: (v) => String(Math.round(v)), unit: "spm" },
  { key: "temp", title: "Temperature (°C)", color: "#f59e0b", area: true, invert: false, pad: 1, format: (v) => v.toFixed(1), unit: "°C" },
];

export function ActivityView() {
  const { id } = useParams();
  const navigate = useNavigate();
  const all = useActivities();
  const list = all.data ?? [];
  const current = id ?? list[list.length - 1]?.id;
  const activity = useActivity(current);
  const track = useTrack(current, TRACK_POINTS);
  const prefs = usePrefs();

  const series = useMemo(
    () => (activity.data && track.data ? buildSeries(activity.data, track.data) : null),
    [activity.data, track.data],
  );
  const t = series?.t ?? EMPTY;
  const pb = usePlayback(t, prefs.speed);

  const index = list.findIndex((a) => a.id === current);
  const step = (n: number) => {
    const next = list[index + n];
    if (next) navigate(`/activity/${next.id}`);
  };

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const tag = (e.target as HTMLElement).tagName;
      if (tag === "INPUT" || tag === "SELECT" || tag === "TEXTAREA") return;
      if (e.key === " ") {
        e.preventDefault();
        pb.toggle();
      } else if (e.key === "ArrowRight") {
        pb.stop();
        pb.goTo(pb.idx + 1);
      } else if (e.key === "ArrowLeft") {
        pb.stop();
        pb.goTo(pb.idx - 1);
      } else if (e.key === "PageDown") step(1);
      else if (e.key === "PageUp") step(-1);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });

  if (all.isPending || activity.isPending || track.isPending) return <p className="text-muted">Loading…</p>;
  if (all.isError) return <p className="text-danger">Could not load activities: {all.error.message}</p>;
  if (!activity.data || !series) return <p className="text-danger">Activity not found.</p>;
  const a = activity.data;
  const hover = (i: number) => {
    pb.stop();
    pb.goTo(i);
  };
  const avg = {
    ele: Number.isFinite(mean(series.ele) ?? NaN) ? `avg ${Math.round(mean(series.ele)!)} m` : undefined,
    pace: `avg ${fmtPace(a.paceSecPerKm)} /km`,
    cad: a.avgCadence ? `avg ${Math.round(a.avgCadence)} spm` : undefined,
    temp: a.avgTempC != null ? `avg ${a.avgTempC.toFixed(1)} °C` : undefined,
  };

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold">
            {series.hasGps ? "Outdoor" : "Indoor"} Run · Day {a.dayOfYear}
          </h1>
          <div className="text-xs text-muted">
            {longDate(a.date)} <b className="text-text">{a.startTime}</b> · activity <b className="text-text">{a.id}</b>
            {a.warnings.length > 0 && (
              <>
                {" "}
                · <WarningIcons signals={a.warnings} dash={false} />
              </>
            )}
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <span className="rounded bg-surface2 px-2 py-1 text-xs" data-testid="act-wx">
            {a.weather
              ? `${wxEmoji(a.weather.description)} ${a.weather.description} · ${a.weather.tempC}°C · ${a.weather.humidityPct}% RH`
              : "no weather record"}
          </span>
          <div className="flex items-center gap-1">
            <button type="button" aria-label="Previous day" className="rounded border border-border px-2 py-1 hover:bg-surface2" onClick={() => step(-1)} disabled={index <= 0}>
              ‹
            </button>
            <select
              aria-label="Choose activity"
              className="rounded border border-border bg-surface px-2 py-1 text-xs"
              value={current}
              onChange={(e) => navigate(`/activity/${e.target.value}`)}
            >
              {list.map((x) => (
                <option key={x.id} value={x.id}>
                  {fmtShortDate(x.date)} · {fmtKm(x.distanceKm)} km
                </option>
              ))}
            </select>
            <button type="button" aria-label="Next day" className="rounded border border-border px-2 py-1 hover:bg-surface2" onClick={() => step(1)} disabled={index >= list.length - 1}>
              ›
            </button>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3 md:grid-cols-5" data-testid="act-kpis">
        <KpiCard label="Distance" value={fmtKm(a.distanceKm)} unit="km" accent="accent" />
        <KpiCard label="Time" value={fmtDuration(a.durationSec)} accent="accent2" />
        <KpiCard label="Avg Pace" value={fmtPace(a.paceSecPerKm)} unit="/km" accent="warn" />
        <KpiCard label="Total Ascent" value={a.ascentM != null ? String(Math.round(a.ascentM)) : "–"} unit="m" accent="violet" />
        <KpiCard label="Calories" value={a.calories != null ? String(a.calories) : "–"} unit="kcal" accent="danger" />
      </div>

      <div className="grid gap-4 lg:grid-cols-[1fr_300px]">
        <RouteMap series={series} idx={pb.idx} onHover={hover} />
        <LiveCard series={series} idx={pb.idx} playing={pb.playing} speed={pb.speed} onToggle={pb.toggle} onSpeed={pb.setSpeed} onScrub={hover} />
      </div>

      <div className="grid gap-4 md:grid-cols-2">
        {SPECS.map((spec) => (
          <SeriesChart key={spec.key} series={series} spec={spec} idx={pb.idx} average={avg[spec.key]} onHover={hover} />
        ))}
      </div>
    </div>
  );
}

const EMPTY: number[] = [];
