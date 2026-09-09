import type { TrackSeries } from "./series";
import { SPEEDS } from "./usePlayback";
import { fmtDuration, fmtPace } from "@/lib/format";

function Stat({ value, unit, label }: { value: string; unit: string; label: string }) {
  return (
    <div>
      <div className="text-lg font-semibold">
        {value}
        <small className="ml-0.5 text-[10px] text-muted">{unit}</small>
      </div>
      <div className="text-[10px] uppercase tracking-wide text-muted">{label}</div>
    </div>
  );
}

export function LiveCard({
  series,
  idx,
  playing,
  speed,
  onToggle,
  onSpeed,
  onScrub,
}: {
  series: TrackSeries;
  idx: number;
  playing: boolean;
  speed: number;
  onToggle: () => void;
  onSpeed: (s: number) => void;
  onScrub: (i: number) => void;
}) {
  const f = (v: number, digits = 0) => (Number.isFinite(v) ? v.toFixed(digits) : "–");
  return (
    <aside className="flex flex-col gap-4 rounded-card border border-border bg-surface p-4" data-testid="live">
      <div>
        <div className="text-[10px] uppercase tracking-wide text-muted">Elapsed</div>
        <div className="text-3xl font-semibold tabular-nums" data-testid="elapsed">
          {fmtDuration(series.t[idx] ?? 0)}
        </div>
      </div>
      <div className="grid grid-cols-3 gap-3">
        <Stat value={((series.dist[idx] ?? 0) / 1000).toFixed(2)} unit="km" label="Distance" />
        <Stat value={Number.isFinite(series.pace[idx]) ? fmtPace(series.pace[idx] * 60) : "–"} unit="/km" label="Pace" />
        <Stat value={f(series.ele[idx])} unit="m" label="Elevation" />
        <Stat value={f(series.cad[idx])} unit="spm" label="Cadence" />
        <Stat value={f(series.temp[idx], 1)} unit="°C" label="Temp" />
        <Stat value={String(Math.round(((series.t[idx] ?? 0) / series.totalSec) * 100))} unit="%" label="Progress" />
      </div>
      <div className="flex items-center gap-3">
        <button
          type="button"
          onClick={onToggle}
          aria-label={playing ? "Pause" : "Play"}
          className="flex h-10 w-10 items-center justify-center rounded-full bg-accent text-bg hover:brightness-110"
        >
          <svg viewBox="0 0 16 16" className="h-4 w-4 fill-current">
            {playing ? <path d="M3 2h4v12H3zM9 2h4v12H9z" /> : <path d="M3 2l11 6-11 6z" />}
          </svg>
        </button>
        <div role="group" aria-label="Playback speed" className="flex overflow-hidden rounded border border-border text-xs">
          {SPEEDS.map((s) => (
            <button
              key={s}
              type="button"
              aria-pressed={speed === s}
              onClick={() => onSpeed(s)}
              className={`px-2.5 py-1 ${speed === s ? "bg-accent/20 text-accent" : "text-muted hover:bg-surface2"}`}
            >
              {s}×
            </button>
          ))}
        </div>
      </div>
      <input
        type="range"
        min={0}
        max={Math.max(0, series.n - 1)}
        value={idx}
        onChange={(e) => onScrub(Number(e.target.value))}
        aria-label="Position in activity"
        className="w-full accent-accent"
      />
      <p className="text-[11px] leading-relaxed text-muted">
        Play to run the route, drag to scrub, or hover any chart or the route to jump to that moment. Space and the arrow
        keys also work.
      </p>
    </aside>
  );
}
