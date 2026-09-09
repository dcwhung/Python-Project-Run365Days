import { useCallback, useEffect, useRef } from "react";
import type { SeriesSpec, TrackSeries } from "./series";
import { indexAtTime, seriesRange } from "./series";
import { fmtDuration, fmtPace } from "@/lib/format";

const L = 44;
const R = 8;
const TOP = 6;
const BOT = 18;
const HEIGHT = 120;

/** Canvas line chart with a cursor at `idx`; hover moves the cursor via onHover. */
export function SeriesChart({
  series,
  spec,
  idx,
  average,
  onHover,
}: {
  series: TrackSeries;
  spec: SeriesSpec;
  idx: number;
  average?: string;
  onHover: (i: number) => void;
}) {
  const canvas = useRef<HTMLCanvasElement>(null);
  const data = series[spec.key];
  const value = Number.isFinite(data[idx]) ? spec.format(data[idx]) : "–";

  const draw = useCallback(() => {
    const cv = canvas.current;
    const g = cv?.getContext("2d");
    if (!cv || !g) return;
    const dpr = window.devicePixelRatio || 1;
    const W = cv.clientWidth;
    const H = cv.clientHeight;
    if (!W) return;
    if (cv.width !== W * dpr) {
      cv.width = W * dpr;
      cv.height = H * dpr;
    }
    g.setTransform(dpr, 0, 0, dpr, 0, 0);
    g.clearRect(0, 0, W, H);
    const iw = W - L - R;
    const ih = H - TOP - BOT;
    if (!data.some(Number.isFinite)) {
      g.fillStyle = "#7c85a8";
      g.font = '12px "Segoe UI",system-ui,sans-serif';
      g.textAlign = "center";
      g.fillText("no data for this run", W / 2, H / 2);
      return;
    }
    const { lo, hi } = seriesRange(data, spec);
    const X = (i: number) => L + (series.t[i] / series.totalSec) * iw;
    const Y = (v: number) => {
      let t = (v - lo) / (hi - lo);
      if (spec.invert) t = 1 - t;
      return TOP + ih - t * ih;
    };
    g.strokeStyle = "#2e3250";
    g.lineWidth = 1;
    g.fillStyle = "#7c85a8";
    g.font = '500 10px "Segoe UI",system-ui,sans-serif';
    g.textAlign = "right";
    g.textBaseline = "middle";
    for (const v of [lo, (lo + hi) / 2, hi]) {
      const y = Y(v);
      g.beginPath();
      g.moveTo(L, y);
      g.lineTo(W - R, y);
      g.stroke();
      g.fillText(spec.key === "pace" ? fmtPace(v * 60) : v.toFixed(spec.key === "temp" ? 1 : 0), L - 6, y);
    }
    g.textAlign = "center";
    g.textBaseline = "top";
    const step = series.totalSec > 3600 ? 600 : 300;
    for (let m = 0; m <= series.totalSec; m += step) g.fillText(fmtDuration(m), L + (m / series.totalSec) * iw, H - BOT + 5);
    const trace = () => {
      g.beginPath();
      let started = false;
      for (let i = 0; i < series.n; i++) {
        if (!Number.isFinite(data[i])) continue;
        const x = X(i);
        const y = Y(data[i]);
        if (started) g.lineTo(x, y);
        else g.moveTo(x, y);
        started = true;
      }
    };
    if (spec.area) {
      trace();
      g.lineTo(X(series.n - 1), TOP + ih);
      g.lineTo(L, TOP + ih);
      g.closePath();
      g.fillStyle = `${spec.color}22`;
      g.fill();
    }
    trace();
    g.strokeStyle = spec.color;
    g.lineWidth = spec.key === "cad" ? 1 : 1.5;
    g.stroke();
    const x = X(idx);
    g.strokeStyle = "#e2e8f0";
    g.lineWidth = 1;
    g.beginPath();
    g.moveTo(x, TOP);
    g.lineTo(x, TOP + ih);
    g.stroke();
    if (Number.isFinite(data[idx])) {
      g.fillStyle = spec.color;
      g.beginPath();
      g.arc(x, Y(data[idx]), 4, 0, Math.PI * 2);
      g.fill();
      g.strokeStyle = "#1a1d27";
      g.lineWidth = 1.5;
      g.stroke();
    }
  }, [data, idx, series, spec]);

  useEffect(() => {
    draw();
    window.addEventListener("resize", draw);
    return () => window.removeEventListener("resize", draw);
  }, [draw]);

  const onPointerMove = (e: React.PointerEvent<HTMLCanvasElement>) => {
    const r = e.currentTarget.getBoundingClientRect();
    const sec = ((e.clientX - r.left - L) / (r.width - L - R)) * series.totalSec;
    onHover(indexAtTime(series.t, sec));
  };

  return (
    <div className="relative rounded-card border border-border bg-surface p-3">
      <div className="mb-1 flex items-center justify-between text-xs">
        <h3 className="font-semibold">{spec.title}</h3>
        {average && <span className="text-muted">{average}</span>}
      </div>
      <canvas ref={canvas} className="block w-full" style={{ height: HEIGHT }} onPointerMove={onPointerMove} aria-label={spec.title} />
      <div className="absolute right-3 top-8 text-sm font-semibold" style={{ color: spec.color }} data-testid={`value-${spec.key}`}>
        {value}
        {value !== "–" && <small className="ml-0.5 text-[10px] text-muted">{spec.unit}</small>}
      </div>
    </div>
  );
}
