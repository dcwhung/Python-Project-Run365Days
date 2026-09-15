import { useCallback, useEffect, useRef } from "react";
import type { SeriesSpec, TrackSeries } from "./series";
import { indexAtTime, seriesRange } from "./series";
import { fmtDuration, fmtPace } from "@/lib/format";
import { SEC_PER_HOUR, minToSec } from "@/lib/units";
import { FONT_SANS, TOKENS } from "@/styles/tokens";

/** Canvas padding: the left gutter holds the value labels, the bottom the clock. */
const PAD_LEFT = 44;
const PAD_RIGHT = 8;
const PAD_TOP = 6;
const PAD_BOTTOM = 18;
const CANVAS_HEIGHT_PX = 120;

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
    const canvasEl = canvas.current;
    const ctx = canvasEl?.getContext("2d");
    if (!canvasEl || !ctx) return;
    const dpr = window.devicePixelRatio || 1;
    const cssWidth = canvasEl.clientWidth;
    const cssHeight = canvasEl.clientHeight;
    if (!cssWidth) return;
    if (canvasEl.width !== cssWidth * dpr) {
      canvasEl.width = cssWidth * dpr;
      canvasEl.height = cssHeight * dpr;
    }
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, cssWidth, cssHeight);
    const plotWidth = cssWidth - PAD_LEFT - PAD_RIGHT;
    const plotHeight = cssHeight - PAD_TOP - PAD_BOTTOM;
    if (!data.some(Number.isFinite)) {
      ctx.fillStyle = TOKENS.muted;
      ctx.font = `12px ${FONT_SANS}`;
      ctx.textAlign = "center";
      ctx.fillText("no data for this run", cssWidth / 2, cssHeight / 2);
      return;
    }
    const { lo, hi } = seriesRange(data, spec);
    const xAt = (i: number) => PAD_LEFT + (series.t[i] / series.totalSec) * plotWidth;
    const yAt = (sample: number) => {
      let t = (sample - lo) / (hi - lo);
      if (spec.invert) t = 1 - t;
      return PAD_TOP + plotHeight - t * plotHeight;
    };
    ctx.strokeStyle = TOKENS.border;
    ctx.lineWidth = 1;
    ctx.fillStyle = TOKENS.muted;
    ctx.font = `500 10px ${FONT_SANS}`;
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    for (const gridValue of [lo, (lo + hi) / 2, hi]) {
      const y = yAt(gridValue);
      ctx.beginPath();
      ctx.moveTo(PAD_LEFT, y);
      ctx.lineTo(cssWidth - PAD_RIGHT, y);
      ctx.stroke();
      ctx.fillText(
        spec.key === "pace"
          ? fmtPace(minToSec(gridValue))
          : gridValue.toFixed(spec.key === "temp" ? 1 : 0),
        PAD_LEFT - 6,
        y,
      );
    }
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    // Ticks every ten minutes on a long run, every five on a short one.
    const step = minToSec(series.totalSec > SEC_PER_HOUR ? 10 : 5);
    for (let tickSec = 0; tickSec <= series.totalSec; tickSec += step)
      ctx.fillText(
        fmtDuration(tickSec),
        PAD_LEFT + (tickSec / series.totalSec) * plotWidth,
        cssHeight - PAD_BOTTOM + 5,
      );
    const trace = () => {
      ctx.beginPath();
      let started = false;
      for (let i = 0; i < series.n; i++) {
        if (!Number.isFinite(data[i])) continue;
        const x = xAt(i);
        const y = yAt(data[i]);
        if (started) ctx.lineTo(x, y);
        else ctx.moveTo(x, y);
        started = true;
      }
    };
    if (spec.area) {
      trace();
      ctx.lineTo(xAt(series.n - 1), PAD_TOP + plotHeight);
      ctx.lineTo(PAD_LEFT, PAD_TOP + plotHeight);
      ctx.closePath();
      ctx.fillStyle = `${spec.color}22`;
      ctx.fill();
    }
    trace();
    ctx.strokeStyle = spec.color;
    ctx.lineWidth = spec.key === "cad" ? 1 : 1.5;
    ctx.stroke();
    const x = xAt(idx);
    ctx.strokeStyle = TOKENS.text;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(x, PAD_TOP);
    ctx.lineTo(x, PAD_TOP + plotHeight);
    ctx.stroke();
    if (Number.isFinite(data[idx])) {
      ctx.fillStyle = spec.color;
      ctx.beginPath();
      ctx.arc(x, yAt(data[idx]), 4, 0, Math.PI * 2);
      ctx.fill();
      ctx.strokeStyle = TOKENS.surface;
      ctx.lineWidth = 1.5;
      ctx.stroke();
    }
  }, [data, idx, series, spec]);

  useEffect(() => {
    draw();
    window.addEventListener("resize", draw);
    return () => window.removeEventListener("resize", draw);
  }, [draw]);

  const onPointerMove = (e: React.PointerEvent<HTMLCanvasElement>) => {
    const box = e.currentTarget.getBoundingClientRect();
    const sec =
      ((e.clientX - box.left - PAD_LEFT) / (box.width - PAD_LEFT - PAD_RIGHT)) * series.totalSec;
    onHover(indexAtTime(series.t, sec));
  };

  return (
    <div className="relative rounded-card border border-border bg-surface p-3">
      <div className="mb-1 flex items-center justify-between text-xs">
        <h3 className="font-semibold">{spec.title}</h3>
        {average && <span className="text-muted">{average}</span>}
      </div>
      <canvas
        ref={canvas}
        className="block w-full"
        style={{ height: CANVAS_HEIGHT_PX }}
        onPointerMove={onPointerMove}
        aria-label={spec.title}
      />
      <div
        className="absolute right-3 top-8 text-sm font-semibold"
        style={{ color: spec.color }}
        data-testid={`value-${spec.key}`}
      >
        {value}
        {value !== "–" && <small className="ml-0.5 text-[10px] text-muted">{spec.unit}</small>}
      </div>
    </div>
  );
}
