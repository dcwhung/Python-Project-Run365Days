import { useCallback, useEffect, useRef } from "react";
import type { SeriesSpec, TrackSeries } from "./series";
import { indexAtTime } from "./series";
import { PAD_LEFT, PAD_RIGHT, drawSeries } from "./seriesCanvas";

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
    if (canvas.current) drawSeries(canvas.current, series, spec, idx);
  }, [idx, series, spec]);

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
