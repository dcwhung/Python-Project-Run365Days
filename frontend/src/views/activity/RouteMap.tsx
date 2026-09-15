import { useCallback, useEffect, useRef, useState } from "react";
import type { TrackSeries } from "./series";
import { project, type Projection } from "./projection";
import { drawRoute, nearestPointIndex } from "./routeCanvas";
import { PACE_RAMP } from "@/lib/paceColor";
import { usePrefs } from "@/lib/prefs";

/** What the scale bar shows before the first layout has measured the box. */
const PLACEHOLDER_SCALE = { px: 100, metres: 200 };

/** The distance legend in the bottom-left corner, sized by the live projection. */
function ScaleBar({ px, metres }: { px: number; metres: number }) {
  return (
    <div className="absolute bottom-3 left-3 flex items-center gap-2 text-[10px] text-muted">
      <i className="block h-0.5 bg-text" style={{ width: `${px.toFixed(0)}px` }} />
      <span>{metres} m</span>
    </div>
  );
}

/** The slow-to-fast colour ramp in the bottom-right corner. */
function PaceLegend() {
  return (
    <div className="absolute bottom-3 right-3 flex items-center gap-2 text-[10px] text-muted">
      <span>Slower</span>
      <i
        className="block h-1.5 w-20 rounded"
        style={{ background: `linear-gradient(90deg,${PACE_RAMP.join(",")})` }}
      />
      <span>Faster</span>
    </div>
  );
}

/**
 * The route canvas. This component owns only the React side of the map -- the
 * element refs, measuring the box on mount and on resize, and turning a pointer
 * position into a track index. The painting itself lives in routeCanvas.ts.
 */
export function RouteMap({
  series,
  idx,
  onHover,
}: {
  series: TrackSeries;
  idx: number;
  onHover: (i: number) => void;
}) {
  const { paceFast, paceSlow } = usePrefs();
  const wrap = useRef<HTMLDivElement>(null);
  const canvas = useRef<HTMLCanvasElement>(null);
  // The projection is read by every frame and by every pointer move, so it
  // stays in a ref: re-rendering on it would repaint the whole card per frame.
  const projectionRef = useRef<Projection | null>(null);
  // The scale bar, in contrast, is two numbers React can render itself.
  const [scale, setScale] = useState(PLACEHOLDER_SCALE);

  const draw = useCallback(() => {
    const canvasEl = canvas.current;
    const projection = projectionRef.current;
    if (!canvasEl || !projection) return;
    drawRoute(canvasEl, { projection, series, idx, paceFast, paceSlow });
  }, [idx, series, paceFast, paceSlow]);

  const layout = useCallback(() => {
    const el = wrap.current;
    if (!el) return;
    const projection = series.hasGps ? project(series, el.clientWidth, el.clientHeight) : null;
    projectionRef.current = projection;
    if (projection) setScale({ px: projection.scaleBarPx, metres: projection.scaleBarM });
    draw();
  }, [series, draw]);

  useEffect(() => {
    layout();
    window.addEventListener("resize", layout);
    return () => window.removeEventListener("resize", layout);
  }, [layout]);

  useEffect(draw, [draw]);

  const onPointerMove = (e: React.PointerEvent<HTMLCanvasElement>) => {
    const projection = projectionRef.current;
    if (!projection) return;
    const box = e.currentTarget.getBoundingClientRect();
    const best = nearestPointIndex(projection, e.clientX - box.left, e.clientY - box.top);
    if (best >= 0) onHover(best);
  };

  return (
    <div
      ref={wrap}
      className="relative h-[380px] overflow-hidden rounded-card border border-border bg-surface"
      data-testid="route-map"
    >
      {series.hasGps ? (
        <canvas
          ref={canvas}
          className="block h-full w-full"
          onPointerMove={onPointerMove}
          aria-label="Route map"
        />
      ) : (
        <div className="flex h-full items-center justify-center px-6 text-center text-sm text-muted">
          No GPS track recorded for this run (treadmill / indoor). Charts below still play.
        </div>
      )}
      <div className="absolute left-3 top-3 text-[10px] uppercase tracking-wide text-muted">
        Route · GPS track
      </div>
      {series.hasGps && <ScaleBar px={scale.px} metres={scale.metres} />}
      <PaceLegend />
    </div>
  );
}
