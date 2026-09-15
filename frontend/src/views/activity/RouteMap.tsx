import { useCallback, useEffect, useRef } from "react";
import type { TrackSeries } from "./series";
import { project, type Projection } from "./projection";
import { PACE_RAMP, paceColor } from "@/lib/paceColor";
import { usePrefs } from "@/lib/prefs";
import { FONT_SANS, TOKENS, alpha } from "@/styles/tokens";

const HOVER_RADIUS_PX = 18;
/** Pure white, so the current-position dot reads against any pace colour under
 *  it. Not a theme colour: it has to stay maximum contrast if the theme moves. */
const MARKER_RING = "#fff";

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
  const projectionRef = useRef<Projection | null>(null);
  const scale = useRef<HTMLDivElement>(null);

  const draw = useCallback(() => {
    const canvasEl = canvas.current;
    const projection = projectionRef.current;
    const ctx = canvasEl?.getContext("2d");
    if (!canvasEl || !projection || !ctx) return;
    const dpr = window.devicePixelRatio || 1;
    if (canvasEl.width !== projection.w * dpr) {
      canvasEl.width = projection.w * dpr;
      canvasEl.height = projection.h * dpr;
    }
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, projection.w, projection.h);
    ctx.strokeStyle = TOKENS.surface2;
    ctx.lineWidth = 1;
    ctx.beginPath();
    for (let x = 0; x < projection.w; x += 40) {
      ctx.moveTo(x, 0);
      ctx.lineTo(x, projection.h);
    }
    for (let y = 0; y < projection.h; y += 40) {
      ctx.moveTo(0, y);
      ctx.lineTo(projection.w, y);
    }
    ctx.stroke();
    ctx.lineWidth = 3;
    ctx.lineJoin = "round";
    ctx.lineCap = "round";
    ctx.strokeStyle = TOKENS.border;
    ctx.beginPath();
    projection.xy.forEach(([x, y], i) => (i ? ctx.lineTo(x, y) : ctx.moveTo(x, y)));
    ctx.stroke();
    ctx.lineWidth = 4;
    for (let i = 1; i <= idx; i++) {
      ctx.strokeStyle = paceColor(series.pace[i], paceFast, paceSlow);
      ctx.beginPath();
      ctx.moveTo(projection.xy[i - 1][0], projection.xy[i - 1][1]);
      ctx.lineTo(projection.xy[i][0], projection.xy[i][1]);
      ctx.stroke();
    }
    ctx.font = `600 10px ${FONT_SANS}`;
    ctx.textBaseline = "middle";
    const [startX, startY] = projection.xy[0];
    ctx.beginPath();
    ctx.arc(startX, startY, 6, 0, Math.PI * 2);
    ctx.fillStyle = TOKENS.surface;
    ctx.fill();
    ctx.strokeStyle = TOKENS.accent2;
    ctx.lineWidth = 2;
    ctx.stroke();
    ctx.fillStyle = TOKENS.text;
    ctx.fillText("START", startX + 11, startY);
    const [endX, endY] = projection.xy[series.n - 1];
    ctx.fillStyle = TOKENS.danger;
    ctx.fillRect(endX - 5, endY - 5, 10, 10);
    ctx.fillStyle = TOKENS.text;
    ctx.fillText("FINISH", endX + 11, endY);
    const [markerX, markerY] = projection.xy[idx];
    ctx.fillStyle = alpha(TOKENS.accent, 0.22);
    ctx.beginPath();
    ctx.arc(markerX, markerY, 15, 0, Math.PI * 2);
    ctx.fill();
    ctx.fillStyle = TOKENS.accent;
    ctx.beginPath();
    ctx.arc(markerX, markerY, 7, 0, Math.PI * 2);
    ctx.fill();
    ctx.strokeStyle = MARKER_RING;
    ctx.lineWidth = 2;
    ctx.stroke();
  }, [idx, series, paceFast, paceSlow]);

  const layout = useCallback(() => {
    const el = wrap.current;
    if (!el) return;
    projectionRef.current = series.hasGps ? project(series, el.clientWidth, el.clientHeight) : null;
    if (scale.current && projectionRef.current) {
      (scale.current.firstElementChild as HTMLElement).style.width =
        `${projectionRef.current.scaleBarPx.toFixed(0)}px`;
      scale.current.lastElementChild!.textContent = `${projectionRef.current.scaleBarM} m`;
    }
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
    const pointerX = e.clientX - box.left;
    const pointerY = e.clientY - box.top;
    let best = -1;
    let bestDistSq = HOVER_RADIUS_PX * HOVER_RADIUS_PX;
    projection.xy.forEach(([x, y], i) => {
      const distSq = (x - pointerX) ** 2 + (y - pointerY) ** 2;
      if (distSq < bestDistSq) {
        bestDistSq = distSq;
        best = i;
      }
    });
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
      {series.hasGps && (
        <div
          ref={scale}
          className="absolute bottom-3 left-3 flex items-center gap-2 text-[10px] text-muted"
        >
          <i className="block h-0.5 bg-text" style={{ width: 100 }} />
          <span>200 m</span>
        </div>
      )}
      <div className="absolute bottom-3 right-3 flex items-center gap-2 text-[10px] text-muted">
        <span>Slower</span>
        <i
          className="block h-1.5 w-20 rounded"
          style={{ background: `linear-gradient(90deg,${PACE_RAMP.join(",")})` }}
        />
        <span>Faster</span>
      </div>
    </div>
  );
}
