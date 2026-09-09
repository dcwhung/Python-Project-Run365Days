import { useCallback, useEffect, useRef } from "react";

const HOVER_RADIUS_PX = 18;
import type { TrackSeries } from "./series";
import { project, type Projection } from "./projection";
import { paceColor } from "@/lib/paceColor";
import { usePrefs } from "@/lib/prefs";

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
  const proj = useRef<Projection | null>(null);
  const scale = useRef<HTMLDivElement>(null);

  const draw = useCallback(() => {
    const cv = canvas.current;
    const p = proj.current;
    const g = cv?.getContext("2d");
    if (!cv || !p || !g) return;
    const dpr = window.devicePixelRatio || 1;
    if (cv.width !== p.w * dpr) {
      cv.width = p.w * dpr;
      cv.height = p.h * dpr;
    }
    g.setTransform(dpr, 0, 0, dpr, 0, 0);
    g.clearRect(0, 0, p.w, p.h);
    g.strokeStyle = "#22263a";
    g.lineWidth = 1;
    g.beginPath();
    for (let x = 0; x < p.w; x += 40) {
      g.moveTo(x, 0);
      g.lineTo(x, p.h);
    }
    for (let y = 0; y < p.h; y += 40) {
      g.moveTo(0, y);
      g.lineTo(p.w, y);
    }
    g.stroke();
    g.lineWidth = 3;
    g.lineJoin = "round";
    g.lineCap = "round";
    g.strokeStyle = "#2e3250";
    g.beginPath();
    p.xy.forEach(([x, y], i) => (i ? g.lineTo(x, y) : g.moveTo(x, y)));
    g.stroke();
    g.lineWidth = 4;
    for (let i = 1; i <= idx; i++) {
      g.strokeStyle = paceColor(series.pace[i], paceFast, paceSlow);
      g.beginPath();
      g.moveTo(p.xy[i - 1][0], p.xy[i - 1][1]);
      g.lineTo(p.xy[i][0], p.xy[i][1]);
      g.stroke();
    }
    g.font = '600 10px "Segoe UI",system-ui,sans-serif';
    g.textBaseline = "middle";
    const [sx, sy] = p.xy[0];
    g.beginPath();
    g.arc(sx, sy, 6, 0, Math.PI * 2);
    g.fillStyle = "#1a1d27";
    g.fill();
    g.strokeStyle = "#34d399";
    g.lineWidth = 2;
    g.stroke();
    g.fillStyle = "#e2e8f0";
    g.fillText("START", sx + 11, sy);
    const [ex, ey] = p.xy[series.n - 1];
    g.fillStyle = "#f87171";
    g.fillRect(ex - 5, ey - 5, 10, 10);
    g.fillStyle = "#e2e8f0";
    g.fillText("FINISH", ex + 11, ey);
    const [cx, cy] = p.xy[idx];
    g.fillStyle = "rgba(79,142,247,.22)";
    g.beginPath();
    g.arc(cx, cy, 15, 0, Math.PI * 2);
    g.fill();
    g.fillStyle = "#4f8ef7";
    g.beginPath();
    g.arc(cx, cy, 7, 0, Math.PI * 2);
    g.fill();
    g.strokeStyle = "#fff";
    g.lineWidth = 2;
    g.stroke();
  }, [idx, series, paceFast, paceSlow]);

  const layout = useCallback(() => {
    const el = wrap.current;
    if (!el) return;
    proj.current = series.hasGps ? project(series, el.clientWidth, el.clientHeight) : null;
    if (scale.current && proj.current) {
      (scale.current.firstElementChild as HTMLElement).style.width = `${proj.current.scaleBarPx.toFixed(0)}px`;
      scale.current.lastElementChild!.textContent = `${proj.current.scaleBarM} m`;
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
    const p = proj.current;
    if (!p) return;
    const r = e.currentTarget.getBoundingClientRect();
    const px = e.clientX - r.left;
    const py = e.clientY - r.top;
    let best = -1;
    let bd = HOVER_RADIUS_PX * HOVER_RADIUS_PX;
    p.xy.forEach(([x, y], i) => {
      const d = (x - px) ** 2 + (y - py) ** 2;
      if (d < bd) {
        bd = d;
        best = i;
      }
    });
    if (best >= 0) onHover(best);
  };

  return (
    <div ref={wrap} className="relative h-[380px] overflow-hidden rounded-card border border-border bg-surface" data-testid="route-map">
      {series.hasGps ? (
        <canvas ref={canvas} className="block h-full w-full" onPointerMove={onPointerMove} aria-label="Route map" />
      ) : (
        <div className="flex h-full items-center justify-center px-6 text-center text-sm text-muted">
          No GPS track recorded for this run (treadmill / indoor). Charts below still play.
        </div>
      )}
      <div className="absolute left-3 top-3 text-[10px] uppercase tracking-wide text-muted">Route · GPS track</div>
      {series.hasGps && (
        <div ref={scale} className="absolute bottom-3 left-3 flex items-center gap-2 text-[10px] text-muted">
          <i className="block h-0.5 bg-text" style={{ width: 100 }} />
          <span>200 m</span>
        </div>
      )}
      <div className="absolute bottom-3 right-3 flex items-center gap-2 text-[10px] text-muted">
        <span>Slower</span>
        <i className="block h-1.5 w-20 rounded" style={{ background: "linear-gradient(90deg,#f87171,#f59e0b,#34d399)" }} />
        <span>Faster</span>
      </div>
    </div>
  );
}
