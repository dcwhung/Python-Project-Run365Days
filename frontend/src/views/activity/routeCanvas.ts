import type { TrackSeries } from "./series";
import type { Projection } from "./projection";
import { paceColor } from "@/lib/paceColor";
import { FONT_SANS, TOKENS, alpha } from "@/styles/tokens";

/** How far from a track point a pointer still counts as hovering it. */
export const HOVER_RADIUS_PX = 18;

/** Pure white, so the current-position dot reads against any pace colour under
 *  it. Not a theme colour: it has to stay maximum contrast if the theme moves. */
const MARKER_RING = "#fff";

const GRID_STEP_PX = 40;

export interface RoutePaint {
  projection: Projection;
  series: TrackSeries;
  /** Index of the playback cursor; the track is coloured up to here. */
  idx: number;
  paceFast: number;
  paceSlow: number;
}

/** The faint 40px graticule the track sits on. */
function drawGrid(ctx: CanvasRenderingContext2D, projection: Projection) {
  ctx.strokeStyle = TOKENS.surface2;
  ctx.lineWidth = 1;
  ctx.beginPath();
  for (let x = 0; x < projection.w; x += GRID_STEP_PX) {
    ctx.moveTo(x, 0);
    ctx.lineTo(x, projection.h);
  }
  for (let y = 0; y < projection.h; y += GRID_STEP_PX) {
    ctx.moveTo(0, y);
    ctx.lineTo(projection.w, y);
  }
  ctx.stroke();
}

/** The whole route in the border colour, then the run so far in pace colours. */
function drawTrack(ctx: CanvasRenderingContext2D, paint: RoutePaint) {
  const { projection, series, idx, paceFast, paceSlow } = paint;
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
}

/** Ringed dot at the start, filled square at the finish, both labelled. */
function drawEndpoints(ctx: CanvasRenderingContext2D, projection: Projection, lastIndex: number) {
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
  const [endX, endY] = projection.xy[lastIndex];
  ctx.fillStyle = TOKENS.danger;
  ctx.fillRect(endX - 5, endY - 5, 10, 10);
  ctx.fillStyle = TOKENS.text;
  ctx.fillText("FINISH", endX + 11, endY);
}

/**
 * The playback cursor: a soft halo, a solid dot, and a white rim on the dot.
 *
 * "Rim" is chosen for a reason worth knowing before editing any comment in
 * src/: Tailwind scans source text -- prose in comments included -- for
 * class-name candidates, so an ordinary English word that happens to be a
 * bare utility name ships that utility to every visitor. The two obvious
 * words for this rim are both bare utility names; each was measured at 255
 * and 65 bytes of otherwise unused CSS.
 */
function drawMarker(ctx: CanvasRenderingContext2D, projection: Projection, idx: number) {
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
}

/**
 * Paints one frame of the route map, sizing the backing store for the display's
 * pixel ratio first. Everything here is pure drawing against the projection it
 * is handed; nothing reads the DOM beyond the canvas it paints.
 */
export function drawRoute(canvasEl: HTMLCanvasElement, paint: RoutePaint) {
  const ctx = canvasEl.getContext("2d");
  if (!ctx) return;
  const { projection, series, idx } = paint;
  const dpr = window.devicePixelRatio || 1;
  if (canvasEl.width !== projection.w * dpr) {
    canvasEl.width = projection.w * dpr;
    canvasEl.height = projection.h * dpr;
  }
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, projection.w, projection.h);
  drawGrid(ctx, projection);
  drawTrack(ctx, paint);
  drawEndpoints(ctx, projection, series.n - 1);
  drawMarker(ctx, projection, idx);
}

/**
 * Index of the projected track point nearest the pointer, or -1 when nothing is
 * within HOVER_RADIUS_PX of it.
 */
export function nearestPointIndex(
  projection: Projection,
  pointerX: number,
  pointerY: number,
): number {
  let best = -1;
  let bestDistSq = HOVER_RADIUS_PX * HOVER_RADIUS_PX;
  projection.xy.forEach(([x, y], i) => {
    const distSq = (x - pointerX) ** 2 + (y - pointerY) ** 2;
    if (distSq < bestDistSq) {
      bestDistSq = distSq;
      best = i;
    }
  });
  return best;
}
