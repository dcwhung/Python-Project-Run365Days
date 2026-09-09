import type { TrackSeries } from "./series";

const PAD = 60;
const METRES_PER_DEGREE = 111_320;

export interface Projection {
  w: number;
  h: number;
  xy: [number, number][];
  scaleBarPx: number;
  scaleBarM: number;
}

/** Equirectangular fit of the track into the canvas. */
export function project(series: TrackSeries, w: number, h: number): Projection | null {
  const pts = series.latLon.filter((p): p is [number, number] => p != null);
  if (!w || !h || !pts.length) return null;
  const lats = pts.map((p) => p[0]);
  const lons = pts.map((p) => p[1]);
  const la0 = Math.min(...lats);
  const la1 = Math.max(...lats);
  const lo0 = Math.min(...lons);
  const lo1 = Math.max(...lons);
  const cosL = Math.cos((((la0 + la1) / 2) * Math.PI) / 180);
  const spanX = Math.max((lo1 - lo0) * cosL, 1e-5);
  const spanY = Math.max(la1 - la0, 1e-5);
  const s = Math.min((w - PAD * 2) / spanX, (h - PAD * 2) / spanY);
  const ox = (w - spanX * s) / 2;
  const oy = (h - spanY * s) / 2;
  const xy = series.latLon.map((p) =>
    p ? ([ox + (p[1] - lo0) * cosL * s, h - oy - (p[0] - la0) * s] as [number, number]) : ([0, 0] as [number, number]),
  );
  const scaleBarM = (s * 200) / METRES_PER_DEGREE > 40 ? 200 : 500;
  return { w, h, xy, scaleBarPx: (scaleBarM / METRES_PER_DEGREE) * s, scaleBarM };
}

