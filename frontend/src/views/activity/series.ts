import type { Activity, TrackPoint } from "@/data/types";
import { fmtPace } from "@/lib/format";
import { smooth } from "@/lib/stats-helpers";
import { minToSec } from "@/lib/units";
import { TOKENS } from "@/styles/tokens";

/** Derived, chart-ready arrays for one run. Pure so it can be unit-tested. */
export interface TrackSeries {
  n: number;
  hasGps: boolean;
  /** Seconds from start. */
  t: number[];
  /** Smoothed elevation in metres (NaN where missing). */
  ele: number[];
  /** Cumulative distance in metres. */
  dist: number[];
  /** Cadence in steps per minute (both feet), NaN under the walking floor. */
  cad: number[];
  /** Ambient temperature in Celsius (NaN where missing). */
  temp: number[];
  /** Smoothed pace in min/km (NaN when stationary). */
  pace: number[];
  totalSec: number;
  /** [lat, lon] per point, holes filled from the last known position. */
  latLon: ([number, number] | null)[];
}

export const CADENCE_FLOOR = 50;
export const STEPS_PER_CADENCE_SAMPLE = 2;
const MIN_MOVING_SPEED_MPS = 0.5;
const ELE_SMOOTH = 2;
const PACE_SMOOTH = 1;

const nn = (v: number | null | undefined) => (v == null ? NaN : v);

export function buildSeries(activity: Activity, track: TrackPoint[]): TrackSeries {
  const n = track.length;
  const t = track.map((p) => p.sec);
  const hasGps = activity.hasGps && track.some((p) => p.lat != null);
  let acc = 0;
  const dist = track.map((p) => {
    if (p.distanceM != null) acc = p.distanceM;
    return acc;
  });
  const latLon: TrackSeries["latLon"] = [];
  let last: [number, number] | null = null;
  for (const p of track) {
    if (p.lat != null && p.lon != null) last = [p.lat, p.lon];
    latLon.push(last);
  }
  for (let i = latLon.length - 1, next: [number, number] | null = null; i >= 0; i--) {
    if (latLon[i]) next = latLon[i];
    else latLon[i] = next;
  }
  return {
    n,
    hasGps,
    t,
    ele: smooth(
      track.map((p) => nn(p.elevationM)),
      ELE_SMOOTH,
    ),
    dist,
    cad: track.map((p) =>
      p.cadence != null && p.cadence >= CADENCE_FLOOR ? p.cadence * STEPS_PER_CADENCE_SAMPLE : NaN,
    ),
    temp: track.map((p) => nn(p.tempC)),
    pace: smooth(
      track.map((p) =>
        p.speedMps != null && p.speedMps > MIN_MOVING_SPEED_MPS ? 1000 / p.speedMps / 60 : NaN,
      ),
      PACE_SMOOTH,
    ),
    totalSec: activity.durationSec || t[n - 1] || 1,
    latLon,
  };
}

/** Index of the first sample at or after `sec`, clamped to the last sample. */
export function indexAtTime(t: number[], sec: number): number {
  const i = t.findIndex((v) => v >= sec);
  return i < 0 ? Math.max(0, t.length - 1) : i;
}

export interface SeriesSpec {
  key: "ele" | "pace" | "cad" | "temp";
  title: string;
  color: string;
  area: boolean;
  invert: boolean;
  pad: number;
  format: (v: number) => string;
  unit: string;
}

export function seriesRange(values: number[], spec: SeriesSpec): { lo: number; hi: number } {
  const v = values.filter(Number.isFinite);
  let lo = v.length ? Math.min(...v) - spec.pad : 0;
  let hi = v.length ? Math.max(...v) + spec.pad : 1;
  if (spec.key === "pace") {
    lo = Math.max(3, lo);
    hi = Math.min(9, Math.max(hi, lo + 1));
  }
  return { lo, hi };
}

/**
 * The four traces an activity page plots, in the order they appear. Which
 * measures a run is described by, what unit each is read in and how tight its
 * axis sits are facts about the sport, not about the page that draws them --
 * they belong next to the series they configure.
 */
export const SERIES_SPECS: SeriesSpec[] = [
  {
    key: "ele",
    title: "Elevation (m)",
    color: TOKENS.accent2,
    area: true,
    invert: false,
    pad: 4,
    format: (v) => String(Math.round(v)),
    unit: "m",
  },
  {
    key: "pace",
    title: "Pace (min/km)",
    color: TOKENS.accent,
    area: true,
    invert: true,
    pad: 0.3,
    format: (v) => fmtPace(minToSec(v)),
    unit: "/km",
  },
  {
    key: "cad",
    title: "Run Cadence (spm)",
    color: TOKENS.violet,
    area: false,
    invert: false,
    pad: 8,
    format: (v) => String(Math.round(v)),
    unit: "spm",
  },
  {
    key: "temp",
    title: "Temperature (°C)",
    color: TOKENS.warn,
    area: true,
    invert: false,
    pad: 1,
    format: (v) => v.toFixed(1),
    unit: "°C",
  },
];
