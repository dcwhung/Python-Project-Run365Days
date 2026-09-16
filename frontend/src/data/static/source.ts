import type { Activity, ActivityFilter, DataSource, DateRange, TrackPoint } from "../types";
import { downsample } from "../../lib/downsample";
import { yearSummary } from "../stats";
import {
  mapActivity,
  mapMeta,
  mapTrack,
  mapWarning,
  mapWeather,
  mapWeight,
  type StaticActivity,
  type StaticMeta,
  type StaticTrack,
  type StaticWarning,
  type StaticWeather,
  type StaticWeight,
} from "./mappers";

export type Fetcher = (url: string) => Promise<unknown>;

const defaultFetcher: Fetcher = async (url) => {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${url}: HTTP ${res.status}`);
  return res.json();
};

const inRange = (date: string, range: DateRange) =>
  (!range.fromDate || date >= range.fromDate) && (!range.toDate || date <= range.toDate);

/**
 * Ceiling for `track(points)`, mirroring `MAX_TRACK_POINTS` in `src/api/schema.py`.
 *
 * Static mode has no server to protect, so this is not here as a DoS bound the
 * way it is on the api side. It is here so the two modes answer the same
 * question the same way: a client that computes a `points` and sends it should
 * not have to know which deployment it is talking to before it knows whether
 * the value is legal (CUI-0025).
 */
export const MAX_TRACK_POINTS = 1000;

/**
 * Bounds-check `points` the way api mode's `_track_points` does, or throw.
 *
 * Rejects the same values and says the same sentence, so a client writes one
 * error path rather than one per mode. Non-integers are refused too: api mode
 * cannot even express one (its SDL argument is `Int!`), and `downsample`'s
 * contract explicitly does not cover a non-integer `limit` -- Python raises on
 * `range(2.5)` where this side would quietly return two items.
 */
function checkPoints(points: number): number {
  if (!Number.isInteger(points) || points < 1 || points > MAX_TRACK_POINTS) {
    throw new RangeError(`points must be between 1 and ${MAX_TRACK_POINTS}, got ${points}`);
  }
  return points;
}

/**
 * DataSource over the JSON files written by `run365-export --static-dir`.
 * Filtering and aggregation happen in the browser; each file is fetched
 * once and memoised for the page's lifetime.
 */
export function createStaticSource(base: string, fetcher: Fetcher = defaultFetcher): DataSource {
  const cache = new Map<string, Promise<unknown>>();
  const load = <T>(file: string): Promise<T> => {
    const url = `${base}/${file}`;
    if (!cache.has(url)) cache.set(url, fetcher(url));
    return cache.get(url) as Promise<T>;
  };
  const allActivities = async (): Promise<Activity[]> =>
    (await load<StaticActivity[]>("activities.json")).map(mapActivity);

  return {
    mode: "static",
    async meta() {
      return mapMeta(await load<StaticMeta>("meta.json"));
    },
    async year() {
      const [meta, acts] = await Promise.all([this.meta(), allActivities()]);
      return yearSummary(acts, meta.year);
    },
    async activities(filter: ActivityFilter = {}) {
      return (await allActivities()).filter(
        (a) =>
          inRange(a.date, filter) &&
          (filter.minKm == null || a.distanceKm >= filter.minKm) &&
          (filter.hasGps == null || a.hasGps === filter.hasGps),
      );
    },
    async activity(id: string) {
      return (await allActivities()).find((a) => a.id === id) ?? null;
    },
    async track(id: string, points?: number): Promise<TrackPoint[]> {
      // Checked before the fetch, so an illegal `points` costs no request.
      // `undefined` is the one value that skips the check rather than failing
      // it: omitting the argument still means "the whole stored track", which
      // api mode has no way to ask for (`points: Int! = 150` is NonNull with a
      // default) and so has no answer to disagree with. `0` used to land here
      // too, because it is falsy; that is the divergence CUI-0025 closed.
      const limit = points === undefined ? undefined : checkPoints(points);
      const rows = mapTrack(await load<StaticTrack>(`tracks/${id}.json`));
      return limit === undefined ? rows : downsample(rows, limit);
    },
    async weight(range: DateRange = {}) {
      return (await load<StaticWeight[]>("weight.json")).map(mapWeight).filter((w) => inRange(w.date, range));
    },
    async weather(range: DateRange = {}) {
      return (await load<StaticWeather[]>("weather.json")).map(mapWeather).filter((d) => inRange(d.date, range));
    },
    async warnings(range: DateRange = {}) {
      return (await load<StaticWarning[]>("warnings.json")).map(mapWarning).filter((w) => inRange(w.date, range));
    },
  };
}
