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
      const rows = mapTrack(await load<StaticTrack>(`tracks/${id}.json`));
      return points ? downsample(rows, points) : rows;
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
