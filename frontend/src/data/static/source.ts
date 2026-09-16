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
 * Rejects the same values, so a client writes one error path rather than one
 * per mode -- and says the same sentence for every value the GraphQL `Int`
 * scalar can carry, which is where that promise ends (CUI-0033 (b)).
 *
 * Outside that range the two modes still agree on the *decision* and differ
 * only in wording, because api mode never reaches `_track_points`: `Int!`
 * coercion runs during execution set-up and refuses the value first. Measured
 * against a real Flask + Strawberry app:
 *
 * | `points`     | this side                          | api mode                                              |
 * | ------------ | ---------------------------------- | ----------------------------------------------------- |
 * | `-1`/`0`/`1001` | `points must be between 1 and 1000, got N` | same sentence                                  |
 * | `2.5`        | same sentence                      | `Int cannot represent non-integer value: 2.5`         |
 * | `2147483648` | same sentence                      | `Int cannot represent non 32-bit signed integer value` |
 *
 * Non-integers are refused here rather than passed through because
 * `downsample`'s contract does not cover a non-integer `limit`: Python raises
 * on `range(2.5)` where this side would quietly return two items. A coercion
 * failure also nulls the whole api-mode response rather than just `track`,
 * since it is not a field error.
 * `test_a_points_the_int_scalar_cannot_carry_is_refused_before_the_resolver`
 * in tests/test_api.py is what holds the right-hand column.
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
      //
      // Checking first is also why an unknown `id` reads differently in the
      // two modes (CUI-0033 (c)): `track("no-such-activity", 0)` throws the
      // bounds error here, having sent no request, while api mode answers
      // `{ activity: null }` with no error at all -- `track` is a field on
      // `Activity`, so a null parent means this resolver, bounds check
      // included, never runs. Deliberately left alone rather than aligned:
      // reaching that state means asking for an activity the `activities`
      // list did not hand out, and making api mode raise instead would trade a
      // real property (an illegal argument costs no round trip) for a symmetry
      // nothing asks for. The divergence is narrower than it looks -- a
      // `points` the `Int` scalar cannot carry is refused in both modes even
      // for an unknown id, because coercion precedes every resolver.
      // `test_an_unknown_activity_swallows_an_illegal_points_that_static_mode_refuses`
      // in tests/test_api.py holds the api-mode side of this.
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
