import { GraphQLClient } from "graphql-request";
import type {
  Activity,
  ActivityFilter,
  DataSource,
  DateRange,
  TrackPoint,
} from "../types";
import {
  ActivitiesQuery,
  ActivityQuery,
  MetaQuery,
  TrackQuery,
  WarningsQuery,
  WeatherQuery,
  WeightQuery,
  YearQuery,
} from "./queries";

export const DEFAULT_TRACK_POINTS = 600;

/** GraphQL variables are nullable, not optional: turn undefined into null. */
const rangeVars = (r: DateRange) => ({ fromDate: r.fromDate ?? null, toDate: r.toDate ?? null });
const filterVars = (f: ActivityFilter) => ({
  ...rangeVars(f),
  minKm: f.minKm ?? null,
  hasGps: f.hasGps ?? null,
});

/**
 * graphql-request needs an absolute URL in the browser; resolve a relative
 * endpoint such as "/api/graphql" against the current origin.
 */
export function absoluteUrl(url: string, base = globalThis.location?.href): string {
  return base ? new URL(url, base).toString() : url;
}

/** DataSource backed by the Flask + Strawberry GraphQL endpoint. */
export function createApiSource(
  url: string,
  client = new GraphQLClient(absoluteUrl(url)),
): DataSource {
  return {
    mode: "api",
    async meta() {
      return (await client.request(MetaQuery)).meta;
    },
    async year() {
      return (await client.request(YearQuery)).year;
    },
    async activities(filter: ActivityFilter = {}): Promise<Activity[]> {
      return (await client.request(ActivitiesQuery, filterVars(filter))).activities;
    },
    async activity(id: string) {
      return (await client.request(ActivityQuery, { id })).activity ?? null;
    },
    async track(id: string, points = DEFAULT_TRACK_POINTS): Promise<TrackPoint[]> {
      const { activity } = await client.request(TrackQuery, { id, points });
      // Two nullable things on one line, meaning two different things, and
      // `tsc` had nothing to say when the second one appeared: `?? []` was
      // already here for the first, so `track` turning nullable in CUI-0018 (b)
      // typechecked unchanged. Written out rather than left implicit.
      //
      // `activity == null` is an id the `activities` list never handed out. No
      // error comes back, and an empty track is the honest answer.
      //
      // `track == null` is a refused field. A refusal always arrives with an
      // entry in `errors`, and `graphql-request`'s default `errorPolicy` of
      // "none" rejects the promise whenever `errors` is non-empty -- partial
      // `data` included -- so this line is not reached for one. The caller gets
      // the rejection, and `ActivityView` renders `TrackError` from it.
      //
      // Kept as `?? []` rather than `!` for the case that reasoning does not
      // cover: an `errorPolicy: "all"` set here later, or a null that somehow
      // arrives without an error. Degrading to "no track" beats a TypeError
      // inside `buildSeries`.
      return activity?.track ?? [];
    },
    async weight(range: DateRange = {}) {
      return (await client.request(WeightQuery, rangeVars(range))).weight;
    },
    async weather(range: DateRange = {}) {
      return (await client.request(WeatherQuery, rangeVars(range))).weather;
    },
    async warnings(range: DateRange = {}) {
      return (await client.request(WarningsQuery, rangeVars(range))).warnings;
    },
  };
}
