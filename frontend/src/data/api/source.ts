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
      return (await client.request(TrackQuery, { id, points })).activity?.track ?? [];
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
