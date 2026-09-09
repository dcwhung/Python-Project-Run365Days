import { describe, expect, it, vi } from "vitest";
import type { GraphQLClient } from "graphql-request";
import { absoluteUrl, createApiSource, DEFAULT_TRACK_POINTS } from "./source";

function client(response: unknown) {
  const request = vi.fn<(doc: unknown, vars?: unknown) => Promise<unknown>>(async () => response);
  return { client: { request } as unknown as GraphQLClient, request };
}

describe("absoluteUrl", () => {
  it("resolves a relative endpoint against the page origin", () => {
    expect(absoluteUrl("/api/graphql", "http://localhost:5173/overview")).toBe("http://localhost:5173/api/graphql");
    expect(absoluteUrl("https://x.test/g", "http://localhost/")).toBe("https://x.test/g");
    expect(absoluteUrl("/api/graphql", "")).toBe("/api/graphql");
  });
});

describe("api source", () => {
  it("unwraps the activities field and normalises variables to null", async () => {
    const { client: c, request } = client({ activities: [{ id: "a" }] });
    const src = createApiSource("/api/graphql", c);
    expect(src.mode).toBe("api");
    expect(await src.activities({ fromDate: "2021-01-01" })).toEqual([{ id: "a" }]);
    expect(request.mock.calls[0][1]).toEqual({ fromDate: "2021-01-01", toDate: null, minKm: null, hasGps: null });
  });

  it("returns null for a missing activity and an empty track", async () => {
    const { client: c, request } = client({ activity: null });
    const src = createApiSource("/api/graphql", c);
    expect(await src.activity("x")).toBeNull();
    expect(await src.track("x")).toEqual([]);
    expect(request.mock.calls[1][1]).toEqual({ id: "x", points: DEFAULT_TRACK_POINTS });
  });

  it("unwraps year, meta, weight, weather and warnings", async () => {
    const payload = { year: { year: 2021 }, meta: { year: 2021 }, weight: [1], weather: [2], warnings: [3] };
    const { client: c } = client(payload);
    const src = createApiSource("/api/graphql", c);
    expect(await src.year()).toEqual({ year: 2021 });
    expect(await src.meta()).toEqual({ year: 2021 });
    expect(await src.weight()).toEqual([1]);
    expect(await src.weather({ toDate: "2021-02-01" })).toEqual([2]);
    expect(await src.warnings()).toEqual([3]);
  });
});
