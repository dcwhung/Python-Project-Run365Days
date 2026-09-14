import { describe, expect, it, vi } from "vitest";
import type { GraphQLClient } from "graphql-request";
import { absoluteUrl, createApiSource, DEFAULT_TRACK_POINTS } from "./source";
// The schema the API publishes, generated from src/api/schema.py by `run365-schema`.
import SDL from "../../../schema.graphql?raw";

function client(response: unknown) {
  const request = vi.fn<(doc: unknown, vars?: unknown) => Promise<unknown>>(async () => response);
  return { client: { request } as unknown as GraphQLClient, request };
}

describe("absoluteUrl", () => {
  it("resolves a relative endpoint against the page origin", () => {
    expect(absoluteUrl("/api/graphql", "http://localhost:5173/overview")).toBe(
      "http://localhost:5173/api/graphql",
    );
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
    expect(request.mock.calls[0][1]).toEqual({
      fromDate: "2021-01-01",
      toDate: null,
      minKm: null,
      hasGps: null,
    });
  });

  it("returns null for a missing activity and an empty track", async () => {
    const { client: c, request } = client({ activity: null });
    const src = createApiSource("/api/graphql", c);
    expect(await src.activity("x")).toBeNull();
    expect(await src.track("x")).toEqual([]);
    expect(request.mock.calls[1][1]).toEqual({ id: "x", points: DEFAULT_TRACK_POINTS });
  });

  // Asking for fewer samples than the export stored would quietly hand api
  // mode a coarser track than static mode, which has no argument to pass and
  // returns everything in the file. The two 600s used to be typed out
  // separately in Python and here and matched only by luck; raising the export
  // limit would have moved one and not the other (CUI-0024).
  it("asks for exactly as many samples as the export stores", () => {
    const [, stored] = SDL.match(/export stores at most (\d+) samples per run/) ?? [];
    expect(stored, "schema.graphql no longer states the stored sample count").toBeDefined();
    expect(Number(stored)).toBe(DEFAULT_TRACK_POINTS);
  });

  it("unwraps year, meta, weight, weather and warnings", async () => {
    const payload = {
      year: { year: 2021 },
      meta: { year: 2021 },
      weight: [1],
      weather: [2],
      warnings: [3],
    };
    const { client: c } = client(payload);
    const src = createApiSource("/api/graphql", c);
    expect(await src.year()).toEqual({ year: 2021 });
    expect(await src.meta()).toEqual({ year: 2021 });
    expect(await src.weight()).toEqual([1]);
    expect(await src.weather({ toDate: "2021-02-01" })).toEqual([2]);
    expect(await src.warnings()).toEqual([3]);
  });
});
