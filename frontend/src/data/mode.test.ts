import { describe, expect, it } from "vitest";
import { resolveConfig, resolveMode } from "./mode";
import { createSourceFromEnv } from "./source";

describe("data mode", () => {
  it("defaults to api and only 'static' switches", () => {
    expect(resolveMode({})).toBe("api");
    expect(resolveMode({ VITE_DATA_MODE: "static" })).toBe("static");
    expect(resolveMode({ VITE_DATA_MODE: "STATIC" })).toBe("api");
  });

  it("fills defaults and trims a trailing slash", () => {
    expect(resolveConfig({})).toEqual({ mode: "api", apiUrl: "/api/graphql", staticBase: "/data" });
    expect(resolveConfig({ VITE_DATA_MODE: "static", VITE_STATIC_BASE: "/x/" }).staticBase).toBe("/x");
  });

  it("builds the matching source", () => {
    expect(createSourceFromEnv({}).mode).toBe("api");
    expect(createSourceFromEnv({ VITE_DATA_MODE: "static" }).mode).toBe("static");
  });
});
