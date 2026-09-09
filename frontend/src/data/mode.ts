import type { DataMode } from "./types";

export const DEFAULT_MODE: DataMode = "api";
export const DEFAULT_API_URL = "/api/graphql";
export const DEFAULT_STATIC_BASE = "/data";

/** Resolve the data mode from an env-like record; anything but "static" means api. */
export function resolveMode(env: Record<string, string | undefined>): DataMode {
  return env.VITE_DATA_MODE === "static" ? "static" : DEFAULT_MODE;
}

export interface DataConfig {
  mode: DataMode;
  apiUrl: string;
  staticBase: string;
}

export function resolveConfig(env: Record<string, string | undefined>): DataConfig {
  return {
    mode: resolveMode(env),
    apiUrl: env.VITE_API_URL || DEFAULT_API_URL,
    staticBase: (env.VITE_STATIC_BASE || DEFAULT_STATIC_BASE).replace(/\/$/, ""),
  };
}
