import type { DataSource } from "./types";
import { resolveConfig } from "./mode";
import { createApiSource } from "./api/source";
import { createStaticSource } from "./static/source";

/** Build the source once from Vite env vars (VITE_DATA_MODE, VITE_API_URL, VITE_STATIC_BASE). */
export function createSourceFromEnv(env: Record<string, string | undefined>): DataSource {
  const cfg = resolveConfig(env);
  return cfg.mode === "static" ? createStaticSource(cfg.staticBase) : createApiSource(cfg.apiUrl);
}
