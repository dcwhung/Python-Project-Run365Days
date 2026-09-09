import { useSyncExternalStore } from "react";

/** User preferences (Settings view), persisted in localStorage like v2. */
export interface Prefs {
  landing: string;
  speed: number;
  weightUnit: "lbs" | "kg";
  heightCm: number;
  paceFast: number;
  paceSlow: number;
}

export const STORAGE_KEY = "run365.prefs";
export const DEFAULT_PREFS: Prefs = {
  landing: "overview",
  speed: 30,
  weightUnit: "lbs",
  heightCm: 170,
  paceFast: 4.0,
  paceSlow: 7.0,
};
export const LBS_TO_KG = 0.45359237;

const listeners = new Set<() => void>();
let current: Prefs = read();

function read(): Prefs {
  try {
    const raw = globalThis.localStorage?.getItem(STORAGE_KEY);
    return { ...DEFAULT_PREFS, ...(raw ? (JSON.parse(raw) as Partial<Prefs>) : {}) };
  } catch {
    return { ...DEFAULT_PREFS };
  }
}

export function getPrefs(): Prefs {
  return current;
}

export function savePrefs(next: Partial<Prefs>): Prefs {
  current = { ...current, ...next };
  try {
    globalThis.localStorage?.setItem(STORAGE_KEY, JSON.stringify(current));
  } catch {
    /* private mode: keep in memory only */
  }
  listeners.forEach((l) => l());
  return current;
}

export function resetPrefs(): Prefs {
  try {
    globalThis.localStorage?.removeItem(STORAGE_KEY);
  } catch {
    /* ignore */
  }
  current = { ...DEFAULT_PREFS };
  listeners.forEach((l) => l());
  return current;
}

function subscribe(l: () => void) {
  listeners.add(l);
  return () => listeners.delete(l);
}

export function usePrefs(): Prefs {
  return useSyncExternalStore(subscribe, getPrefs, getPrefs);
}

/** Convert pounds to the preferred display unit, rounded to one decimal. */
export function toWeightUnit(lbs: number | null | undefined, unit: Prefs["weightUnit"]): number | null {
  if (lbs == null) return null;
  return Math.round((unit === "kg" ? lbs * LBS_TO_KG : lbs) * 10) / 10;
}
