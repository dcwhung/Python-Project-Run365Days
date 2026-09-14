import { beforeEach, describe, expect, it } from "vitest";
import {
  DEFAULT_PREFS,
  getPrefs,
  LBS_TO_KG,
  resetPrefs,
  savePrefs,
  STORAGE_KEY,
  toWeightUnit,
} from "./prefs";
// The schema the API publishes, generated from src/api/schema.py by `run365-schema`.
import SDL from "../../schema.graphql?raw";

describe("prefs", () => {
  beforeEach(() => resetPrefs());

  it("starts from defaults and persists changes", () => {
    expect(getPrefs()).toEqual(DEFAULT_PREFS);
    savePrefs({ weightUnit: "kg", speed: 90 });
    expect(getPrefs().weightUnit).toBe("kg");
    expect(JSON.parse(localStorage.getItem(STORAGE_KEY)!)).toMatchObject({
      weightUnit: "kg",
      speed: 90,
    });
    resetPrefs();
    expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
    expect(getPrefs()).toEqual(DEFAULT_PREFS);
  });

  // These two numbers decide what a weigh-in reads as, and the API applies its
  // own copies of them to the same pounds before publishing weightKg and bmi.
  // The SDL states both, so read them back rather than trusting that two
  // hand-written copies still agree -- they did not: the API converted at
  // 0.454 while this file used the exact factor, and nothing noticed (AU-008).
  it("converts pounds at the factor the API publishes", () => {
    const [, factor] = SDL.match(/1 lb = ([\d.]+) kg/) ?? [];
    expect(factor, "schema.graphql no longer states the pound conversion").toBeDefined();
    expect(Number(factor)).toBe(LBS_TO_KG);
  });

  it("defaults to the height the API computes BMI against", () => {
    const [, height] = SDL.match(/fixed height of ([\d.]+) cm/) ?? [];
    expect(height, "schema.graphql no longer states the BMI height").toBeDefined();
    expect(Number(height)).toBe(DEFAULT_PREFS.heightCm);
  });

  it("is the only module that writes the pound factor down", () => {
    const sources = import.meta.glob("../**/*.{ts,tsx}", {
      query: "?raw",
      import: "default",
      eager: true,
    }) as Record<string, string>;
    const spelled = Object.entries(sources)
      // Tests may quote the number, and src/gql is codegen output: it carries
      // the factor because the SDL states it, which is the point, not a copy.
      .filter(([path]) => !/\.test\.tsx?$/.test(path) && !path.startsWith("../gql/"))
      .filter(([, text]) => /(?<![\w.])0\.45359237(?![\w.])/.test(text))
      .map(([path]) => path);
    expect(spelled).toEqual(["./prefs.ts"]);
  });

  it("converts weight units", () => {
    expect(toWeightUnit(154.8, "lbs")).toBe(154.8);
    expect(toWeightUnit(154.8, "kg")).toBe(70.2);
    expect(toWeightUnit(null, "kg")).toBeNull();
  });
});
