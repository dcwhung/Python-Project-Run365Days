import { beforeEach, describe, expect, it } from "vitest";
import { DEFAULT_PREFS, getPrefs, resetPrefs, savePrefs, STORAGE_KEY, toWeightUnit } from "./prefs";

describe("prefs", () => {
  beforeEach(() => resetPrefs());

  it("starts from defaults and persists changes", () => {
    expect(getPrefs()).toEqual(DEFAULT_PREFS);
    savePrefs({ weightUnit: "kg", speed: 90 });
    expect(getPrefs().weightUnit).toBe("kg");
    expect(JSON.parse(localStorage.getItem(STORAGE_KEY)!)).toMatchObject({ weightUnit: "kg", speed: 90 });
    resetPrefs();
    expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
    expect(getPrefs()).toEqual(DEFAULT_PREFS);
  });

  it("converts weight units", () => {
    expect(toWeightUnit(154.8, "lbs")).toBe(154.8);
    expect(toWeightUnit(154.8, "kg")).toBe(70.2);
    expect(toWeightUnit(null, "kg")).toBeNull();
  });
});
