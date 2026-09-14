import { describe, expect, it } from "vitest";
import { fmtThousands } from "./format";
import {
  M_PER_KM,
  SEC_PER_HOUR,
  SEC_PER_MIN,
  metresToKm,
  minToSec,
  paceToPlotMin,
  secToHours,
  secToMin,
} from "./units";

describe("unit conversions", () => {
  it("should convert seconds to minutes and back", () => {
    expect(secToMin(330)).toBe(5.5);
    expect(minToSec(5.5)).toBe(330);
    expect(minToSec(secToMin(1234))).toBe(1234);
  });

  it("should convert seconds to hours", () => {
    expect(secToHours(5400)).toBe(1.5);
    expect(secToHours(126000).toFixed(1)).toBe("35.0");
  });

  it("should convert metres to kilometres", () => {
    expect(metresToKm(5432)).toBe(5.432);
  });

  it("should round plotted pace to two decimals", () => {
    expect(paceToPlotMin(330)).toBe(5.5);
    expect(paceToPlotMin(334)).toBe(5.57);
  });

  it("should reproduce the arithmetic these helpers replaced", () => {
    // The views used to inline these expressions; the whole point of the move is
    // that the numbers did not change, so pin them against the literal form.
    for (const sec of [0, 1, 59, 60, 333, 3599, 3600, 86_399]) {
      expect(secToMin(sec)).toBe(sec / 60);
      expect(secToHours(sec)).toBe(sec / 3600);
      expect(minToSec(sec)).toBe(sec * 60);
      expect(metresToKm(sec)).toBe(sec / 1000);
      expect(paceToPlotMin(sec)).toBe(Math.round((sec / 60) * 100) / 100);
    }
  });

  it("should name the constants the conversions use", () => {
    expect([SEC_PER_MIN, SEC_PER_HOUR, M_PER_KM]).toEqual([60, 3600, 1000]);
  });

  it("should shorten a large total to thousands", () => {
    expect(fmtThousands(123_456)).toBe("123k");
    expect(fmtThousands(1_500)).toBe("2k");
  });
});
