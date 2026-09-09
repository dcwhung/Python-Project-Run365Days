import { describe, expect, it } from "vitest";
import { downsample } from "./downsample";
import { fmtDuration, fmtPace, fmtShortDate } from "./format";

describe("downsample", () => {
  it("keeps everything under the limit", () => {
    expect(downsample([1, 2, 3], 5)).toEqual([1, 2, 3]);
  });
  it("keeps first and last evenly", () => {
    expect(downsample([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)).toEqual([0, 5, 10]);
    expect(downsample([1, 2, 3], 1)).toEqual([3]);
  });
});

describe("format", () => {
  it("fmtDuration", () => {
    expect(fmtDuration(59)).toBe("0:59");
    expect(fmtDuration(1833)).toBe("30:33");
    expect(fmtDuration(3661)).toBe("1:01:01");
  });
  it("fmtPace", () => {
    expect(fmtPace(308)).toBe("5:08");
    expect(fmtPace(null)).toBe("–");
    expect(fmtPace(NaN)).toBe("–");
  });
  it("fmtShortDate", () => {
    expect(fmtShortDate("2021-03-09")).toBe("Mar 09");
  });
});
