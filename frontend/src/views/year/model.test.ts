import { describe, expect, it } from "vitest";
import { monthlyHours, weightReview } from "./model";
import { STATS_ACTS } from "@/test/fixtures";

describe("year model", () => {
  it("monthly hours", () => {
    const h = monthlyHours(STATS_ACTS);
    expect(h[0]).toBeCloseTo(5500 / 3600);
    expect(h[1]).toBeCloseTo(1620 / 3600);
    expect(h[2]).toBe(0);
  });
  it("weight review", () => {
    const W = [
      { date: "2021-01-01", weightLbs: 160, weightKg: 0, bmi: 0 },
      { date: "2021-06-01", weightLbs: 140, weightKg: 0, bmi: 0 },
      { date: "2021-12-31", weightLbs: 144, weightKg: 0, bmi: 0 },
    ];
    const r = weightReview(W)!;
    expect(r.min.date).toBe("2021-06-01");
    expect(r.lossPct).toBe(10);
    expect(weightReview([])).toBeNull();
  });
});
