import { describe, expect, it } from "vitest";
import { actTemp, isSevere, warnInfo, wxEmoji } from "./weather";
import { paceColor, NO_PACE_COLOR } from "./paceColor";
import { act } from "@/test/fixtures";

describe("weather helpers", () => {
  it("maps signals to icons, most specific first", () => {
    expect(warnInfo("NO. 8 NORTHEAST GALE OR STORM SIGNAL")?.id).toBe("ws-t8ne");
    expect(warnInfo("STRONG WIND SIGNAL NO. 3")?.id).toBe("ws-t3");
    expect(warnInfo("AMBER RAINSTORM WARNING SIGNAL")?.name).toBe("Amber Rainstorm Warning Signal");
    expect(warnInfo("SOMETHING ELSE")).toBeNull();
  });
  it("severity and emoji", () => {
    expect(isSevere("STRONG WIND SIGNAL NO. 3")).toBe(true);
    expect(isSevere("RED FIRE DANGER WARNING")).toBe(false);
    expect(wxEmoji("Rain")).toBe("🌧");
    expect(wxEmoji(null)).toBe("🌡");
  });
  it("actTemp prefers the Garmin sensor", () => {
    expect(actTemp(act({ id: "a", date: "d", avgTempC: 18, weather: { description: null, tempC: 25, humidityPct: null, windKmh: null } }))).toBe(18);
    expect(actTemp(act({ id: "a", date: "d", avgTempC: null, weather: { description: null, tempC: 25, humidityPct: null, windKmh: null } }))).toBe(25);
    expect(actTemp(act({ id: "a", date: "d" }))).toBeNull();
  });
});

describe("paceColor", () => {
  it("interpolates red -> amber -> green and handles NaN", () => {
    expect(paceColor(NaN, 4, 7)).toBe(NO_PACE_COLOR);
    expect(paceColor(7, 4, 7)).toBe("rgb(248,113,113)");
    expect(paceColor(5.5, 4, 7)).toBe("rgb(245,158,11)");
    expect(paceColor(4, 4, 7)).toBe("rgb(52,211,153)");
  });
});
