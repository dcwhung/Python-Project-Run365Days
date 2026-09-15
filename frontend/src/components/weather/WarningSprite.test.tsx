import { describe, expect, it } from "vitest";
import { render } from "@testing-library/react";
import { WarningSprite } from "./WarningSprite";
import { WARN_ICON_IDS } from "@/lib/weather";
import { SIGNAL } from "@/styles/signalColors";
import { TOKENS } from "@/styles/tokens";

function sprite() {
  return render(<WarningSprite />).container;
}

describe("WarningSprite", () => {
  it("should define a symbol for every warning the app can show", () => {
    const defined = [...sprite().querySelectorAll("symbol")].map((s) => s.id).sort();
    expect(defined).toEqual([...WARN_ICON_IDS].sort());
  });

  it("should give every shape an explicit paint", () => {
    // An SVG shape with neither fill nor stroke falls back to black, which on a
    // dark card is an invisible icon that no test would otherwise notice.
    const unpainted = [...sprite().querySelectorAll("path, rect, circle")].filter(
      (el) => !el.getAttribute("fill") && !el.getAttribute("stroke"),
    );
    expect(unpainted).toEqual([]);
  });

  it("should draw the cyclone signals in the theme foreground", () => {
    // These follow the theme. The rest of the palette deliberately does not.
    const t1 = sprite().querySelector("#ws-t1 path");
    expect(t1?.getAttribute("fill")).toBe(TOKENS.text);
  });

  it("should keep the rainstorm ranks on their fixed signal colours", () => {
    const container = sprite();
    const fill = (id: string) => container.querySelector(`#${id} path`)?.getAttribute("fill");
    expect(fill("ws-rain-amber")).toBe(SIGNAL.rainAmber);
    expect(fill("ws-rain-red")).toBe(SIGNAL.rainRed);
    expect(fill("ws-rain-black")).toBe(SIGNAL.rainBlack);
    // The three ranks have to stay distinguishable by colour alone.
    expect(new Set([SIGNAL.rainAmber, SIGNAL.rainRed, SIGNAL.rainBlack]).size).toBe(3);
  });
});
