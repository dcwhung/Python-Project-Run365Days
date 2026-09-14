import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { FONT_SANS, TOKENS, alpha, rgbChannels } from "./tokens";

/**
 * The guard that lets a TypeScript mirror of the stylesheet exist safely: if
 * anyone edits one side of the palette and not the other, this fails. It reads
 * the file rather than the computed styles, so it works headless and needs no
 * build step to keep a generated copy in sync.
 */
function themeBlock(): Map<string, string> {
  // Vitest resolves relative to the Vite root, which is the `frontend/` folder.
  const css = readFileSync(resolve("src/index.css"), "utf8");
  const block = /@theme\s*\{([\s\S]*?)\n\}/.exec(css);
  if (!block) throw new Error("index.css has no @theme block");
  const declarations = new Map<string, string>();
  for (const line of block[1].split("\n")) {
    const match = /^\s*(--[\w-]+)\s*:\s*(.+?);\s*$/.exec(line);
    if (match) declarations.set(match[1], match[2]);
  }
  return declarations;
}

describe("design tokens", () => {
  const declarations = themeBlock();

  it("should find the palette in index.css", () => {
    expect(declarations.size).toBeGreaterThan(0);
  });

  it("should mirror every --color-* declaration from index.css", () => {
    const fromCss = Object.fromEntries(
      [...declarations].filter(([name]) => name.startsWith("--color-")),
    );
    const fromTs = Object.fromEntries(
      Object.entries(TOKENS).map(([name, value]) => [`--color-${name}`, value]),
    );
    expect(fromTs).toEqual(fromCss);
  });

  it("should mirror --font-sans", () => {
    expect(FONT_SANS).toBe(declarations.get("--font-sans"));
  });

  it("should split a token into its channels", () => {
    expect(rgbChannels(TOKENS.accent)).toEqual([79, 142, 247]);
    expect(rgbChannels(TOKENS.danger)).toEqual([248, 113, 113]);
  });

  it("should render a token at partial opacity", () => {
    expect(alpha(TOKENS.accent, 0.5)).toBe("rgba(79,142,247,0.5)");
    expect(alpha(TOKENS.warn, 0.08)).toBe("rgba(245,158,11,0.08)");
  });
});
