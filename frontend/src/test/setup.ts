import "@testing-library/jest-dom/vitest";
import { vi } from "vitest";

// jsdom has no canvas: give every canvas a no-op 2D context so drawing code runs.
const noop = () => {};
const ctx = new Proxy({} as CanvasRenderingContext2D, {
  get: (_t, prop) => (prop === "measureText" ? () => ({ width: 0 }) : noop),
  set: () => true,
});
HTMLCanvasElement.prototype.getContext = vi.fn(() => ctx) as unknown as typeof HTMLCanvasElement.prototype.getContext;
globalThis.requestAnimationFrame = (cb) => setTimeout(() => cb(performance.now()), 16) as unknown as number;
globalThis.cancelAnimationFrame = (id) => clearTimeout(id);
globalThis.ResizeObserver =
  globalThis.ResizeObserver ||
  class {
    observe() {}
    unobserve() {}
    disconnect() {}
  };
