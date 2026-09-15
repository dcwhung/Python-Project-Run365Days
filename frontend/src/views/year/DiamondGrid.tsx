import { useEffect, useRef } from "react";
import type { DayDistance } from "@/data/types";
import { MONTHS } from "@/lib/format";
import { monthLengths } from "@/lib/dates";
import { FONT_SANS, TOKENS } from "@/styles/tokens";

/** Width of the month-name gutter down the left edge. */
const LABEL_WIDTH_PX = 34;
const GAP = 4;
/** Every row is sized for the longest month, so all twelve line up. */
const COLUMNS = 31;
const ROWS = 12;

/** One diamond per day, twelve rows, lit on days with a run (v2 infographic). */
export function DiamondGrid({ daily, year }: { daily: DayDistance[]; year: number }) {
  const ref = useRef<HTMLCanvasElement>(null);
  useEffect(() => {
    const canvasEl = ref.current;
    const ctx = canvasEl?.getContext("2d");
    if (!canvasEl || !ctx) return;
    const available = (canvasEl.parentElement?.clientWidth ?? 600) - 8;
    const cell = Math.max(
      6,
      Math.min(24, Math.floor((available - LABEL_WIDTH_PX - COLUMNS * GAP) / COLUMNS)),
    );
    const rowHeight = cell + GAP;
    const dpr = window.devicePixelRatio || 1;
    const cssWidth = LABEL_WIDTH_PX + COLUMNS * (cell + GAP);
    const cssHeight = ROWS * rowHeight;
    canvasEl.width = cssWidth * dpr;
    canvasEl.height = cssHeight * dpr;
    canvasEl.style.width = `${cssWidth}px`;
    canvasEl.style.height = `${cssHeight}px`;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    const half = cell * 0.42;
    let dayOfYear = 1;
    monthLengths(year).forEach((days, monthIndex) => {
      const rowCenterY = monthIndex * rowHeight + rowHeight / 2;
      ctx.font = `600 9px ${FONT_SANS}`;
      ctx.fillStyle = TOKENS.muted;
      ctx.textBaseline = "middle";
      ctx.fillText(MONTHS[monthIndex].toUpperCase(), 0, rowCenterY);
      for (let dayIndex = 0; dayIndex < days; dayIndex++, dayOfYear++) {
        ctx.save();
        ctx.translate(LABEL_WIDTH_PX + dayIndex * (cell + GAP) + cell / 2, rowCenterY);
        ctx.rotate(Math.PI / 4);
        ctx.fillStyle = daily[dayOfYear - 1]?.activityId ? TOKENS.violet : TOKENS.surface2;
        ctx.fillRect(-half, -half, half * 2, half * 2);
        ctx.restore();
      }
    });
  }, [daily, year]);
  return <canvas ref={ref} aria-label="One diamond per day" data-testid="diamonds" />;
}
