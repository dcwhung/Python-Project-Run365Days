import { useEffect, useRef } from "react";
import type { DayDistance } from "@/data/types";
import { MONTHS } from "@/lib/format";
import { monthLengths } from "@/lib/dates";

const LABEL_W = 34;
const GAP = 4;

/** One diamond per day, twelve rows, lit on days with a run (v2 infographic). */
export function DiamondGrid({ daily, year }: { daily: DayDistance[]; year: number }) {
  const ref = useRef<HTMLCanvasElement>(null);
  useEffect(() => {
    const cv = ref.current;
    const g = cv?.getContext("2d");
    if (!cv || !g) return;
    const avail = (cv.parentElement?.clientWidth ?? 600) - 8;
    const cell = Math.max(6, Math.min(24, Math.floor((avail - LABEL_W - 31 * GAP) / 31)));
    const rowH = cell + GAP;
    const dpr = window.devicePixelRatio || 1;
    const W = LABEL_W + 31 * (cell + GAP);
    const H = 12 * rowH;
    cv.width = W * dpr;
    cv.height = H * dpr;
    cv.style.width = `${W}px`;
    cv.style.height = `${H}px`;
    g.setTransform(dpr, 0, 0, dpr, 0, 0);
    const half = cell * 0.42;
    let doy = 1;
    monthLengths(year).forEach((days, mi) => {
      const cy = mi * rowH + rowH / 2;
      g.font = '600 9px "Segoe UI",system-ui,sans-serif';
      g.fillStyle = "#7c85a8";
      g.textBaseline = "middle";
      g.fillText(MONTHS[mi].toUpperCase(), 0, cy);
      for (let d = 0; d < days; d++, doy++) {
        g.save();
        g.translate(LABEL_W + d * (cell + GAP) + cell / 2, cy);
        g.rotate(Math.PI / 4);
        g.fillStyle = daily[doy - 1]?.activityId ? "#a78bfa" : "#22263a";
        g.fillRect(-half, -half, half * 2, half * 2);
        g.restore();
      }
    });
  }, [daily, year]);
  return <canvas ref={ref} aria-label="One diamond per day" data-testid="diamonds" />;
}
