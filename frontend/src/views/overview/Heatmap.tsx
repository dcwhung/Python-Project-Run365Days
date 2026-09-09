import { useState } from "react";
import { useNavigate } from "react-router-dom";
import type { DayDistance } from "@/data/types";
import { MONTHS, fmtKm } from "@/lib/format";
import { monthLengths, weekday, longDate } from "@/lib/dates";
import { heatLevel } from "./model";

const CELL = 11;
const GAP = 4;
const LEVEL_CLASS = [
  "bg-surface2",
  "bg-accent/25",
  "bg-accent/45",
  "bg-accent/65",
  "bg-accent/85",
  "bg-accent",
];

/** GitHub-style calendar: one column per week, Monday at the top. */
export function Heatmap({ daily, year }: { daily: DayDistance[]; year: number }) {
  const navigate = useNavigate();
  const [hover, setHover] = useState<{ text: string; x: number; y: number } | null>(null);
  const startOffset = weekday(daily[0]?.date ?? `${year}-01-01`);
  const numWeeks = Math.ceil((startOffset + daily.length) / 7);
  const monthWeeks: number[] = [];
  let acc = 0;
  for (const n of monthLengths(year)) {
    monthWeeks.push(Math.floor((acc + startOffset) / 7));
    acc += n;
  }
  monthWeeks.push(numWeeks);

  return (
    <div className="relative overflow-x-auto" data-testid="heatmap">
      <div className="ml-9 flex text-[10px] text-muted" style={{ gap: 0 }}>
        {MONTHS.map((m, i) => (
          <span key={m} style={{ width: (monthWeeks[i + 1] - monthWeeks[i]) * (CELL + GAP) }}>
            {m}
          </span>
        ))}
      </div>
      <div className="flex">
        <div className="mr-1 flex w-8 flex-col text-[10px] text-muted" style={{ gap: GAP }}>
          {["", "Mon", "", "Wed", "", "Fri", ""].map((l, i) => (
            <span key={i} style={{ height: CELL, lineHeight: `${CELL}px` }}>
              {l}
            </span>
          ))}
        </div>
        <div className="flex" style={{ gap: GAP }}>
          {Array.from({ length: numWeeks }, (_, w) => (
            <div key={w} className="flex flex-col" style={{ gap: GAP }}>
              {Array.from({ length: 7 }, (_, wd) => {
                const doy = w * 7 + wd - startOffset + 1;
                const day = doy >= 1 && doy <= daily.length ? daily[doy - 1] : null;
                if (!day) return <div key={wd} style={{ width: CELL, height: CELL }} />;
                const text = `${longDate(day.date)} — ${day.distanceKm ? `${fmtKm(day.distanceKm)} km` : "rest day"}`;
                return (
                  <button
                    key={wd}
                    type="button"
                    aria-label={text}
                    className={`rounded-[2px] ${LEVEL_CLASS[heatLevel(day.distanceKm)]} ${day.activityId ? "cursor-pointer hover:outline hover:outline-1 hover:outline-text" : ""}`}
                    style={{ width: CELL, height: CELL }}
                    onMouseEnter={(e) => setHover({ text, x: e.clientX, y: e.clientY })}
                    onMouseMove={(e) => setHover({ text, x: e.clientX, y: e.clientY })}
                    onMouseLeave={() => setHover(null)}
                    onClick={() => day.activityId && navigate(`/activity/${day.activityId}`)}
                  />
                );
              })}
            </div>
          ))}
        </div>
      </div>
      {hover && (
        <div
          className="pointer-events-none fixed z-50 rounded border border-border bg-surface2 px-2 py-1 text-xs shadow"
          style={{ left: hover.x + 12, top: hover.y - 28 }}
        >
          {hover.text}
        </div>
      )}
    </div>
  );
}
