import { useNavigate } from "react-router-dom";
import type { PersonalBests, Totals } from "@/data/types";
import { fmtShortDate } from "@/lib/format";
import { Card } from "@/components/Card";
import { personalBestRows } from "./model";

const BAR = {
  violet: "bg-violet",
  accent: "bg-accent",
  accent2: "bg-accent2",
  warn: "bg-warn",
  danger: "bg-danger",
};

export function PersonalBestList({ personalBests, totals }: { personalBests: PersonalBests; totals: Totals }) {
  const navigate = useNavigate();
  const rows = personalBestRows(personalBests, totals);
  return (
    <Card title="Personal Bests">
      <ul className="space-y-2 text-xs" data-testid="pbs">
        {rows.map((r) => (
          <li
            key={r.label}
            className="flex cursor-pointer items-center gap-3 hover:text-text"
            onClick={() => navigate(`/activity/${r.activity.id}`)}
          >
            <span className="w-20 text-muted">{r.label}</span>
            <span className="h-1.5 flex-1 overflow-hidden rounded bg-surface2">
              <span className={`block h-full ${BAR[r.color]}`} style={{ width: `${Math.round(r.ratio * 100)}%` }} />
            </span>
            <span className="w-20 text-right font-medium">{r.value}</span>
            <span className="w-14 text-right text-muted">{fmtShortDate(r.activity.date)}</span>
          </li>
        ))}
      </ul>
      <p className="mt-2 text-[10px] text-muted">Bar shows the typical (average) run relative to the record.</p>
    </Card>
  );
}
