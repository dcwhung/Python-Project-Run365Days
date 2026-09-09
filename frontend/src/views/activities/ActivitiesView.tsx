import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useActivities, useMeta } from "@/data/hooks";
import type { Activity } from "@/data/types";
import { MONTHS, fmtDuration, fmtKm, fmtPace, fmtShortDate } from "@/lib/format";
import { actTemp, wxEmoji } from "@/lib/weather";
import { WarningIcons } from "@/components/WarningIcons";
import { DEFAULT_FILTERS, filterAndSort, toCsv, type Filters, type SortKey } from "./model";

interface Column {
  key: SortKey;
  label: string;
  numeric?: boolean;
  render: (a: Activity) => React.ReactNode;
}

const COLUMNS: Column[] = [
  { key: "date", label: "Date", render: (a) => `${fmtShortDate(a.date)} ${a.startTime}` },
  { key: "distanceKm", label: "Dist.", numeric: true, render: (a) => `${fmtKm(a.distanceKm)} km` },
  { key: "durationSec", label: "Time", numeric: true, render: (a) => fmtDuration(a.durationSec) },
  { key: "paceSecPerKm", label: "Pace", numeric: true, render: (a) => `${fmtPace(a.paceSecPerKm)}/km` },
  { key: "avgCadence", label: "Cadence", numeric: true, render: (a) => (a.avgCadence ? `${Math.round(a.avgCadence)} spm` : "–") },
  { key: "ascentM", label: "Ascent", numeric: true, render: (a) => (a.ascentM != null ? `${Math.round(a.ascentM)} m` : "–") },
  { key: "calories", label: "kcal", numeric: true, render: (a) => a.calories ?? "–" },
  { key: "temp", label: "Temp", numeric: true, render: (a) => (actTemp(a) != null ? `${actTemp(a)!.toFixed(1)} °C` : "–") },
  {
    key: "weather",
    label: "Weather",
    render: (a) =>
      a.weather ? (
        <span className="rounded bg-surface2 px-2 py-0.5 text-xs">
          {wxEmoji(a.weather.description)} {a.weather.description}
        </span>
      ) : (
        "–"
      ),
  },
  { key: "warnings", label: "Warnings", render: (a) => <WarningIcons signals={a.warnings} /> },
  {
    key: "hasGps",
    label: "GPS",
    render: (a) => (
      <span className={`rounded px-1.5 py-0.5 text-[10px] ${a.hasGps ? "bg-accent2/15 text-accent2" : "bg-surface2 text-muted"}`}>
        {a.hasGps ? "outdoor" : "indoor"}
      </span>
    ),
  },
];

function download(name: string, text: string) {
  const blob = new Blob([text], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = Object.assign(document.createElement("a"), { href: url, download: name });
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

export function ActivitiesView() {
  const navigate = useNavigate();
  const all = useActivities();
  const meta = useMeta();
  const [filters, setFilters] = useState<Filters>(DEFAULT_FILTERS);
  const [sort, setSort] = useState<{ key: SortKey; dir: 1 | -1 }>({ key: "date", dir: -1 });
  const rows = useMemo(() => filterAndSort(all.data ?? [], filters, sort.key, sort.dir), [all.data, filters, sort]);

  if (all.isPending) return <p className="text-muted">Loading…</p>;
  if (all.isError) return <p className="text-danger">Could not load activities: {all.error.message}</p>;
  const total = all.data.length;
  const totalKm = rows.reduce((s, a) => s + a.distanceKm, 0);
  const setSortKey = (key: SortKey) =>
    setSort((s) => (s.key === key ? { key, dir: s.dir === 1 ? -1 : 1 } : { key, dir: -1 }));
  const input = "rounded border border-border bg-surface px-2 py-1 text-xs";

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold">Activities</h1>
          <p className="text-xs text-muted" data-testid="act-count">
            {rows.length} of {total} runs · {totalKm.toFixed(1)} km
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <input
            aria-label="Search"
            className={input}
            placeholder="Search date, weather, warning…"
            value={filters.query}
            onChange={(e) => setFilters({ ...filters, query: e.target.value })}
          />
          <select aria-label="Month" className={input} value={filters.month ?? ""} onChange={(e) => setFilters({ ...filters, month: e.target.value ? Number(e.target.value) : null })}>
            <option value="">All months</option>
            {MONTHS.map((m, i) => (
              <option key={m} value={i + 1}>
                {m}
              </option>
            ))}
          </select>
          <select aria-label="GPS" className={input} value={filters.gps == null ? "" : filters.gps ? "1" : "0"} onChange={(e) => setFilters({ ...filters, gps: e.target.value === "" ? null : e.target.value === "1" })}>
            <option value="">Outdoor + indoor</option>
            <option value="1">Outdoor</option>
            <option value="0">Indoor</option>
          </select>
          <label className="flex items-center gap-1 text-xs text-muted">
            ≥
            <input aria-label="Minimum km" type="number" min={0} step={0.5} className={`${input} w-16`} value={filters.minKm} onChange={(e) => setFilters({ ...filters, minKm: Number(e.target.value) || 0 })} />
            km
          </label>
          <button type="button" className="rounded border border-border px-2 py-1 text-xs hover:bg-surface2" onClick={() => download(`run365days_${meta.data?.year ?? "export"}.csv`, toCsv(rows))}>
            Export CSV
          </button>
        </div>
      </div>
      <div className="overflow-x-auto rounded-card border border-border bg-surface">
        <table className="w-full text-xs" data-testid="act-table">
          <thead className="text-left text-muted">
            <tr>
              {COLUMNS.map((c) => (
                <th key={c.key} className={`cursor-pointer select-none px-3 py-2 font-medium hover:text-text ${c.numeric ? "text-right" : ""} ${sort.key === c.key ? "text-text" : ""}`} onClick={() => setSortKey(c.key)}>
                  {c.label}
                  {sort.key === c.key ? (sort.dir > 0 ? " ↑" : " ↓") : ""}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((a) => (
              <tr key={a.id} className="cursor-pointer border-t border-border hover:bg-surface2" onClick={() => navigate(`/activity/${a.id}`)}>
                {COLUMNS.map((c) => (
                  <td key={c.key} className={`px-3 py-1.5 ${c.numeric ? "text-right tabular-nums" : ""}`}>
                    {c.render(a)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
