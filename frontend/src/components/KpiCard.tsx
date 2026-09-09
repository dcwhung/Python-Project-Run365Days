const ACCENTS = {
  accent: "bg-accent",
  accent2: "bg-accent2",
  warn: "bg-warn",
  danger: "bg-danger",
  violet: "bg-violet",
} as const;

export function KpiCard({
  label,
  value,
  unit,
  sub,
  accent = "accent",
}: {
  label: string;
  value: string;
  unit?: string;
  sub?: string;
  accent?: keyof typeof ACCENTS;
}) {
  return (
    <div className="relative overflow-hidden rounded-card border border-border bg-surface p-4">
      <div className="text-xs uppercase tracking-wide text-muted">{label}</div>
      <div className="mt-1 text-2xl font-semibold">
        {value}
        {unit && <span className="ml-1 text-base text-muted">{unit}</span>}
      </div>
      {sub && <div className="mt-1 text-xs text-muted">{sub}</div>}
      <div className={`absolute inset-x-0 bottom-0 h-0.5 ${ACCENTS[accent]}`} />
    </div>
  );
}
