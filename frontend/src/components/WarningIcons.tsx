import { warnInfo } from "@/lib/weather";

/** Row of HKO signal icons; unknown signals are skipped, empty lists render a dash. */
export function WarningIcons({ signals, dash = true }: { signals: string[]; dash?: boolean }) {
  const icons = signals.map((s) => ({ s, w: warnInfo(s) })).filter((x) => x.w);
  if (!icons.length) return dash ? <span className="text-muted">–</span> : null;
  return (
    <span className="inline-flex items-center gap-1 align-middle">
      {icons.map(({ s, w }) => (
        <svg key={s} className="h-4 w-4" role="img" aria-label={w!.name}>
          <title>{w!.name}</title>
          <use href={`#${w!.id}`} />
        </svg>
      ))}
    </span>
  );
}
