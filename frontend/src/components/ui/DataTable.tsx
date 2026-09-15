import type { ReactNode } from "react";
import { useNavigate } from "react-router-dom";
import { activateOnKey } from "@/lib/a11y";

export interface Col {
  h: ReactNode;
  num?: boolean;
}
export interface Row {
  id?: string;
  key?: string;
  c: ReactNode[];
  bold?: boolean;
}

/** Small read-only table; rows with an id open that activity on click. */
export function DataTable({ cols, rows, testId }: { cols: Col[]; rows: Row[]; testId?: string }) {
  const navigate = useNavigate();
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs" data-testid={testId}>
        <thead className="text-left text-muted">
          <tr>
            {cols.map((c, i) => (
              <th key={i} className={`py-1 pr-3 font-medium ${c.num ? "text-right" : ""}`}>
                {c.h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, ri) => {
            // A row cannot hold a <button> without breaking the row/cell
            // semantics screen readers rely on, so the row itself is made
            // focusable and answers to the same keys a button would.
            const open = r.id ? () => navigate(`/activity/${r.id}`) : undefined;
            return (
              <tr
                key={r.key ?? r.id ?? ri}
                className={`border-t border-border ${r.id ? "cursor-pointer hover:bg-surface2" : ""} ${r.bold ? "font-semibold" : ""}`}
                tabIndex={open ? 0 : undefined}
                onClick={open}
                onKeyDown={open && activateOnKey(open)}
              >
                {r.c.map((v, i) => (
                  <td
                    key={i}
                    className={`py-1.5 pr-3 ${cols[i]?.num ? "text-right tabular-nums" : ""}`}
                  >
                    {v}
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
