import { NavLink, Outlet } from "react-router-dom";
import { VIEWS } from "./views";
import { useMeta } from "@/data/hooks";
import { useDataSource } from "@/data/context";
import { WarningSprite } from "@/components/WarningSprite";

export function Layout() {
  const meta = useMeta();
  const source = useDataSource();
  return (
    <div className="flex h-full flex-col">
      <WarningSprite />
      <header className="flex items-center gap-4 border-b border-border bg-surface px-5 py-3">
        <div className="text-base font-semibold tracking-wide">
          Run<span className="text-warn">365</span>Days
          <span className="ml-2 rounded bg-surface2 px-2 py-0.5 text-xs text-muted">
            {meta.data?.year ?? "…"}
          </span>
        </div>
        <nav className="flex flex-wrap gap-1" aria-label="Views">
          {VIEWS.map((v) => (
            <NavLink
              key={v.path}
              to={`/${v.path}`}
              className={({ isActive }) =>
                `rounded px-3 py-1.5 text-sm transition-colors ${
                  isActive ? "bg-accent/15 text-accent" : "text-muted hover:bg-surface2 hover:text-text"
                }`
              }
            >
              {v.label}
            </NavLink>
          ))}
        </nav>
        <div className="ml-auto text-xs text-muted" title="Where the data comes from">
          {source.mode === "api" ? "GraphQL API" : "static JSON"}
        </div>
      </header>
      <main className="flex-1 overflow-auto p-5">
        <Outlet />
      </main>
    </div>
  );
}
