import { useMemo, type ReactNode } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { DataSource } from "./types";
import { DataSourceContext } from "./context";
import { createSourceFromEnv } from "./source";

export function DataProvider({
  children,
  source,
  client,
}: {
  children: ReactNode;
  /** Override for tests; defaults to the env-driven source. */
  source?: DataSource;
  client?: QueryClient;
}) {
  const value = useMemo(
    () => source ?? createSourceFromEnv(import.meta.env as Record<string, string | undefined>),
    [source],
  );
  const queryClient = useMemo(
    () =>
      client ??
      new QueryClient({
        defaultOptions: { queries: { staleTime: Infinity, retry: 1, refetchOnWindowFocus: false } },
      }),
    [client],
  );
  return (
    <DataSourceContext.Provider value={value}>
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    </DataSourceContext.Provider>
  );
}
