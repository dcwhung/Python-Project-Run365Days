import { createContext, useContext } from "react";
import type { DataSource } from "./types";

export const DataSourceContext = createContext<DataSource | null>(null);

export function useDataSource(): DataSource {
  const ctx = useContext(DataSourceContext);
  if (!ctx) throw new Error("useDataSource must be used inside <DataProvider>");
  return ctx;
}
