import { useQuery } from "@tanstack/react-query";
import { useDataSource } from "./context";
import type { ActivityFilter, DateRange } from "./types";

/**
 * Thin TanStack Query wrappers over the active DataSource. Keys include the
 * mode so switching sources never serves stale data.
 */

export function useMeta() {
  const src = useDataSource();
  return useQuery({ queryKey: [src.mode, "meta"], queryFn: () => src.meta() });
}

export function useYear() {
  const src = useDataSource();
  return useQuery({ queryKey: [src.mode, "year"], queryFn: () => src.year() });
}

export function useActivities(filter: ActivityFilter = {}) {
  const src = useDataSource();
  return useQuery({
    queryKey: [src.mode, "activities", filter],
    queryFn: () => src.activities(filter),
  });
}

export function useActivity(id: string | undefined) {
  const src = useDataSource();
  return useQuery({
    queryKey: [src.mode, "activity", id],
    queryFn: () => src.activity(id!),
    enabled: !!id,
  });
}

export function useTrack(id: string | undefined, points?: number) {
  const src = useDataSource();
  return useQuery({
    queryKey: [src.mode, "track", id, points ?? null],
    queryFn: () => src.track(id!, points),
    enabled: !!id,
  });
}

export function useWeight(range: DateRange = {}) {
  const src = useDataSource();
  return useQuery({ queryKey: [src.mode, "weight", range], queryFn: () => src.weight(range) });
}

export function useWeather(range: DateRange = {}) {
  const src = useDataSource();
  return useQuery({ queryKey: [src.mode, "weather", range], queryFn: () => src.weather(range) });
}

export function useWarnings(range: DateRange = {}) {
  const src = useDataSource();
  return useQuery({ queryKey: [src.mode, "warnings", range], queryFn: () => src.warnings(range) });
}
