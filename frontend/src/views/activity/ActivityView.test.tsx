import { describe, expect, it, vi, beforeEach } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { act as activity } from "@/test/fixtures";
import type { Activity, TrackPoint } from "@/data/types";
import { useActivities, useActivity, useTrack } from "@/data/hooks";
import { ActivityView } from "./ActivityView";

vi.mock("@/data/hooks", () => ({
  useActivities: vi.fn(),
  useActivity: vi.fn(),
  useTrack: vi.fn(),
}));

/** The four `SeriesChart` canvases, labelled by their spec title. */
const CHART_LABELS = ["Elevation (m)", "Pace (min/km)", "Run Cadence (spm)", "Temperature (°C)"];

const ACT: Activity = activity({
  id: "a",
  date: "2021-01-08",
  startTime: "07:00",
  dayOfYear: 8,
  distanceKm: 5,
  weather: { description: "Rain", tempC: 19, humidityPct: 80, windKmh: 10 },
});

const TRACK: TrackPoint[] = [0, 1, 2, 3].map((i) => ({
  sec: i * 10,
  lat: 22.3 + i * 0.001,
  lon: 114.2,
  elevationM: 10 + i,
  distanceM: i * 30,
  speedMps: 3,
  cadence: 85,
  tempC: 20,
}));

/**
 * Minimal stand-ins for TanStack's UseQueryResult union. The view only reads
 * `data` / `isPending` / `isError` / `error` / `refetch`, so a structural stub
 * cast at the mock boundary keeps the tests free of query-client plumbing.
 */
type QueryStub = { data: unknown; isPending: boolean; isError: boolean; error: Error | null; refetch: () => Promise<unknown> };

function succeeded(data: unknown): QueryStub {
  return { data, isPending: false, isError: false, error: null, refetch: vi.fn(async () => undefined) };
}

function failed(message: string): QueryStub {
  return { data: undefined, isPending: false, isError: true, error: new Error(message), refetch: vi.fn(async () => undefined) };
}

function wire({ all, one, track }: { all?: QueryStub; one?: QueryStub; track?: QueryStub } = {}) {
  const stubs = {
    all: all ?? succeeded([ACT]),
    one: one ?? succeeded(ACT),
    track: track ?? succeeded(TRACK),
  };
  vi.mocked(useActivities).mockReturnValue(stubs.all as unknown as ReturnType<typeof useActivities>);
  vi.mocked(useActivity).mockReturnValue(stubs.one as unknown as ReturnType<typeof useActivity>);
  vi.mocked(useTrack).mockReturnValue(stubs.track as unknown as ReturnType<typeof useTrack>);
  return stubs;
}

function renderView(id = "a") {
  render(
    <MemoryRouter initialEntries={[`/activity/${id}`]}>
      <Routes>
        <Route path="/activity/:id" element={<ActivityView />} />
      </Routes>
    </MemoryRouter>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("ActivityView", () => {
  it("should render the route, live card and charts when every query succeeds", () => {
    wire();
    renderView();
    expect(screen.getByRole("heading", { name: /Outdoor Run · Day 8/ })).toBeInTheDocument();
    expect(screen.getByLabelText("Route map")).toBeInTheDocument();
    expect(screen.getByTestId("elapsed")).toBeInTheDocument();
    for (const label of CHART_LABELS) expect(screen.getByLabelText(label)).toBeInTheDocument();
    expect(screen.queryByTestId("track-error")).not.toBeInTheDocument();
  });

  it("should show a track-specific error instead of \"Activity not found.\" when the track query fails", () => {
    wire({ track: failed("boom") });
    renderView();
    expect(screen.queryByText("Activity not found.")).not.toBeInTheDocument();
    expect(screen.getByTestId("track-error")).toHaveTextContent("Could not load the track for this activity: boom");
    expect(screen.queryByLabelText("Route map")).not.toBeInTheDocument();
    for (const label of CHART_LABELS) expect(screen.queryByLabelText(label)).not.toBeInTheDocument();
  });

  it("should keep rendering the activity header, weather and KPIs when the track query fails", () => {
    wire({ track: failed("boom") });
    renderView();
    expect(screen.getByRole("heading", { name: /Run · Day 8/ })).toBeInTheDocument();
    expect(screen.getByText(/08 Jan 2021/)).toBeInTheDocument();
    expect(screen.getByTestId("act-wx")).toHaveTextContent("Rain");
    expect(screen.getByTestId("act-kpis")).toHaveTextContent("5.00");
    expect(screen.getByTestId("act-kpis")).toHaveTextContent("Avg Pace");
  });

  it("should refetch the track when Retry is pressed", () => {
    const stubs = wire({ track: failed("boom") });
    renderView();
    fireEvent.click(screen.getByRole("button", { name: /retry/i }));
    expect(stubs.track.refetch).toHaveBeenCalledTimes(1);
  });

  it("should still say \"Activity not found.\" when the activity really is missing", () => {
    wire({ one: succeeded(null), track: succeeded([]) });
    renderView("missing");
    expect(screen.getByText("Activity not found.")).toBeInTheDocument();
  });

  it("should show an activity-specific error when the activity query fails", () => {
    wire({ one: failed("activity exploded") });
    renderView();
    expect(screen.queryByText("Activity not found.")).not.toBeInTheDocument();
    expect(screen.getByText(/Could not load this activity: activity exploded/)).toBeInTheDocument();
  });

  it("should show a list-specific error when the activities query fails", () => {
    wire({ all: failed("list exploded") });
    renderView();
    expect(screen.getByText(/Could not load activities: list exploded/)).toBeInTheDocument();
  });
});
