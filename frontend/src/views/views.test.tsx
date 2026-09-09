import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, within } from "@testing-library/react";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import { QueryClient } from "@tanstack/react-query";
import { DataProvider } from "@/data/DataProvider";
import { router } from "@/app/router";
import { act, fakeSource } from "@/test/fixtures";
import type { DataSource, TrackPoint } from "@/data/types";

vi.mock("react-chartjs-2", () => {
  const Stub = ({ data }: { data: unknown }) => <div data-testid="chart">{JSON.stringify(data).length}</div>;
  return { Bar: Stub, Line: Stub, Scatter: Stub };
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

function source(): DataSource {
  const acts = [
    act({ id: "a", date: "2021-01-08", startTime: "07:00", distanceKm: 5, warnings: ["THUNDERSTORM WARNING"], weather: { description: "Rain", tempC: 19, humidityPct: 80, windKmh: 10 } }),
    act({ id: "b", date: "2021-01-09", startTime: "18:30", distanceKm: 10, paceSecPerKm: 360, hasGps: false }),
  ];
  return { ...fakeSource(acts), track: async (id) => (id === "a" ? TRACK : []) };
}

function renderAt(path: string) {
  const memory = createMemoryRouter(router.routes, { initialEntries: [path] });
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <DataProvider source={source()} client={client}>
      <RouterProvider router={memory} />
    </DataProvider>,
  );
  return memory;
}

describe("Overview", () => {
  it("renders heatmap, charts, recent runs, personal bests and weather strip", async () => {
    renderAt("/overview");
    expect(await screen.findByText("Total Distance")).toBeInTheDocument();
    expect(screen.getByTestId("heatmap").querySelectorAll("button")).toHaveLength(365);
    expect(screen.getAllByTestId("chart").length).toBeGreaterThanOrEqual(5);
    expect(within(screen.getByTestId("recent")).getAllByRole("row")).toHaveLength(3);
    expect(within(screen.getByTestId("pbs")).getAllByRole("listitem")).toHaveLength(5);
    expect(screen.getByTestId("weather-strip")).toHaveTextContent("Severe Warning Runs");
  });

  it("navigates to an activity when a heatmap day is clicked", async () => {
    const memory = renderAt("/overview");
    await screen.findByText("Total Distance");
    fireEvent.click(screen.getByLabelText(/08 Jan 2021 — 5.00 km/));
    expect(memory.state.location.pathname).toBe("/activity/a");
  });
});

describe("Activity", () => {
  it("shows the latest run by default and an indoor notice", async () => {
    renderAt("/activity");
    expect(await screen.findByRole("heading", { name: /Indoor Run · Day 1/ })).toBeInTheDocument();
    expect(screen.getByText(/No GPS track recorded/)).toBeInTheDocument();
    expect(screen.getByTestId("act-wx")).toHaveTextContent("no weather record");
  });

  it("renders the route, live readout and charts for a GPS run and scrubs", async () => {
    renderAt("/activity/a");
    expect(await screen.findByRole("heading", { name: /Outdoor Run/ })).toBeInTheDocument();
    expect(screen.getByLabelText("Route map")).toBeInTheDocument();
    expect(screen.getByTestId("elapsed")).toHaveTextContent("0:00");
    fireEvent.change(screen.getByLabelText("Position in activity"), { target: { value: "3" } });
    expect(screen.getByTestId("elapsed")).toHaveTextContent("0:30");
    expect(screen.getByTestId("value-ele")).toHaveTextContent("12"); // smoothed over neighbours
    expect(screen.getByTestId("act-wx")).toHaveTextContent("Rain");
    expect(screen.getByRole("button", { name: "Play" })).toBeInTheDocument();
  });

  it("steps to the next run", async () => {
    const memory = renderAt("/activity/a");
    await screen.findByRole("heading", { name: /Outdoor Run/ });
    fireEvent.click(screen.getByLabelText("Next day"));
    expect(memory.state.location.pathname).toBe("/activity/b");
  });
});

describe("Activities", () => {
  it("lists, filters and sorts runs", async () => {
    renderAt("/activities");
    expect(await screen.findByTestId("act-count")).toHaveTextContent("2 of 2 runs · 15.0 km");
    const rows = () => within(screen.getByTestId("act-table")).getAllByRole("row").slice(1);
    expect(rows()[0]).toHaveTextContent("Jan 09");
    fireEvent.click(screen.getByText("Dist."));
    expect(rows()[0]).toHaveTextContent("10.00 km");
    fireEvent.change(screen.getByLabelText("GPS"), { target: { value: "0" } });
    expect(screen.getByTestId("act-count")).toHaveTextContent("1 of 2 runs");
    fireEvent.change(screen.getByLabelText("Search"), { target: { value: "thunder" } });
    expect(screen.getByTestId("act-count")).toHaveTextContent("0 of 2 runs");
  });
});
