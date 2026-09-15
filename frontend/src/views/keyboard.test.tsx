import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, within } from "@testing-library/react";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import { QueryClient } from "@tanstack/react-query";
import { DataProvider } from "@/data/DataProvider";
import { DataTable } from "@/components/ui/DataTable";
import { router } from "@/app/router";
import { act, fakeSource } from "@/test/fixtures";
import type { DataSource } from "@/data/types";

vi.mock("react-chartjs-2", () => {
  const Stub = () => <div data-testid="chart" />;
  return { Bar: Stub, Line: Stub, Scatter: Stub, Chart: Stub };
});

function source(): DataSource {
  return fakeSource([
    act({ id: "a", date: "2021-01-08", startTime: "07:00", distanceKm: 5 }),
    act({ id: "b", date: "2021-01-09", startTime: "18:30", distanceKm: 10, paceSecPerKm: 360 }),
  ]);
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

/**
 * Every one of these was reachable with a mouse and dead to the keyboard. The
 * assertions are deliberately about behaviour (focusable, and Enter/Space go
 * where the click goes) rather than about which attribute delivers it.
 */
describe("keyboard parity for row and list navigation", () => {
  it("should open the activity when Enter is pressed on a Recent Runs row", async () => {
    const memory = renderAt("/overview");
    await screen.findByTestId("recent");
    const row = within(screen.getByTestId("recent")).getAllByRole("row")[1];
    expect(row).toHaveAttribute("tabindex", "0");
    fireEvent.keyDown(row, { key: "Enter" });
    expect(memory.state.location.pathname).toBe("/activity/b");
  });

  it("should open the activity when Space is pressed on a Recent Runs row", async () => {
    const memory = renderAt("/overview");
    await screen.findByTestId("recent");
    fireEvent.keyDown(within(screen.getByTestId("recent")).getAllByRole("row")[1], { key: " " });
    expect(memory.state.location.pathname).toBe("/activity/b");
  });

  it("should ignore keys that do not activate a button", async () => {
    const memory = renderAt("/overview");
    await screen.findByTestId("recent");
    fireEvent.keyDown(within(screen.getByTestId("recent")).getAllByRole("row")[1], { key: "a" });
    expect(memory.state.location.pathname).toBe("/overview");
  });

  it("should expose each personal best as a real button that navigates", async () => {
    const memory = renderAt("/overview");
    await screen.findByTestId("pbs");
    const buttons = within(screen.getByTestId("pbs")).getAllByRole("button");
    expect(buttons).toHaveLength(5);
    fireEvent.click(buttons[0]);
    expect(memory.state.location.pathname).toMatch(/^\/activity\//);
  });

  it("should open the activity when Enter is pressed on an Activities row", async () => {
    const memory = renderAt("/activities");
    await screen.findByTestId("act-table");
    const row = within(screen.getByTestId("act-table")).getAllByRole("row")[1];
    expect(row).toHaveAttribute("tabindex", "0");
    fireEvent.keyDown(row, { key: "Enter" });
    expect(memory.state.location.pathname).toBe("/activity/b");
  });

  it("should sort from a focusable header button and report the direction", async () => {
    renderAt("/activities");
    await screen.findByTestId("act-table");
    const header = within(screen.getByTestId("act-table")).getAllByRole("columnheader")[1];
    expect(header).toHaveAttribute("aria-sort", "none");
    fireEvent.click(within(header).getByRole("button"));
    expect(header).toHaveAttribute("aria-sort");
    expect(header.getAttribute("aria-sort")).not.toBe("none");
  });

  it("should make DataTable rows with an activity focusable and keyboard-activatable", () => {
    const memory = createMemoryRouter(
      [
        {
          path: "/",
          element: (
            <DataTable
              testId="dt"
              cols={[{ h: "Run" }]}
              rows={[
                { id: "a", c: ["clickable"] },
                { key: "gap", c: ["not clickable"] },
              ]}
            />
          ),
        },
        { path: "/activity/:id", element: <p>detail</p> },
      ],
      { initialEntries: ["/"] },
    );
    render(<RouterProvider router={memory} />);
    const [clickable, plain] = within(screen.getByTestId("dt")).getAllByRole("row").slice(1);
    expect(plain).not.toHaveAttribute("tabindex");
    expect(clickable).toHaveAttribute("tabindex", "0");
    fireEvent.keyDown(clickable, { key: "Enter" });
    expect(memory.state.location.pathname).toBe("/activity/a");
  });
});
