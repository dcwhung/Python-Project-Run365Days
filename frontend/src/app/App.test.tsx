import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import { QueryClient } from "@tanstack/react-query";
import { DataProvider } from "@/data/DataProvider";
import { router } from "./router";
import { fakeSource } from "@/test/fixtures";

function renderApp(path = "/overview") {
  const memory = createMemoryRouter(router.routes, { initialEntries: [path] });
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <DataProvider source={fakeSource()} client={client}>
      <RouterProvider router={memory} />
    </DataProvider>,
  );
}

describe("app shell", () => {
  it("renders the nine views in the nav and the data mode", async () => {
    renderApp();
    const nav = screen.getByRole("navigation", { name: "Views" });
    expect(nav.querySelectorAll("a")).toHaveLength(9);
    expect(screen.getByText("static JSON")).toBeInTheDocument();
    expect(await screen.findByText("2021")).toBeInTheDocument();
  });

  it("shows overview KPIs from the data source", async () => {
    renderApp();
    expect(await screen.findByText("Total Distance")).toBeInTheDocument();
    const kpis = screen.getByTestId("kpis");
    expect(kpis.textContent).toContain("23");
    expect(kpis.textContent).toContain("3");
    expect(screen.getByText(/best 4:30/)).toBeInTheDocument();
  });

  it("shows a placeholder for views not yet rebuilt", async () => {
    renderApp("/weight");
    expect(await screen.findByRole("heading", { name: "Weight" })).toBeInTheDocument();
  });

  it("redirects unknown paths to the overview", async () => {
    renderApp("/nope");
    expect(await screen.findByText("Total Distance")).toBeInTheDocument();
  });
});
