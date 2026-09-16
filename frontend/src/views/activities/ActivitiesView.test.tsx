import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { useActivities, useMeta } from "@/data/hooks";
import { ActivitiesView } from "./ActivitiesView";

vi.mock("@/data/hooks", () => ({
  useActivities: vi.fn(),
  useMeta: vi.fn(),
}));

/** The GraphQL error the API really returns when a document outgrows the list row budget. */
const BUDGET_MESSAGE = "list row budget exhausted: one request may read at most 4000 rows";

/** A distinctive slice of the document `graphql-request` folds into `message`. */
const DOCUMENT_MARKER = "query Activities";

/**
 * Minimal stand-in for TanStack's UseQueryResult union: this view reads only
 * `data` / `isPending` / `isError` / `error`, so a structural stub cast at the
 * mock boundary keeps the tests free of query-client plumbing.
 */
type QueryStub = { data: unknown; isPending: boolean; isError: boolean; error: Error | null };

function succeeded(data: unknown): QueryStub {
  return { data, isPending: false, isError: false, error: null };
}

function failed(error: Error): QueryStub {
  return { data: undefined, isPending: false, isError: true, error };
}

/**
 * A `graphql-request` ClientError exactly as it arrives: `message` is the
 * serialised response *and* request, so rendering it raw spills the whole
 * GraphQL document and its variables onto the page. Captured from a real run
 * against the Flask API — the real blob measured 1247 characters, and the
 * `ActivitiesQuery` this view sends really does carry these four variables.
 */
function budgetClientError(): Error {
  const errors = [{ message: BUDGET_MESSAGE, locations: [{ line: 7, column: 5 }], path: ["activities"] }];
  const response = { data: null, errors, status: 200, headers: {}, body: JSON.stringify({ data: null, errors }) };
  const request = {
    query: "\n  query Activities($fromDate: Date, $toDate: Date, $minKm: Float, $hasGps: Boolean) {\n    activities(fromDate: $fromDate) { id date }\n  }\n",
    variables: { fromDate: "2021-01-01", toDate: "2021-12-31", minKm: 1, hasGps: true },
  };
  return Object.assign(new Error(`${BUDGET_MESSAGE}: ${JSON.stringify({ response, request })}`), { response, request });
}

function wire(all: QueryStub) {
  vi.mocked(useActivities).mockReturnValue(all as unknown as ReturnType<typeof useActivities>);
  vi.mocked(useMeta).mockReturnValue(succeeded({ year: 2021 }) as unknown as ReturnType<typeof useMeta>);
}

function renderView() {
  render(
    <MemoryRouter>
      <ActivitiesView />
    </MemoryRouter>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("ActivitiesView", () => {
  // Asserts on what the paragraph says, not on which helper produced it: that
  // is what actually keeps the 1247-character blob off the page.
  it("should show only the server's sentence when the activities query fails with a ClientError", () => {
    wire(failed(budgetClientError()));
    renderView();
    const message = screen.getByText(/Could not load activities:/);
    expect(message).toHaveTextContent(BUDGET_MESSAGE);
    expect(message.textContent).not.toContain('{"response"');
    expect(message.textContent).not.toContain(DOCUMENT_MARKER);
    expect(message.textContent).not.toContain('"variables"');
  });

  // A network failure carries no GraphQL response at all, so the error's own
  // message is the only text worth showing. Pinned so a rewrite that reads the
  // GraphQL message alone cannot silently degrade this to "unknown error".
  it("should fall back to the error's own message when the failure carries no GraphQL response", () => {
    wire(failed(new Error("fetch failed")));
    renderView();
    expect(screen.getByText("Could not load activities: fetch failed")).toBeInTheDocument();
  });
});
