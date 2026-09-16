import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { useActivities, useWeather, useWeight, useYear } from "@/data/hooks";
import { OverviewView } from "./OverviewView";

vi.mock("@/data/hooks", () => ({
  useActivities: vi.fn(),
  useWeather: vi.fn(),
  useWeight: vi.fn(),
  useYear: vi.fn(),
}));

/** The GraphQL error the API really returns when a document outgrows the list row budget. */
const BUDGET_MESSAGE = "list row budget exhausted: one request may read at most 4000 rows";

/** A distinctive slice of the document `graphql-request` folds into `message`. */
const DOCUMENT_MARKER = "query Year";

/**
 * Minimal stand-in for TanStack's UseQueryResult union: these views read only
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
 * against the Flask API — the real blob measured 1247 characters.
 */
function budgetClientError(): Error {
  const errors = [{ message: BUDGET_MESSAGE, locations: [{ line: 7, column: 5 }], path: ["year"] }];
  const response = { data: null, errors, status: 200, headers: {}, body: JSON.stringify({ data: null, errors }) };
  const request = {
    query: "\n  query Year($fromDate: Date, $toDate: Date) {\n    year { year totals { runs } }\n  }\n",
    variables: { fromDate: "2021-01-01", toDate: "2021-12-31" },
  };
  return Object.assign(new Error(`${BUDGET_MESSAGE}: ${JSON.stringify({ response, request })}`), { response, request });
}

function wire({ year, activities }: { year?: QueryStub; activities?: QueryStub } = {}) {
  vi.mocked(useYear).mockReturnValue((year ?? succeeded({})) as unknown as ReturnType<typeof useYear>);
  vi.mocked(useActivities).mockReturnValue((activities ?? succeeded([])) as unknown as ReturnType<typeof useActivities>);
  vi.mocked(useWeight).mockReturnValue(succeeded([]) as unknown as ReturnType<typeof useWeight>);
  vi.mocked(useWeather).mockReturnValue(succeeded([]) as unknown as ReturnType<typeof useWeather>);
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("OverviewView", () => {
  // Asserts on what the paragraph says, not on which helper produced it: that
  // is what actually keeps the 1247-character blob off the page.
  it("should show only the server's sentence when the year query fails with a ClientError", () => {
    wire({ year: failed(budgetClientError()) });
    render(<OverviewView />);
    const message = screen.getByText(/Could not load data:/);
    expect(message).toHaveTextContent(BUDGET_MESSAGE);
    expect(message.textContent).not.toContain('{"response"');
    expect(message.textContent).not.toContain(DOCUMENT_MARKER);
    expect(message.textContent).not.toContain('"variables"');
  });

  it("should show only the server's sentence when the activities query fails with a ClientError", () => {
    wire({ activities: failed(budgetClientError()) });
    render(<OverviewView />);
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
    wire({ year: failed(new Error("fetch failed")) });
    render(<OverviewView />);
    expect(screen.getByText("Could not load data: fetch failed")).toBeInTheDocument();
  });
});
