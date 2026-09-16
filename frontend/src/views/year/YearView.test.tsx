import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { useActivities, useWeight, useYear } from "@/data/hooks";
import { YearView } from "./YearView";

vi.mock("@/data/hooks", () => ({
  useActivities: vi.fn(),
  useWeight: vi.fn(),
  useYear: vi.fn(),
}));

vi.mock("react-chartjs-2", () => ({ Line: () => <div data-testid="chart" /> }));

/** The GraphQL error the API really returns when a document outgrows the list row budget. */
const BUDGET_MESSAGE = "list row budget exhausted: one request may read at most 4000 rows";

/** A distinctive slice of the document `graphql-request` folds into `message`. */
const DOCUMENT_MARKER = "query Year";

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
 * Shaped like a `graphql-request` ClientError, not captured from one:
 * `message` is the serialised response *and* request, so rendering it raw
 * spills the whole GraphQL document and its variables onto the page.
 *
 * The document here is a stand-in. The real `YearQuery` (`@/data/api/queries.ts`)
 * takes no variables, and one `year` field spends a single page of the row
 * budget, so this exact request could not have produced this error. The API
 * does refuse at `path: ["year"]` once earlier fields in the same document
 * have spent the budget — what it never sends is this document. What the view
 * has to survive is the shape, and the shape is what this builds.
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

function wire(year: QueryStub) {
  vi.mocked(useYear).mockReturnValue(year as unknown as ReturnType<typeof useYear>);
  vi.mocked(useActivities).mockReturnValue(succeeded([]) as unknown as ReturnType<typeof useActivities>);
  vi.mocked(useWeight).mockReturnValue(succeeded([]) as unknown as ReturnType<typeof useWeight>);
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("YearView", () => {
  // Asserts on what the paragraph says, not on which helper produced it: that
  // is what actually keeps the serialised blob off the page.
  it("should show only the server's sentence when the year query fails with a ClientError", () => {
    wire(failed(budgetClientError()));
    render(<YearView />);
    const message = screen.getByText(/Could not load data:/);
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
    render(<YearView />);
    expect(screen.getByText("Could not load data: fetch failed")).toBeInTheDocument();
  });
});
