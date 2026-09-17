import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { buildSchema, type GraphQLObjectType } from "graphql";
// `?raw` rather than fs: Vite resolves the path at transform time, so this
// keeps pointing at frontend/schema.graphql no matter what directory the
// runner was started from.
import schemaSdl from "../../../schema.graphql?raw";
import { useActivities, useMeta } from "@/data/hooks";
import { ActivitiesView } from "./ActivitiesView";

vi.mock("@/data/hooks", () => ({
  useActivities: vi.fn(),
  useMeta: vi.fn(),
}));

/**
 * The GraphQL error the API really returns when a document outgrows the list
 * row budget. The `4000` in it is `MAX_LIST_ROWS_PER_REQUEST`, which lives in
 * Python; the test at the bottom of this file holds the two together.
 */
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
 * A `graphql-request` ClientError in the shape one arrives in: `message` is the
 * serialised response *and* request, so rendering it raw spills the whole
 * GraphQL document and its variables onto the page.
 *
 * Hand-built, not captured, and two measurements say so. The document below
 * declares four variables and uses one, which `graphql-core` rejects against
 * the real schema with three "Variable '$x' is never used" errors — so a real
 * run would have been refused at validation and never reached the list row
 * budget this fixture blames. And the blob it builds is 708 characters, not
 * the 1247 the previous wording claimed. What *is* true, and is the part worth
 * keeping, is that `ActivitiesQuery` in `src/data/api/queries.ts` really does
 * carry these four variables (S-100, same family as S-074).
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
  // is what actually keeps the serialised blob off the page. (It measures 708
  // characters, not the 1247 this comment used to quote -- S-100.)
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

  // CUI-0046, the second case of what S-101 fixed in ActivityView.test.tsx.
  // `BUDGET_MESSAGE` quotes `MAX_LIST_ROWS_PER_REQUEST`, which lives in Python,
  // and nothing held the two together: tuning the budget would leave this
  // fixture quoting a number the server had stopped using, with every gate
  // green.
  //
  // frontend/schema.graphql is the link, exactly as in CUI-0034: run365-schema
  // --check holds it equal to the Python constant, and this holds the fixture
  // equal to it. Reading the number back out of the SDL is the whole point --
  // spelling `4000` here would be a third copy rather than a guard.
  //
  // Anchored on `Query.activities` by name, which S-101 did not have to do.
  // Unlike the `10000` it read, `4000` reaches the SDL five times: activities,
  // weight, weather, warnings and year all carry `LIST_ROWS_NOTE`, one shared
  // f-string over the one constant, so a whole-document search would not say
  // which field it had found. `activities` is the field this view's document
  // opens and the one the fixture above blames in `path`, so its description is
  // the one this fixture is answerable to.
  it("quotes the list row budget the generated SDL advertises for Query.activities", () => {
    const sdl = buildSchema(schemaSdl);
    const activities = (sdl.getType("Query") as GraphQLObjectType).getFields().activities;

    // Assert the phrasing was found before comparing, so a reworded description
    // fails loudly instead of skipping the comparison and going green on a
    // number it could no longer locate.
    const advertised = /at most (\d+) rows of pages/.exec(activities.description ?? "");
    expect(
      advertised,
      `Query.activities description states no "at most N rows of pages": ${activities.description}`,
    ).not.toBeNull();
    expect(BUDGET_MESSAGE).toContain(`at most ${advertised![1]} rows`);
  });
});
