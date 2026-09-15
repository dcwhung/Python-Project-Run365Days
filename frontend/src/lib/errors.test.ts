import { describe, expect, it } from "vitest";
import { GENERIC_ERROR, readableError } from "./errors";

const BUDGET_MESSAGE = "Track request exceeds the per-request point budget.";

/** A `graphql-request` ClientError: `message` is the serialised response + request. */
function clientError(errors: unknown, message = `${BUDGET_MESSAGE}: {"response":{"data":null}}`) {
  return Object.assign(new Error(message), { response: { data: null, errors, status: 200 } });
}

describe("readableError", () => {
  it("should return the GraphQL message when the response carries one", () => {
    expect(readableError(clientError([{ message: BUDGET_MESSAGE }]))).toBe(BUDGET_MESSAGE);
  });

  it("should return the first GraphQL message when the response carries several", () => {
    expect(readableError(clientError([{ message: "first" }, { message: "second" }]))).toBe("first");
  });

  it("should fall back to the error message when there is no GraphQL response", () => {
    expect(readableError(new Error("fetch failed"))).toBe("fetch failed");
  });

  it("should fall back to the error message when errors is not an array", () => {
    expect(readableError(clientError(undefined, "boom"))).toBe("boom");
  });

  it("should fall back to the error message when the errors array is empty", () => {
    expect(readableError(clientError([], "boom"))).toBe("boom");
  });

  // `undefined` alone cannot pin `Array.isArray`, since indexing it is a no-op
  // either way. An array-like object can: without the check, `errors[0]` finds
  // "x" and the caller is handed a message the response never really carried.
  it("should fall back to the error message when errors is an array-like object", () => {
    expect(readableError(clientError({ 0: { message: "x" } }, "boom"))).toBe("boom");
  });

  // Likewise, a string first entry cannot pin `isRecord(first)` -- reading
  // `.message` off it is merely undefined. `null` can: it is the one JSON value
  // that throws when read through, and throwing inside the component that
  // renders the error message trades a bad sentence for a blank page. (A
  // null-safe rewrite of the guard stays green here, and correctly so: for every
  // value a parsed response can hold, `first?.message` and this guard agree.)
  it("should fall back to the error message when the first entry is null", () => {
    expect(readableError(clientError([null], "boom"))).toBe("boom");
  });

  it("should fall back to the error message when the first entry is not an object", () => {
    expect(readableError(clientError(["oops"], "boom"))).toBe("boom");
  });

  it("should fall back to the error message when the first entry has no message", () => {
    expect(readableError(clientError([{ path: ["activity", "track"] }], "boom"))).toBe("boom");
  });

  it("should fall back to the error message when the GraphQL message is blank", () => {
    expect(readableError(clientError([{ message: "   " }], "boom"))).toBe("boom");
  });

  it("should return the generic text when the error has no usable message", () => {
    expect(readableError(new Error(""))).toBe(GENERIC_ERROR);
  });

  it("should return the generic text for values that are not objects", () => {
    expect(readableError(undefined)).toBe(GENERIC_ERROR);
    expect(readableError(null)).toBe(GENERIC_ERROR);
    expect(readableError("plain string")).toBe(GENERIC_ERROR);
  });
});
