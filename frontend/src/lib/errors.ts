/** Shown when an error carries no usable text at all. */
export const GENERIC_ERROR = "unknown error";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() !== "" ? value : undefined;
}

/**
 * The human-readable half of a query failure.
 *
 * `graphql-request` builds `ClientError.message` by serialising the whole
 * response *and* request, so rendering it raw spills the GraphQL document and
 * its variables onto the page. The server's own `errors[0].message` is the
 * sentence a user can act on, so prefer it and fall back only when it is
 * missing (a network failure has no GraphQL response at all).
 */
export function readableError(error: unknown): string {
  if (!isRecord(error)) return GENERIC_ERROR;
  const response = error.response;
  const errors = isRecord(response) ? response.errors : undefined;
  const first = Array.isArray(errors) ? (errors[0] as unknown) : undefined;
  const fromGraphQL = isRecord(first) ? nonEmptyString(first.message) : undefined;
  return fromGraphQL ?? nonEmptyString(error.message) ?? GENERIC_ERROR;
}
