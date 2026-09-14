import type { KeyboardEvent } from "react";

/**
 * Keys that activate a native `<button>`. Anything we make clickable by hand has
 * to answer to the same two, or keyboard users get a different app than mouse
 * users do.
 */
function isActivationKey(event: KeyboardEvent): boolean {
  return event.key === "Enter" || event.key === " ";
}

/**
 * `onKeyDown` for an element that is clickable but cannot be a `<button>` —
 * a table row, for instance, where a wrapping button would break the table's
 * row/cell semantics. Pair it with `tabIndex={0}` so the element is reachable.
 */
export function activateOnKey(run: () => void) {
  return (event: KeyboardEvent) => {
    if (!isActivationKey(event)) return;
    // Space scrolls the page by default, and Enter can submit a surrounding form.
    event.preventDefault();
    run();
  };
}
