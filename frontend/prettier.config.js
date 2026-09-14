/**
 * Formatting is deliberately pinned to what this codebase already is, not to a
 * greenfield preference. Measured over the 86 tracked source files before this
 * config existed: indentation was 100% spaces in even-numbered runs (2/4/6/8...,
 * zero tabs) and quoting was 321 double-quoted imports to 0 single-quoted.
 *
 * The team default (tabWidth 4, singleQuote) would therefore have rewritten
 * nearly every line of every file to settle two axes the codebase had already
 * settled consistently -- destroying git blame for no readability gain, which is
 * the very harm this formatter is being added to prevent. printWidth and
 * trailingComma are the axes that were actually unenforced, so those follow the
 * team default.
 */
export default {
  printWidth: 100,
  tabWidth: 2,
  singleQuote: false,
  trailingComma: "all",
  semi: true,
};
