import js from "@eslint/js";
import prettierConfig from "eslint-config-prettier";
import globals from "globals";
import jsxA11y from "eslint-plugin-jsx-a11y";
import reactHooks from "eslint-plugin-react-hooks";
import reactRefresh from "eslint-plugin-react-refresh";
import tseslint from "typescript-eslint";

export default tseslint.config(
  { ignores: ["dist", "src/gql", "coverage"] },
  {
    extends: [
      js.configs.recommended,
      ...tseslint.configs.recommended,
      jsxA11y.flatConfigs.recommended,
    ],
    files: ["**/*.{ts,tsx}"],
    languageOptions: { ecmaVersion: 2022, globals: globals.browser },
    plugins: { "react-hooks": reactHooks, "react-refresh": reactRefresh },
    rules: {
      ...reactHooks.configs.recommended.rules,
      "react-refresh/only-export-components": ["warn", { allowConstantExport: true }],
      // jsx-a11y stops at <tr>/<td>/<th> because aria-query gives them the
      // interactive `row`/`cell` roles, so a clickable table row passes every one
      // of its rules while being unreachable by keyboard. Measured on this repo:
      // both the recommended and the strict preset flag 1 of the 3 real
      // violations. This closes the gap the presets leave open.
      "no-restricted-syntax": [
        "error",
        {
          selector:
            "JSXOpeningElement[name.name=/^(tr|td|th|li|dt|dd)$/]:has(JSXAttribute[name.name='onClick']):not(:has(JSXAttribute[name.name='tabIndex']))",
          message:
            "A clickable <tr>/<td>/<th>/<li> needs keyboard parity: either move the handler onto a real <button>, or add tabIndex={0} plus an onKeyDown that fires on Enter and Space.",
        },
      ],
    },
  },
  // Last, so it wins: switches off every ESLint rule that overlaps Prettier, so the
  // two tools can never disagree about the same line. Prettier owns layout; ESLint
  // keeps owning correctness.
  prettierConfig,
);
