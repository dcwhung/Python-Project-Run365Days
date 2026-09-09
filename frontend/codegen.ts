import type { CodegenConfig } from "@graphql-codegen/cli";

// schema.graphql is written by `run365-schema` from the Strawberry schema;
// CI checks it is in sync. Generated code lands in src/gql/ (git-ignored).
const config: CodegenConfig = {
  schema: "schema.graphql",
  documents: ["src/data/api/queries.ts"],
  ignoreNoDocuments: true,
  generates: {
    "src/gql/": {
      preset: "client",
      config: {
        useTypeImports: true,
        scalars: { Date: "string" },
        // nullable fields become `T | null`, never optional, so results match src/data/types.ts
        avoidOptionals: { field: true, inputValue: false, object: true, defaultValue: false },
      },
      presetConfig: { fragmentMasking: false },
    },
  },
};

export default config;
