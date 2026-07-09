import { defineConfig } from "@hey-api/openapi-ts"

export default defineConfig({
  input: "../openapi.json",
  output: "./src/client",

  plugins: [
    "@hey-api/client-axios",
    {
      name: "@hey-api/sdk",
      operations: {
        strategy: "byTags",
        containerName: "{{name}}Service",
        nesting: "operationId",
      },
      // Validate responses against the generated zod schemas at runtime, so
      // malformed/unexpected API payloads surface as query errors instead of
      // silently propagating bad data into the UI.
      validator: {
        response: "zod",
      },
    },
    {
      name: "@hey-api/schemas",
      type: "json",
    },
    "@tanstack/react-query",
    "zod",
  ],
})
