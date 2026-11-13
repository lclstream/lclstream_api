import path from "node:path"
import tailwindcss from "@tailwindcss/vite"
import react from "@vitejs/plugin-react-swc"
import { defineConfig } from "vite"

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const isLib = mode === "lib"

  return {
    resolve: {
      alias: {
        "@": path.resolve(__dirname, "./src"),
      },
    },
    server: {
      host: true,
      port: 5175,
      strictPort: true,
    },
    plugins: [react(), tailwindcss()],
    ...(isLib
      ? {
          build: {
            lib: {
              entry: path.resolve(__dirname, "src/index.ts"),
              name: "LclstreamTransfers",
              formats: ["es"] as const,
              fileName: () => "transfers-panel.js",
              cssFileName: "transfers-panel",
            },
            cssCodeSplit: false,
            rollupOptions: {
              external: ["react", "react-dom", "react/jsx-runtime"],
              output: {
                globals: {
                  react: "React",
                  "react-dom": "ReactDOM",
                },
              },
            },
          },
        }
      : {}),
  }
})
