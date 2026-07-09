/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_API_URL: string
  /** Dev-only: see scripts/dev-token.sh. */
  readonly VITE_DEV_BEARER_TOKEN?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}
