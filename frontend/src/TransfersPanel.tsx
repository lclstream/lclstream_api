import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { ThemeProvider } from "next-themes"
import { useMemo, useState } from "react"
import { Toaster } from "sonner"

import { client } from "@/client/client.gen"
import { ThemeToggle } from "@/components/Common/Appearance"
import { TransferDetail } from "@/components/Transfers/TransferDetail"
import { TransfersList } from "@/components/Transfers/TransfersList"
import { cn } from "@/lib/utils"

// Dev-only escape hatch: in production the API sits behind a gateway that
// already attaches auth, so the app never sends its own Authorization header.
// Locally we point straight at the real dev k8s API (no gateway in front of
// it from here), so `scripts/dev-token.sh` mints a real S3DF/Dex bearer
// token and bakes it in as VITE_DEV_BEARER_TOKEN; if present, attach it to
// every request.
if (import.meta.env.DEV && import.meta.env.VITE_DEV_BEARER_TOKEN) {
  client.instance.interceptors.request.use((config) => {
    config.headers.set(
      "Authorization",
      `Bearer ${import.meta.env.VITE_DEV_BEARER_TOKEN}`,
    )
    return config
  })
}

export interface TransfersPanelProps {
  /** Base URL for the lclstream_api service; configures the generated client. */
  apiBaseUrl?: string
  /**
   * `inherit`: use the host's shadcn vars, mount no provider.
   * `bundled`: mount `next-themes`; host imports our `style.css`.
   */
  theme?: "inherit" | "bundled"
  className?: string
}

type View = { name: "list" } | { name: "detail"; id: string }

/**
 * Transfers dashboard, mountable standalone or embedded. Owns its QueryClient.
 * List/detail uses view state, not a router, so the host's URL stays untouched.
 */
export function TransfersPanel({
  apiBaseUrl,
  theme = "bundled",
  className,
}: TransfersPanelProps) {
  const queryClient = useMemo(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: { staleTime: 10_000, gcTime: 5 * 60_000 },
        },
      }),
    [],
  )

  useMemo(() => {
    if (apiBaseUrl) client.setConfig({ baseURL: apiBaseUrl })
  }, [apiBaseUrl])

  const [view, setView] = useState<View>({ name: "list" })

  const body = (
    <div className={cn("bg-background text-foreground p-4", className)}>
      <div className="mb-4 flex items-center justify-between">
        <h1 className="text-lg font-semibold">Transfers (dev)</h1>
        <ThemeToggle />
      </div>
      {view.name === "list" ? (
        <TransfersList onSelect={(id) => setView({ name: "detail", id })} />
      ) : (
        <TransferDetail
          transferId={view.id}
          onBack={() => setView({ name: "list" })}
        />
      )}
      <Toaster richColors closeButton />
    </div>
  )

  return (
    <QueryClientProvider client={queryClient}>
      {theme === "bundled" ? (
        <ThemeProvider attribute="class" defaultTheme="system" enableSystem>
          {body}
        </ThemeProvider>
      ) : (
        body
      )}
    </QueryClientProvider>
  )
}
