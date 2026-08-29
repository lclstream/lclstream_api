import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { ThemeProvider } from "next-themes"
import { lazy, Suspense, useRef, useState } from "react"
import { Toaster } from "sonner"

import { client } from "@/client/client.gen"
import { ThemeToggle } from "@/components/Common/Appearance"
import { TransfersList } from "@/components/Transfers/TransfersList"
import { cn } from "@/lib/utils"

// Code-split: list-only hosts skip date-fns.
const TransferDetail = lazy(() =>
  import("@/components/Transfers/TransferDetail").then((m) => ({
    default: m.TransferDetail,
  })),
)

// Prod sits behind a gateway that attaches auth.
// Dev hits the k8s API direct; `scripts/dev-token.sh` mints the token.
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
  const [queryClient] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: { staleTime: 10_000, gcTime: 5 * 60_000 },
        },
      }),
  )

  const appliedBaseUrl = useRef<string | undefined>(undefined)
  if (apiBaseUrl && appliedBaseUrl.current !== apiBaseUrl) {
    client.setConfig({ baseURL: apiBaseUrl })
    appliedBaseUrl.current = apiBaseUrl
  }

  const [view, setView] = useState<View>({ name: "list" })

  const body = (
    <div
      className={cn(
        "flex h-screen flex-col overflow-hidden bg-background text-foreground p-4",
        className,
      )}
    >
      <div className="mb-4 flex shrink-0 items-center justify-between">
        <h1 className="text-lg font-semibold">Transfers (dev)</h1>
        <ThemeToggle />
      </div>
      {view.name === "list" ? (
        <TransfersList onSelect={(id) => setView({ name: "detail", id })} />
      ) : (
        <Suspense fallback={null}>
          <TransferDetail
            transferId={view.id}
            onBack={() => setView({ name: "list" })}
          />
        </Suspense>
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
