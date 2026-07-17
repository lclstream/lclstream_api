import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import type { AxiosError } from "axios"
import { PowerOff } from "lucide-react"
import { toast } from "sonner"

import type { CacheShutdownConflict, CacheState } from "@/client"
import {
  getCachesCachesGetOptions,
  shutdownCacheCachesCacheIdDeleteMutation,
} from "@/client/@tanstack/react-query.gen"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"

const STATE_VARIANT: Record<
  CacheState,
  "default" | "secondary" | "destructive" | "outline"
> = {
  new: "secondary",
  queued: "secondary",
  active: "default",
  completed: "outline",
  failed: "destructive",
  canceled: "outline",
}

/**
 * Badge + shutdown control for the long-lived (shared) cache backing an
 * experiment. Renders nothing if the experiment has no active shared cache.
 */
export function SharedCacheBadge({ experiment }: { experiment: string }) {
  const queryClient = useQueryClient()
  const query = getCachesCachesGetOptions({ query: { experiment } })
  const { data } = useQuery(query)
  const shutdown = useMutation(shutdownCacheCachesCacheIdDeleteMutation())

  const cache = data?.data[0]
  if (!cache) return null

  async function handleShutdown(force = false) {
    if (!cache) return
    try {
      await shutdown.mutateAsync({
        path: { cache_id: cache.id },
        query: force ? { force: true } : {},
      })
      toast.success("Cache shutdown requested")
      queryClient.invalidateQueries({ queryKey: query.queryKey })
    } catch (err) {
      const axiosErr = err as AxiosError<CacheShutdownConflict>
      if (axiosErr.response?.status === 409) {
        const count = axiosErr.response.data.active_transfer_count
        if (
          window.confirm(
            `${count} other active transfer(s) still use this cache. Shut it down anyway?`,
          )
        ) {
          await handleShutdown(true)
        }
        return
      }
      toast.error("Failed to shut down cache")
    }
  }

  return (
    <div className="flex items-center gap-1.5">
      <Badge variant={STATE_VARIANT[cache.state]}>
        Shared cache · {cache.state}
      </Badge>
      <Button
        type="button"
        variant="ghost"
        size="sm"
        disabled={shutdown.isPending}
        onClick={(e) => {
          e.stopPropagation()
          void handleShutdown()
        }}
        title="Shut down shared cache"
      >
        <PowerOff />
      </Button>
    </div>
  )
}
