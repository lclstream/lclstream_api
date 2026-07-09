import { formatDistanceToNow } from "date-fns"
import type { TransferPublic } from "@/client"
import { StateBadge } from "@/components/Transfers/StateBadge"

export function TransferCard({
  transfer,
  onClick,
}: {
  transfer: TransferPublic
  onClick: () => void
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="w-full rounded-lg border bg-card p-4 text-left hover:bg-accent/50"
    >
      <div className="flex items-center gap-3">
        <span className="font-mono text-xs text-muted-foreground">
          {transfer.id.slice(0, 8)}
        </span>
        <StateBadge state={transfer.state} />
        <div className="flex-1" />
        <span className="text-xs text-muted-foreground whitespace-nowrap">
          {formatDistanceToNow(new Date(transfer.created_at), {
            addSuffix: true,
          })}
        </span>
      </div>
      <p className="mt-2 text-sm">{transfer.requested_by}</p>
    </button>
  )
}

export function TransferCardSkeleton() {
  return (
    <div className="rounded-lg border p-4 space-y-3 min-h-[76px]">
      <div className="flex items-center gap-3">
        <div className="h-3 w-16 animate-pulse rounded bg-muted" />
        <div className="h-5 w-16 animate-pulse rounded-full bg-muted" />
        <div className="flex-1" />
        <div className="h-3 w-20 animate-pulse rounded bg-muted" />
      </div>
      <div className="h-4 w-32 animate-pulse rounded bg-muted" />
    </div>
  )
}
