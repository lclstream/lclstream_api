import { useVirtualizer } from "@tanstack/react-virtual"
import { Inbox, Loader2, TriangleAlert } from "lucide-react"
import { useRef } from "react"
import { StatusPlaceholder } from "@/components/Common/StatusPlaceholder"
import {
  TransferCard,
  TransferCardSkeleton,
} from "@/components/Transfers/TransferCard"
import { useTransfersQuery } from "@/hooks/useTransfersQuery"

const SKELETON_KEYS = ["a", "b", "c", "d", "e"]

export function TransfersList({
  onSelect,
}: {
  onSelect: (id: string) => void
}) {
  const {
    transfers,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
    isPending,
    isError,
  } = useTransfersQuery()

  const scrollContainerRef = useRef<HTMLDivElement>(null)

  const virtualizer = useVirtualizer({
    count: transfers.length,
    getScrollElement: () => scrollContainerRef.current,
    estimateSize: () => 76,
    overscan: 5,
    gap: 8,
    onChange: (instance) => {
      const lastItem = instance.range?.endIndex ?? 0
      if (
        lastItem >= transfers.length - 5 &&
        hasNextPage &&
        !isFetchingNextPage
      ) {
        fetchNextPage()
      }
    },
  })

  if (isError) {
    return (
      <div className="flex-1 min-h-0 overflow-y-auto">
        <StatusPlaceholder
          icon={TriangleAlert}
          variant="destructive"
          title="Failed to load transfers"
          description="Check that the API is reachable and try again."
        />
      </div>
    )
  }

  if (isPending) {
    return (
      <div className="flex-1 min-h-0 overflow-y-auto space-y-2">
        {SKELETON_KEYS.map((key) => (
          <TransferCardSkeleton key={key} />
        ))}
      </div>
    )
  }

  if (transfers.length === 0) {
    return (
      <div className="flex-1 min-h-0 overflow-y-auto">
        <StatusPlaceholder
          icon={Inbox}
          title="No transfers yet"
          description="Transfers created via lclstream_api will show up here."
        />
      </div>
    )
  }

  const virtualItems = virtualizer.getVirtualItems()

  return (
    <div
      ref={scrollContainerRef}
      className="flex-1 min-h-0 overflow-y-auto"
      aria-busy={isFetchingNextPage}
    >
      <div
        style={{
          height: `${virtualizer.getTotalSize()}px`,
          width: "100%",
          position: "relative",
        }}
      >
        {virtualItems.map((virtualRow) => {
          const transfer = transfers[virtualRow.index]
          if (!transfer) return null

          return (
            <div
              key={transfer.id}
              data-index={virtualRow.index}
              ref={virtualizer.measureElement}
              style={{
                position: "absolute",
                top: 0,
                left: 0,
                width: "100%",
                transform: `translateY(${virtualRow.start}px)`,
              }}
            >
              <TransferCard
                transfer={transfer}
                onClick={() => onSelect(transfer.id)}
              />
            </div>
          )
        })}
      </div>
      {isFetchingNextPage && (
        <div className="flex justify-center py-4">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </div>
      )}
    </div>
  )
}
