import { useQuery } from "@tanstack/react-query"
import { formatDistanceToNow } from "date-fns"
import { Inbox, TriangleAlert } from "lucide-react"
import { getTransfersTransfersGetOptions } from "@/client/@tanstack/react-query.gen"
import { StatusPlaceholder } from "@/components/Common/StatusPlaceholder"
import { StateBadge } from "@/components/Transfers/StateBadge"

export function TransfersList({
  onSelect,
}: {
  onSelect: (id: string) => void
}) {
  const { data, isPending, isError } = useQuery({
    ...getTransfersTransfersGetOptions({ query: { limit: 100 } }),
    refetchInterval: 5_000,
    refetchIntervalInBackground: false,
  })

  if (isError) {
    return (
      <StatusPlaceholder
        icon={TriangleAlert}
        variant="destructive"
        title="Failed to load transfers"
        description="Check that the API is reachable and try again."
      />
    )
  }

  if (!isPending && data.data.length === 0) {
    return (
      <StatusPlaceholder
        icon={Inbox}
        title="No transfers yet"
        description="Transfers created via lclstream_api will show up here."
      />
    )
  }

  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b text-left text-muted-foreground">
          <th className="py-2 pr-4 font-medium">ID</th>
          <th className="py-2 pr-4 font-medium">State</th>
          <th className="py-2 pr-4 font-medium">Requested by</th>
          <th className="py-2 pr-4 font-medium">Created</th>
        </tr>
      </thead>
      <tbody>
        {isPending
          ? ["a", "b", "c", "d", "e"].map((key) => (
              <tr key={`skeleton-${key}`} className="border-b last:border-0">
                <td className="py-3 pr-4">
                  <div className="h-4 w-24 animate-pulse rounded bg-muted" />
                </td>
                <td className="py-3 pr-4">
                  <div className="h-4 w-16 animate-pulse rounded bg-muted" />
                </td>
                <td className="py-3 pr-4">
                  <div className="h-4 w-32 animate-pulse rounded bg-muted" />
                </td>
                <td className="py-3 pr-4">
                  <div className="h-4 w-20 animate-pulse rounded bg-muted" />
                </td>
              </tr>
            ))
          : data.data.map((transfer) => (
              <tr
                key={transfer.id}
                onClick={() => onSelect(transfer.id)}
                className="cursor-pointer border-b last:border-0 hover:bg-accent/50"
              >
                <td className="py-3 pr-4 font-mono text-xs">
                  {transfer.id.slice(0, 8)}
                </td>
                <td className="py-3 pr-4">
                  <StateBadge state={transfer.state} />
                </td>
                <td className="py-3 pr-4">{transfer.requested_by}</td>
                <td className="py-3 pr-4 text-muted-foreground">
                  {formatDistanceToNow(new Date(transfer.created_at), {
                    addSuffix: true,
                  })}
                </td>
              </tr>
            ))}
      </tbody>
    </table>
  )
}
