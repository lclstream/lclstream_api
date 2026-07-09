import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { format, formatDistanceToNow } from "date-fns"
import { ArrowLeft, Ban, TriangleAlert } from "lucide-react"
import { toast } from "sonner"

import {
  cancelTransferTransfersTransferIdDeleteMutation,
  getTransfersTransfersGetQueryKey,
  getTransferTransfersTransferIdGetOptions,
} from "@/client/@tanstack/react-query.gen"
import { StatusPlaceholder } from "@/components/Common/StatusPlaceholder"
import { LogViewer } from "@/components/Transfers/LogViewer"
import { StateBadge } from "@/components/Transfers/StateBadge"
import { Button } from "@/components/ui/button"

export function TransferDetail({
  transferId,
  onBack,
}: {
  transferId: string
  onBack: () => void
}) {
  const queryClient = useQueryClient()

  const {
    data: transfer,
    isPending,
    isError,
  } = useQuery({
    ...getTransferTransfersTransferIdGetOptions({
      path: { transfer_id: transferId },
    }),
    refetchInterval: 5_000,
    refetchIntervalInBackground: false,
  })

  const cancel = useMutation({
    ...cancelTransferTransfersTransferIdDeleteMutation(),
    onSuccess: () => {
      toast.success("Cancellation requested")
      queryClient.invalidateQueries({
        queryKey: getTransferTransfersTransferIdGetOptions({
          path: { transfer_id: transferId },
        }).queryKey,
      })
      queryClient.invalidateQueries({
        queryKey: getTransfersTransfersGetQueryKey(),
      })
    },
    onError: () => toast.error("Failed to cancel transfer"),
  })

  return (
    <div className="flex flex-col gap-4">
      <Button variant="ghost" size="sm" onClick={onBack} className="w-fit">
        <ArrowLeft />
        Back to transfers
      </Button>

      {isError && (
        <StatusPlaceholder
          icon={TriangleAlert}
          variant="destructive"
          title="Failed to load transfer"
          description="It may have been removed, or the API is unreachable."
        />
      )}

      {!isError && !isPending && transfer && (
        <>
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div>
              <h2 className="font-mono text-sm text-muted-foreground">
                {transfer.id}
              </h2>
              <div className="mt-1 flex items-center gap-2">
                <StateBadge state={transfer.state} />
                <span className="text-sm text-muted-foreground">
                  requested by {transfer.requested_by}
                </span>
              </div>
            </div>
            {(transfer.state === "provisioning" ||
              transfer.state === "ready") && (
              <Button
                variant="destructive"
                size="sm"
                disabled={cancel.isPending}
                onClick={() =>
                  cancel.mutate({ path: { transfer_id: transferId } })
                }
              >
                <Ban />
                Cancel
              </Button>
            )}
          </div>

          {transfer.connection_info && (
            <div className="rounded-md border p-3 text-sm">
              <div className="text-muted-foreground">Consumer endpoint</div>
              <code className="text-xs">{transfer.connection_info.uri}</code>
            </div>
          )}

          <div>
            <h3 className="mb-2 text-sm font-medium">History</h3>
            <ul className="flex flex-col gap-1 text-sm">
              {transfer.transitions?.map((t) => (
                <li
                  key={`${t.created_at}-${t.state}-${t.source ?? ""}`}
                  className="flex items-center gap-2 border-b py-1.5 last:border-0"
                >
                  <StateBadge state={t.state} />
                  {t.source && (
                    <span className="text-xs text-muted-foreground">
                      via {t.source}
                    </span>
                  )}
                  {t.info && (
                    <span className="text-xs text-muted-foreground">
                      — {t.info}
                    </span>
                  )}
                  <span className="ml-auto text-xs text-muted-foreground">
                    {format(new Date(t.created_at), "PPpp")} (
                    {formatDistanceToNow(new Date(t.created_at), {
                      addSuffix: true,
                    })}
                    )
                  </span>
                </li>
              ))}
            </ul>
          </div>

          <div>
            <h3 className="mb-2 text-sm font-medium">Logs</h3>
            <LogViewer transferId={transferId} />
          </div>
        </>
      )}
    </div>
  )
}
