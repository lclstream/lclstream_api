import type { TransferState } from "@/client"
import { Badge } from "@/components/ui/badge"

const VARIANT: Record<
  TransferState,
  "default" | "secondary" | "destructive" | "outline"
> = {
  provisioning: "secondary",
  ready: "default",
  canceling: "secondary",
  canceled: "outline",
  completed: "outline",
  failed: "destructive",
}

export function StateBadge({ state }: { state: TransferState }) {
  return <Badge variant={VARIANT[state]}>{state}</Badge>
}
