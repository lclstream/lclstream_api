import { useQuery } from "@tanstack/react-query"
import { useState } from "react"

import type { LogReadMode, LogStream } from "@/client"
import { getTransferLogTransfersTransferIdLogsStreamGetOptions } from "@/client/@tanstack/react-query.gen"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"

const STREAMS: { value: LogStream; label: string }[] = [
  { value: "cache", label: "Cache" },
  { value: "producer_stdout", label: "Producer stdout" },
  { value: "producer_stderr", label: "Producer stderr" },
]

export function LogViewer({ transferId }: { transferId: string }) {
  const [stream, setStream] = useState<LogStream>("cache")
  const [mode, setMode] = useState<LogReadMode>("tail")

  const { data, isPending, isError } = useQuery({
    ...getTransferLogTransfersTransferIdLogsStreamGetOptions({
      path: { transfer_id: transferId, stream },
      query: { mode, lines: 200 },
    }),
    refetchInterval: 3_000,
    refetchIntervalInBackground: false,
  })

  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex gap-1">
          {STREAMS.map((s) => (
            <Button
              key={s.value}
              size="sm"
              variant={stream === s.value ? "default" : "outline"}
              onClick={() => setStream(s.value)}
            >
              {s.label}
            </Button>
          ))}
        </div>
        <div className="flex gap-1">
          {(["tail", "head"] as const).map((m) => (
            <Button
              key={m}
              size="sm"
              variant={mode === m ? "default" : "outline"}
              onClick={() => setMode(m)}
            >
              {m}
            </Button>
          ))}
        </div>
      </div>

      <pre
        className={cn(
          "max-h-96 overflow-auto rounded-md border bg-muted/30 p-3 font-mono text-xs whitespace-pre-wrap",
          isError && "text-destructive",
        )}
      >
        {isError
          ? "Log not available (stream may not exist yet)."
          : isPending
            ? "Loading…"
            : data || "(empty)"}
      </pre>
    </div>
  )
}
