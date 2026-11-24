import type { LucideIcon } from "lucide-react"

/** Centered icon + title + description block for empty, error, and forbidden states. */
export function StatusPlaceholder({
  icon: Icon,
  variant = "muted",
  title,
  description,
}: {
  icon: LucideIcon
  variant?: "destructive" | "muted"
  title: string
  description: string
}) {
  const bg = variant === "destructive" ? "bg-destructive/10" : "bg-muted"
  const iconColor =
    variant === "destructive" ? "text-destructive" : "text-muted-foreground"

  return (
    <div className="flex flex-col items-center justify-center text-center py-12">
      <div className={`rounded-full ${bg} p-4 mb-4`}>
        <Icon className={`h-8 w-8 ${iconColor}`} />
      </div>
      <h3 className="text-lg font-semibold">{title}</h3>
      <p className="text-muted-foreground">{description}</p>
    </div>
  )
}
