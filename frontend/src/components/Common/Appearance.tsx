import { Monitor, Moon, Sun } from "lucide-react"
import { useTheme } from "next-themes"

import { Button } from "@/components/ui/button"

const ORDER = ["light", "dark", "system"] as const

const ICONS = {
  light: Sun,
  dark: Moon,
  system: Monitor,
} as const

/** Cycles light -> dark -> system on click. */
export function ThemeToggle() {
  const { theme, setTheme } = useTheme()
  const current = (theme as (typeof ORDER)[number]) ?? "system"
  const Icon = ICONS[current]

  return (
    <Button
      variant="ghost"
      size="icon"
      aria-label="Toggle theme"
      onClick={() => {
        const next = ORDER[(ORDER.indexOf(current) + 1) % ORDER.length]
        setTheme(next)
      }}
    >
      <Icon />
    </Button>
  )
}
