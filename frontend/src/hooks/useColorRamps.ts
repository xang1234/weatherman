import { useEffect, useState } from 'react'
import { COLOR_RAMPS, loadColorRamps } from '@/layers/color-ramps'

/**
 * Whether the server's colour ramps are in COLOR_RAMPS; the GL layers wait
 * for them. A failed fetch is retried with backoff (1 s, 2 s, … up to 30 s).
 */
export function useColorRamps(): boolean {
  const [ready, setReady] = useState(() => Object.keys(COLOR_RAMPS).length > 0)
  const [attempt, setAttempt] = useState(0)
  useEffect(() => {
    if (ready) return
    let live = true
    let retry: ReturnType<typeof setTimeout> | undefined
    loadColorRamps(import.meta.env.VITE_API_BASE_URL || '')
      .then(() => { if (live) setReady(true) })
      .catch((err) => {
        console.warn('Failed to fetch colour ramps:', err)
        if (live) retry = setTimeout(() => setAttempt((n) => n + 1), Math.min(30_000, 1000 * 2 ** attempt))
      })
    return () => {
      live = false
      clearTimeout(retry)
    }
  }, [ready, attempt])
  return ready
}
