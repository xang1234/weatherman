import { useEffect, useState } from 'react'
import { COLOR_RAMPS, loadColorRamps } from '@/layers/color-ramps'

/** Whether the server's colour ramps are in COLOR_RAMPS; the GL layers wait for them. */
export function useColorRamps(): boolean {
  const [ready, setReady] = useState(() => Object.keys(COLOR_RAMPS).length > 0)
  useEffect(() => {
    if (ready) return
    let live = true
    loadColorRamps(import.meta.env.VITE_API_BASE_URL || '')
      .then(() => { if (live) setReady(true) })
      .catch((err) => console.warn('Failed to fetch colour ramps:', err))
    return () => { live = false }
  }, [ready])
  return ready
}
