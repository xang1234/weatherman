import { useEffect, useState } from 'react'
import type { AISSnapshot } from '@/types/ais'

interface LatestAISResponse {
  snapshot_date: string
  revision?: number
}

/** The newest AIS snapshot on the server, fetched once at load. */
export function useLatestAISSnapshot(): AISSnapshot | null {
  const apiBase = import.meta.env.VITE_API_BASE_URL || ''
  const [snapshot, setSnapshot] = useState<AISSnapshot | null>(null)

  useEffect(() => {
    const controller = new AbortController()

    async function fetchLatest() {
      try {
        const res = await fetch(`${apiBase}/ais/tiles/latest`, {
          signal: controller.signal,
        })
        if (res.status === 404) {
          setSnapshot(null)
          return
        }
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data: LatestAISResponse = await res.json()
        setSnapshot({ date: data.snapshot_date, revision: data.revision ?? 0 })
      } catch (err) {
        if (err instanceof DOMException && err.name === 'AbortError') return
        console.warn('Failed to fetch latest AIS snapshot date:', err)
      }
    }

    fetchLatest()
    return () => controller.abort()
  }, [apiBase])

  return snapshot
}
