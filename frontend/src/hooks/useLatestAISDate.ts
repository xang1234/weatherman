import { useEffect, useState } from 'react'

interface LatestAISResponse {
  snapshot_date: string
}

export function useLatestAISDate(): string | null {
  const apiBase = import.meta.env.VITE_API_BASE_URL || ''
  const [snapshotDate, setSnapshotDate] = useState<string | null>(null)

  useEffect(() => {
    const controller = new AbortController()

    async function fetchLatest() {
      try {
        const res = await fetch(`${apiBase}/ais/tiles/latest`, {
          signal: controller.signal,
        })
        if (res.status === 404) {
          setSnapshotDate(null)
          return
        }
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data: LatestAISResponse = await res.json()
        setSnapshotDate(data.snapshot_date)
      } catch (err) {
        if (err instanceof DOMException && err.name === 'AbortError') return
        console.warn('Failed to fetch latest AIS snapshot date:', err)
      }
    }

    fetchLatest()
    return () => controller.abort()
  }, [apiBase])

  return snapshotDate
}
