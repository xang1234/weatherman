import { useCallback, useEffect, useRef, useState } from 'react'
import type {
  DataAgeState,
  DataAgeThresholds,
  FreshnessStatus,
} from '@/types/data-age'
import { DEFAULT_THRESHOLDS } from '@/types/data-age'

interface CatalogResponse {
  model: string
  current_run_id: string
  runs: Array<{
    run_id: string
    status: string
    published_at: string
  }>
}

function getFreshnessStatus(
  ageMinutes: number,
  thresholds: DataAgeThresholds,
): FreshnessStatus {
  if (ageMinutes < thresholds.amberMinutes) return 'fresh'
  if (ageMinutes < thresholds.redMinutes) return 'aging'
  return 'stale'
}

function minutesSince(date: Date): number {
  return Math.floor((Date.now() - date.getTime()) / 60_000)
}

export interface UseDataAgeOptions {
  /** Model to track (e.g. "gfs") */
  model: string
  /** Polling interval in ms for fetching catalog (default: 5 minutes) */
  pollIntervalMs?: number
  /** Color threshold overrides */
  thresholds?: DataAgeThresholds
  /**
   * Monotonically increasing version counter from SSE.
   * When this changes, the hook immediately re-fetches the catalog
   * instead of waiting for the next poll interval.
   */
  version?: number
}

export function useDataAge({
  model,
  pollIntervalMs = 300_000,
  thresholds = DEFAULT_THRESHOLDS,
  version = 0,
}: UseDataAgeOptions): DataAgeState | null {
  const [state, setState] = useState<DataAgeState | null>(null)
  const publishedAtRef = useRef<Date | null>(null)
  const runIdRef = useRef<string | null>(null)
  const thresholdsRef = useRef(thresholds)
  thresholdsRef.current = thresholds

  const apiBase = import.meta.env.VITE_API_BASE_URL || ''

  // Reset state when model changes
  useEffect(() => {
    setState(null)
    publishedAtRef.current = null
    runIdRef.current = null
  }, [model])

  // Stable fetch function — uses its own AbortController per call so
  // concurrent fetches (poll + SSE nudge) don't cancel each other.
  const fetchCatalog = useCallback(async (signal?: AbortSignal) => {
    try {
      const res = await fetch(`${apiBase}/api/catalog/${model}`, { signal })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)

      const data: CatalogResponse = await res.json()
      const currentRun = data.runs.find(
        (r) => r.run_id === data.current_run_id && r.status === 'published',
      )
      if (!currentRun) throw new Error('No published run found')

      const publishedAt = new Date(currentRun.published_at)
      if (isNaN(publishedAt.getTime())) {
        throw new Error('Invalid published_at timestamp')
      }

      publishedAtRef.current = publishedAt
      runIdRef.current = currentRun.run_id

      const t = thresholdsRef.current
      const ageMinutes = minutesSince(publishedAt)
      setState({
        model,
        runId: currentRun.run_id,
        publishedAt,
        ageMinutes,
        status: getFreshnessStatus(ageMinutes, t),
        isOffline: false,
      })
    } catch (err) {
      if (err instanceof DOMException && err.name === 'AbortError') return

      const t = thresholdsRef.current
      setState((prev) => {
        if (prev) {
          const ageMinutes = minutesSince(prev.publishedAt)
          return {
            ...prev,
            ageMinutes,
            status: getFreshnessStatus(ageMinutes, t),
            isOffline: true,
            error: err instanceof Error ? err.message : 'Unknown error',
          }
        }
        return null
      })
    }
  }, [apiBase, model])

  // Fetch on mount + poll interval (stable — not affected by version bumps)
  useEffect(() => {
    const controller = new AbortController()
    fetchCatalog(controller.signal)
    const id = setInterval(() => fetchCatalog(controller.signal), pollIntervalMs)
    return () => {
      controller.abort()
      clearInterval(id)
    }
  }, [fetchCatalog, pollIntervalMs])

  // SSE-triggered immediate refetch — does not disturb the poll timer
  const prevVersionRef = useRef(version)
  useEffect(() => {
    if (version === prevVersionRef.current) return
    prevVersionRef.current = version
    const controller = new AbortController()
    fetchCatalog(controller.signal)
    return () => controller.abort()
  }, [version, fetchCatalog])

  // Update relative time every 60s without re-fetching
  useEffect(() => {
    const id = setInterval(() => {
      const pub = publishedAtRef.current
      const runId = runIdRef.current
      if (!pub || !runId) return

      const t = thresholdsRef.current
      setState((prev) => {
        if (!prev) return null
        const ageMinutes = minutesSince(pub)
        return {
          ...prev,
          ageMinutes,
          status: getFreshnessStatus(ageMinutes, t),
        }
      })
    }, 60_000)
    return () => clearInterval(id)
  }, [])

  return state
}
