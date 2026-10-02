import { useEffect, useRef, useState } from 'react'
import type { AISSnapshot } from '@/types/ais'

export interface SSEState {
  /**
   * Monotonically increasing counter, bumped on each `run.published` event.
   * Pass as a dependency to data-fetching hooks to trigger an immediate refetch.
   */
  weatherVersion: number
  /**
   * Latest AIS snapshot from an `ais.refreshed` event, or null if none has
   * been received yet. A new object per event, so a rebuild of the same
   * date (new revision) re-renders too.
   */
  aisSnapshot: AISSnapshot | null
  /** Whether the EventSource is currently connected. */
  connected: boolean
}

interface RunPublishedPayload {
  model: string
  run_id: string
  published_at: string
  manifest_url: string
}

interface AISRefreshedPayload {
  ais_date: string
  tile_url_template: string
  revision?: number
}

/**
 * Subscribe to the server's SSE push channel.
 *
 * Uses the native EventSource API which handles automatic reconnection
 * with backoff on disconnect. The backend sends keepalive comments every
 * 15s to detect dead connections early.
 *
 * Returns reactive state that downstream hooks can depend on:
 * - `weatherVersion` bumps on `run.published` → triggers catalog refetch
 * - `aisSnapshot` updates on `ais.refreshed` → refreshes AIS tiles
 */
export function useSSE(): SSEState {
  const apiBase = import.meta.env.VITE_API_BASE_URL || ''
  const [weatherVersion, setWeatherVersion] = useState(0)
  const [aisSnapshot, setAisSnapshot] = useState<AISSnapshot | null>(null)
  const [connected, setConnected] = useState(false)
  const esRef = useRef<EventSource | null>(null)

  useEffect(() => {
    const url = `${apiBase}/events/stream`
    const es = new EventSource(url)
    esRef.current = es

    es.onopen = () => {
      setConnected(true)
    }

    es.onerror = () => {
      // EventSource auto-reconnects; just update connection status.
      // readyState 0 = CONNECTING (reconnecting), 2 = CLOSED (gave up)
      setConnected(false)
      if (es.readyState === EventSource.CLOSED) {
        console.warn(
          '[SSE] Connection closed permanently. If cross-origin, ensure the server sends Access-Control-Allow-Origin headers.',
        )
      }
    }

    es.addEventListener('run.published', (e: MessageEvent) => {
      try {
        const payload: RunPublishedPayload = JSON.parse(e.data)
        // Bump version to trigger downstream refetch (useDataAge)
        setWeatherVersion((v) => v + 1)
        console.info('[SSE] run.published:', payload.model, payload.run_id)
      } catch {
        console.warn('[SSE] Failed to parse run.published event')
      }
    })

    es.addEventListener('ais.refreshed', (e: MessageEvent) => {
      try {
        const payload: AISRefreshedPayload = JSON.parse(e.data)
        // An event without a revision (notify_ais.py, older producers) still
        // means "reload": give it its own, negative so it never matches a real
        // one — the server then serves current tiles with a short cache.
        setAisSnapshot({ date: payload.ais_date, revision: payload.revision ?? -Date.now() })
        console.info('[SSE] ais.refreshed:', payload.ais_date, payload.revision)
      } catch {
        console.warn('[SSE] Failed to parse ais.refreshed event')
      }
    })

    return () => {
      es.close()
      esRef.current = null
      setConnected(false)
    }
  }, [apiBase])

  return { weatherVersion, aisSnapshot, connected }
}
