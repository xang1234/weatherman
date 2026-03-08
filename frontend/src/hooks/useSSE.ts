import { useEffect, useRef, useState } from 'react'

export interface SSEState {
  /**
   * Monotonically increasing counter, bumped on each `run.published` event.
   * Pass as a dependency to data-fetching hooks to trigger an immediate refetch.
   */
  weatherVersion: number
  /**
   * Latest AIS snapshot date from `ais.refreshed` event (YYYY-MM-DD),
   * or null if no event has been received yet.
   */
  aisDate: string | null
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
 * - `aisDate` updates on `ais.refreshed` → swaps AIS tile source
 */
export function useSSE(): SSEState {
  const apiBase = import.meta.env.VITE_API_BASE_URL || ''
  const [weatherVersion, setWeatherVersion] = useState(0)
  const [aisDate, setAisDate] = useState<string | null>(null)
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
        setAisDate(payload.ais_date)
        console.info('[SSE] ais.refreshed:', payload.ais_date)
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

  return { weatherVersion, aisDate, connected }
}
