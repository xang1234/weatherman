import { useEffect, useRef, useState } from 'react'
import type maplibregl from 'maplibre-gl'
import type { CoverageJSON } from '@/types/edr'

/**
 * Hover probe: a debounced, cache-backed EDR position lookup that follows
 * the cursor and resolves a CoverageJSON time-series for the hovered point.
 *
 * Cooperates with the click-based WeatherInspector (different UX beats):
 * hover = ambient exploration, click = drill-down with the full panel.
 * Suppresses while the map is dragging/zooming, while the voyage tool is
 * drawing, or while the click inspector is open — so the cursor pill
 * never stacks on top of other UI.
 */

const AIS_LAYERS = ['ais-vessel-arrows', 'ais-vessel-circles']

/** Round to 0.1° to maximize cache hits as the cursor sweeps. */
const COORD_QUANTUM = 0.1

/** Debounce mousemove to avoid flooding the backend during pan/sweep. */
const HOVER_DEBOUNCE_MS = 180

/** Bounded LRU for recent point responses (shared per hook instance). */
const PROBE_CACHE_SIZE = 128

export interface HoverProbePoint {
  lngLat: { lng: number; lat: number }
  screen: { x: number; y: number }
}

export interface HoverProbeState {
  point: HoverProbePoint | null
  data: CoverageJSON | null
  loading: boolean
}

export interface UseHoverProbeOptions {
  map: React.RefObject<maplibregl.Map | null>
  isLoaded: boolean
  model: string
  runId: string | null
  /** Hide the probe (e.g., during route drawing or when click panel is open). */
  disabled?: boolean
}

function quantize(v: number): number {
  return Math.round(v / COORD_QUANTUM) * COORD_QUANTUM
}

function cacheKey(lat: number, lng: number, runId: string): string {
  return `${runId}|${quantize(lat).toFixed(1)}|${quantize(lng).toFixed(1)}`
}

export function useHoverProbe({
  map,
  isLoaded,
  model,
  runId,
  disabled = false,
}: UseHoverProbeOptions): HoverProbeState {
  const apiBase = import.meta.env.VITE_API_BASE_URL || ''
  const [point, setPoint] = useState<HoverProbePoint | null>(null)
  const [data, setData] = useState<CoverageJSON | null>(null)
  const [loading, setLoading] = useState(false)

  const cacheRef = useRef<Map<string, CoverageJSON>>(new Map())
  const requestIdRef = useRef(0)
  const disabledRef = useRef(disabled)
  disabledRef.current = disabled
  const runIdRef = useRef(runId)
  runIdRef.current = runId
  const modelRef = useRef(model)
  modelRef.current = model

  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded) return
    const activeMap = m

    let debounceTimer: ReturnType<typeof setTimeout> | null = null
    let currentController: AbortController | null = null
    let isDragging = false

    function onDragStart() {
      isDragging = true
      if (debounceTimer) {
        clearTimeout(debounceTimer)
        debounceTimer = null
      }
      currentController?.abort()
      setLoading(false)
    }

    function onDragEnd() {
      isDragging = false
    }

    function clearProbe() {
      if (debounceTimer) {
        clearTimeout(debounceTimer)
        debounceTimer = null
      }
      currentController?.abort()
      setPoint(null)
      setData(null)
      setLoading(false)
    }

    function onMouseMove(e: maplibregl.MapMouseEvent) {
      if (disabledRef.current || isDragging) {
        clearProbe()
        return
      }

      // Don't probe when hovering a vessel — those have their own popup.
      const vesselLayers = AIS_LAYERS.filter((id) => activeMap.getLayer(id))
      if (vesselLayers.length > 0) {
        const vessels = activeMap.queryRenderedFeatures(e.point, {
          layers: vesselLayers,
        })
        if (vessels.length > 0) {
          clearProbe()
          return
        }
      }

      const currentRunId = runIdRef.current
      const currentModel = modelRef.current
      if (!currentRunId) return

      // Update cursor-following bubble immediately.
      const probePoint: HoverProbePoint = {
        lngLat: { lng: e.lngLat.lng, lat: e.lngLat.lat },
        screen: { x: e.point.x, y: e.point.y },
      }
      setPoint(probePoint)

      const key = cacheKey(e.lngLat.lat, e.lngLat.lng, currentRunId)
      const cached = cacheRef.current.get(key)
      if (cached) {
        // Refresh LRU ordering.
        cacheRef.current.delete(key)
        cacheRef.current.set(key, cached)
        setData(cached)
        setLoading(false)
        return
      }

      // Debounce and fetch on pause.
      if (debounceTimer) clearTimeout(debounceTimer)
      setLoading(true)

      debounceTimer = setTimeout(() => {
        debounceTimer = null
        currentController?.abort()
        const controller = new AbortController()
        currentController = controller
        const requestId = ++requestIdRef.current

        const qLat = quantize(e.lngLat.lat)
        const qLon = quantize(e.lngLat.lng)
        const params = new URLSearchParams({
          coords: `POINT(${qLon.toFixed(3)} ${qLat.toFixed(3)})`,
        })
        const url = `${apiBase}/v1/edr/collections/${currentModel}/instances/${currentRunId}/position?${params}`

        fetch(url, { signal: controller.signal })
          .then((res) => {
            if (!res.ok) throw new Error(`HTTP ${res.status}`)
            return res.json() as Promise<CoverageJSON>
          })
          .then((nextData) => {
            if (requestId !== requestIdRef.current) return
            // Evict oldest before inserting to stay under cap.
            if (cacheRef.current.size >= PROBE_CACHE_SIZE) {
              const oldestKey = cacheRef.current.keys().next().value
              if (oldestKey !== undefined) cacheRef.current.delete(oldestKey)
            }
            cacheRef.current.set(key, nextData)
            setData(nextData)
            setLoading(false)
          })
          .catch((err) => {
            if (err instanceof DOMException && err.name === 'AbortError') return
            if (requestId !== requestIdRef.current) return
            setData(null)
            setLoading(false)
          })
      }, HOVER_DEBOUNCE_MS)
    }

    activeMap.on('mousemove', onMouseMove)
    activeMap.on('mouseout', clearProbe)
    activeMap.on('dragstart', onDragStart)
    activeMap.on('dragend', onDragEnd)
    activeMap.on('zoomstart', onDragStart)
    activeMap.on('zoomend', onDragEnd)

    return () => {
      activeMap.off('mousemove', onMouseMove)
      activeMap.off('mouseout', clearProbe)
      activeMap.off('dragstart', onDragStart)
      activeMap.off('dragend', onDragEnd)
      activeMap.off('zoomstart', onDragStart)
      activeMap.off('zoomend', onDragEnd)
      if (debounceTimer) clearTimeout(debounceTimer)
      currentController?.abort()
    }
  }, [map, isLoaded, apiBase])

  // Clear the cache when run changes; previous responses are for a different run.
  useEffect(() => {
    cacheRef.current.clear()
    setData(null)
    setPoint(null)
    setLoading(false)
  }, [runId, model])

  return { point, data, loading }
}
