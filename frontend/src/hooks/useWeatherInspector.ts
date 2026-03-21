import { useEffect, useRef, useState } from 'react'
import type maplibregl from 'maplibre-gl'
import type { CoverageJSON } from '@/types/edr'

const AIS_LAYERS = ['ais-vessel-arrows', 'ais-vessel-circles']

export interface WeatherInspectorState {
  point: { lon: number; lat: number } | null
  data: CoverageJSON | null
  loading: boolean
  error: string | null
  selectedVariable: string | null
  setSelectedVariable: (variable: string) => void
  clear: () => void
}

export interface UseWeatherInspectorOptions {
  map: React.RefObject<maplibregl.Map | null>
  isLoaded: boolean
  model: string
  runId: string | null
  /** When true, click handling is suppressed (e.g., during route drawing). */
  disabled?: boolean
}

export function useWeatherInspector({
  map,
  isLoaded,
  model,
  runId,
  disabled = false,
}: UseWeatherInspectorOptions): WeatherInspectorState {
  const apiBase = import.meta.env.VITE_API_BASE_URL || ''
  const [point, setPoint] = useState<{ lon: number; lat: number } | null>(null)
  const [data, setData] = useState<CoverageJSON | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [selectedVariable, setSelectedVariable] = useState<string | null>(null)
  const requestIdRef = useRef(0)
  const disabledRef = useRef(disabled)
  disabledRef.current = disabled

  useEffect(() => {
    if (!point || !runId) return
    const selectedPoint = point
    const controller = new AbortController()
    const requestId = ++requestIdRef.current

    async function fetchPointForecast() {
      setLoading(true)
      setError(null)
      try {
        const params = new URLSearchParams({
          coords: `POINT(${selectedPoint.lon.toFixed(6)} ${selectedPoint.lat.toFixed(6)})`,
        })
        const res = await fetch(
          `${apiBase}/v1/edr/collections/${model}/instances/${runId}/position?${params}`,
          { signal: controller.signal },
        )
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const nextData: CoverageJSON = await res.json()
        if (requestId !== requestIdRef.current) return
        setData(nextData)
        setSelectedVariable((prev) => {
          if (prev && nextData.ranges[prev]) return prev
          return Object.keys(nextData.ranges)[0] ?? null
        })
      } catch (err) {
        if (err instanceof DOMException && err.name === 'AbortError') return
        if (requestId !== requestIdRef.current) return
        setData(null)
        setError(err instanceof Error ? err.message : 'Unknown error')
      } finally {
        if (requestId === requestIdRef.current) {
          setLoading(false)
        }
      }
    }

    fetchPointForecast()
    return () => controller.abort()
  }, [apiBase, model, point, runId])

  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded) return
    const activeMap = m

    function onClick(e: maplibregl.MapMouseEvent) {
      if (disabledRef.current) return
      const layers = AIS_LAYERS.filter((layerId) => activeMap.getLayer(layerId))
      const vesselFeatures = layers.length > 0
        ? activeMap.queryRenderedFeatures(e.point, { layers })
        : []
      if (vesselFeatures.length > 0) return

      setPoint({ lon: e.lngLat.lng, lat: e.lngLat.lat })
    }

    activeMap.on('click', onClick)
    return () => {
      activeMap.off('click', onClick)
    }
  }, [map, isLoaded])

  return {
    point,
    data,
    loading,
    error,
    selectedVariable,
    setSelectedVariable,
    clear: () => {
      setPoint(null)
      setData(null)
      setError(null)
      setLoading(false)
    },
  }
}
