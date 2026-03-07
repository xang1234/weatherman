import { useEffect, useRef, useCallback } from 'react'
import type maplibregl from 'maplibre-gl'

export interface UseWeatherLayerOptions {
  /** Ref to the MapLibre map instance */
  map: React.RefObject<maplibregl.Map | null>
  /** Whether the map has finished loading */
  isLoaded: boolean
  /** Weather layer name (e.g. "temperature", "wind_speed") */
  layer: string
  /** Model name (e.g. "gfs") */
  model: string
  /** Current run ID from the catalog (e.g. "20260306T12Z"), or null if unknown */
  runId: string | null
  /** Forecast hour to display */
  forecastHour?: number
  /** Overlay opacity 0-1 (default 0.7) */
  opacity?: number
  /** Tile size in pixels (default 256) */
  tileSize?: number
  /** Whether the overlay is visible (default true) */
  visible?: boolean
}

const SOURCE_PREFIX = 'wx-raster'
const LAYER_PREFIX = 'wx-raster-layer'

function sourceId(model: string, runId: string, layer: string, forecastHour: number): string {
  return `${SOURCE_PREFIX}-${model}-${runId}-${layer}-${forecastHour}`
}

function layerId(model: string, runId: string, layer: string, forecastHour: number): string {
  return `${LAYER_PREFIX}-${model}-${runId}-${layer}-${forecastHour}`
}

/** Find the first symbol layer in the style, so we can insert raster below labels. */
function firstSymbolLayerId(map: maplibregl.Map): string | undefined {
  const layers = map.getStyle()?.layers
  if (!layers) return undefined
  for (const l of layers) {
    if (l.type === 'symbol') return l.id
  }
  return undefined
}

function buildTileUrl(
  apiBase: string,
  model: string,
  runId: string,
  layer: string,
  forecastHour: number,
): string {
  return (
    `${apiBase}/tiles/${model}/${runId}/${layer}/${forecastHour}` +
    '/{z}/{x}/{y}.png'
  )
}

export function useWeatherLayer({
  map,
  isLoaded,
  layer,
  model,
  runId,
  forecastHour = 0,
  opacity = 0.7,
  tileSize = 256,
  visible = true,
}: UseWeatherLayerOptions): void {
  const apiBase = import.meta.env.VITE_API_BASE_URL || ''

  // Track the currently active source/layer so we can remove it on swap
  const activeRef = useRef<{ srcId: string; lyrId: string } | null>(null)

  // Refs for values used in the main effect's paint but that shouldn't trigger re-add
  const opacityRef = useRef(opacity)
  opacityRef.current = opacity
  const visibleRef = useRef(visible)
  visibleRef.current = visible

  // Remove a source+layer pair from the map if they exist
  const removePair = useCallback((m: maplibregl.Map, srcId: string, lyrId: string) => {
    try {
      if (m.getLayer(lyrId)) m.removeLayer(lyrId)
      if (m.getSource(srcId)) m.removeSource(srcId)
    } catch {
      // Map may have been destroyed between scheduling and execution
    }
  }, [])

  // Add or swap the raster overlay when runId/layer/forecastHour changes
  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded || !runId || !layer) return

    const srcId = sourceId(model, runId, layer, forecastHour)
    const lyrId = layerId(model, runId, layer, forecastHour)

    // Already showing this exact combination — nothing to do
    if (activeRef.current?.srcId === srcId) return

    const prev = activeRef.current
    const tileUrl = buildTileUrl(apiBase, model, runId, layer, forecastHour)

    // Add new source
    m.addSource(srcId, {
      type: 'raster',
      tiles: [tileUrl],
      tileSize,
      minzoom: 0,
      maxzoom: 8,
    })

    // Insert raster layer below first symbol layer so labels stay on top
    const beforeLayer = firstSymbolLayerId(m)
    m.addLayer(
      {
        id: lyrId,
        type: 'raster',
        source: srcId,
        paint: {
          'raster-opacity': visibleRef.current ? opacityRef.current : 0,
          'raster-fade-duration': 300,
        },
      },
      beforeLayer,
    )

    // Record the new active pair
    activeRef.current = { srcId, lyrId }

    // Remove previous source/layer after a short delay to allow new tiles to render,
    // preventing a flash of missing data during run switches.
    if (prev) {
      const timer = setTimeout(() => {
        removePair(m, prev.srcId, prev.lyrId)
      }, 500)
      return () => clearTimeout(timer)
    }
  }, [map, isLoaded, runId, layer, forecastHour, model, apiBase, tileSize, removePair])

  // Update opacity on existing layer when it changes
  useEffect(() => {
    const m = map.current
    const active = activeRef.current
    if (!m || !isLoaded || !active) return
    if (m.getLayer(active.lyrId)) {
      m.setPaintProperty(active.lyrId, 'raster-opacity', visible ? opacity : 0)
    }
  }, [map, isLoaded, opacity, visible])

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      const m = map.current
      const active = activeRef.current
      if (m && active) {
        removePair(m, active.srcId, active.lyrId)
        activeRef.current = null
      }
    }
  }, [map, removePair])
}
