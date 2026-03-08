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
  /** Adjacent forecast hours to prefetch for smoother scrubbing/playback */
  prefetchForecastHours?: number[]
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

/**
 * Build the TileJSON URL for a given model/run/layer/forecastHour.
 *
 * Weatherman returns a TileJSON document whose tile URL templates point
 * directly to TiTiler — so MapLibre fetches tiles from TiTiler without
 * an extra Weatherman proxy hop.
 */
function buildTileJsonUrl(
  apiBase: string,
  model: string,
  runId: string,
  layer: string,
  forecastHour: number,
): string {
  const params = new URLSearchParams({ layer, forecast_hour: String(forecastHour) })
  return `${apiBase}/tiles/${model}/${runId}/tilejson.json?${params}`
}

function wrapLon(lng: number): number {
  return ((lng + 180) % 360 + 360) % 360 - 180
}

function lngLatToTile(lng: number, lat: number, z: number): { x: number; y: number } {
  const n = 2 ** z
  const wrappedLng = wrapLon(lng)
  const x = Math.floor(((wrappedLng + 180) / 360) * n)
  const latRad = lat * Math.PI / 180
  const merc = Math.log(Math.tan(Math.PI / 4 + latRad / 2))
  const y = Math.floor((1 - merc / Math.PI) / 2 * n)
  return {
    x: Math.max(0, Math.min(n - 1, x)),
    y: Math.max(0, Math.min(n - 1, y)),
  }
}

function visibleTiles(map: maplibregl.Map, z: number): Array<{ x: number; y: number }> {
  const bounds = map.getBounds()
  const northWest = lngLatToTile(bounds.getWest(), bounds.getNorth(), z)
  const southEast = lngLatToTile(bounds.getEast(), bounds.getSouth(), z)
  const n = 2 ** z
  const xs: number[] = []

  if (northWest.x <= southEast.x) {
    for (let x = northWest.x; x <= southEast.x; x += 1) xs.push(x)
  } else {
    for (let x = northWest.x; x < n; x += 1) xs.push(x)
    for (let x = 0; x <= southEast.x; x += 1) xs.push(x)
  }

  const tiles: Array<{ x: number; y: number }> = []
  const yStart = Math.min(northWest.y, southEast.y)
  const yEnd = Math.max(northWest.y, southEast.y)
  for (const x of xs) {
    for (let y = yStart; y <= yEnd; y += 1) {
      tiles.push({ x, y })
    }
  }
  return tiles
}

function resolveUrl(template: string): string {
  if (/^https?:\/\//.test(template)) return template
  return new URL(template, window.location.origin).toString()
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
  prefetchForecastHours = [],
}: UseWeatherLayerOptions): void {
  const apiBase = import.meta.env.VITE_API_BASE_URL || ''

  // Track the currently active source/layer so we can remove it on swap
  const activeRef = useRef<{ srcId: string; lyrId: string } | null>(null)

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

    // Remove stale source/layer with the same ID that might exist from a
    // canceled delayed cleanup (e.g. rapid A->B->A toggling)
    removePair(m, srcId, lyrId)

    // Use TileJSON: one small fetch to Weatherman, then all tile requests
    // go directly to TiTiler (bypassing the Weatherman proxy).
    const tileJsonUrl = buildTileJsonUrl(apiBase, model, runId, layer, forecastHour)

    m.addSource(srcId, {
      type: 'raster',
      url: tileJsonUrl,
      tileSize,
    })

    // Insert raster layer below first symbol layer so labels stay on top
    const beforeLayer = firstSymbolLayerId(m)
    m.addLayer(
      {
        id: lyrId,
        type: 'raster',
        source: srcId,
        paint: {
          'raster-opacity': visible ? opacity : 0,
          'raster-fade-duration': 300,
        },
      },
      beforeLayer,
    )

    // Record the new active pair
    activeRef.current = { srcId, lyrId }

    // Remove previous source/layer immediately — the raster-fade-duration
    // on the new layer handles visual transitions.
    if (prev) {
      removePair(m, prev.srcId, prev.lyrId)
    }
  }, [map, isLoaded, runId, layer, forecastHour, model, apiBase, tileSize, removePair, opacity, visible])

  // Update opacity on existing layer when it changes
  useEffect(() => {
    const m = map.current
    const active = activeRef.current
    if (!m || !isLoaded || !active) return
    if (m.getLayer(active.lyrId)) {
      m.setPaintProperty(active.lyrId, 'raster-opacity', visible ? opacity : 0)
    }
  }, [map, isLoaded, opacity, visible])

  // Prefetch adjacent hours for the current viewport to smooth playback.
  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded || !runId || !layer || prefetchForecastHours.length === 0) return
    const activeMap = m
    const activeRunId = runId
    const activeLayer = layer

    const controller = new AbortController()

    async function prefetchAdjacentHours() {
      const z = Math.max(0, Math.min(8, Math.floor(activeMap.getZoom())))
      const tiles = visibleTiles(activeMap, z)
      if (tiles.length === 0) return

      await Promise.all(prefetchForecastHours.map(async (hour) => {
        try {
          const tileJsonRes = await fetch(
            buildTileJsonUrl(apiBase, model, activeRunId, activeLayer, hour),
            { signal: controller.signal },
          )
          if (!tileJsonRes.ok) return
          const tileJson: { tiles?: string[] } = await tileJsonRes.json()
          const template = tileJson.tiles?.[0]
          if (!template) return
          const absoluteTemplate = resolveUrl(template)
          await Promise.all(tiles.map(async ({ x, y }) => {
            const url = absoluteTemplate
              .replace('{z}', String(z))
              .replace('{x}', String(x))
              .replace('{y}', String(y))
            await fetch(url, { signal: controller.signal }).catch(() => undefined)
          }))
        } catch (err) {
          if (err instanceof DOMException && err.name === 'AbortError') return
        }
      }))
    }

    prefetchAdjacentHours()
    return () => controller.abort()
  }, [map, isLoaded, runId, layer, model, apiBase, prefetchForecastHours])

  // Cleanup on unmount
  useEffect(() => {
    const activeMap = map.current
    return () => {
      const active = activeRef.current
      if (activeMap && active) {
        removePair(activeMap, active.srcId, active.lyrId)
        activeRef.current = null
      }
    }
  }, [map, removePair])
}
