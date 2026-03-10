import { useEffect, useRef, useCallback } from 'react'
import type maplibregl from 'maplibre-gl'
import { setWeatherOverlayOpacity } from '@/utils/basemap-style'
import { useWebGLWeatherLayer } from './useWebGLWeatherLayer'

/** Feature flag: use WebGL data-tile pipeline instead of raster TileJSON. */
const USE_WEBGL = import.meta.env.VITE_USE_WEBGL_WEATHER === 'true'

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
const TILE_LOAD_TIMEOUT_MS = 4000

interface BufferEntry { srcId: string; lyrId: string }
interface BufferState {
  front: BufferEntry | null
  back: BufferEntry | null
  generation: number
}

function sourceId(model: string, runId: string, layer: string, forecastHour: number): string {
  return `${SOURCE_PREFIX}-${model}-${runId}-${layer}-${forecastHour}`
}

function layerId(model: string, runId: string, layer: string, forecastHour: number): string {
  return `${LAYER_PREFIX}-${model}-${runId}-${layer}-${forecastHour}`
}

/**
 * Find the layer ID to insert weather *before* in the MapLibre stack.
 *
 * Returns the first fill layer (e.g. 'water') so weather renders below
 * the semi-transparent basemap fills — the Windy.com layering trick:
 *   [background] → [WEATHER] → [water@0.3] → [earth@0.3] → [lines] → [labels]
 *
 * Falls back to the first symbol layer if no fill layer exists (e.g.
 * raster-only basemaps), so weather still renders below labels.
 */
function weatherInsertBeforeId(map: maplibregl.Map): string | undefined {
  const layers = map.getStyle()?.layers
  if (!layers) return undefined
  let firstSymbol: string | undefined
  for (const l of layers) {
    if (l.type === 'fill') return l.id
    if (l.type === 'symbol' && !firstSymbol) firstSymbol = l.id
  }
  return firstSymbol
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

export function useWeatherLayer(options: UseWeatherLayerOptions): void {
  // Feature flag: WebGL data-tile pipeline vs raster TileJSON pipeline.
  // USE_WEBGL is a build-time constant (Vite replaces import.meta.env at compile time),
  // so the conditional hook call is safe — call order is stable across renders.
  if (USE_WEBGL) {
    // eslint-disable-next-line react-hooks/rules-of-hooks
    useWebGLWeatherLayer(options)
    return
  }
  // eslint-disable-next-line react-hooks/rules-of-hooks
  useRasterWeatherLayer(options)
}

/** Original raster TileJSON pipeline. */
function useRasterWeatherLayer({
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

  // Double-buffer state: front = visible layer, back = loading invisibly
  const bufferRef = useRef<BufferState>({ front: null, back: null, generation: 0 })

  // Keep current opacity/visible in a ref so swap callbacks always read latest
  // values without needing them in the main effect's dependency array.
  const opacityRef = useRef(opacity)
  const visibleRef = useRef(visible)
  opacityRef.current = opacity
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

  // Atomically swap back buffer to front
  const performSwap = useCallback((
    m: maplibregl.Map,
    state: BufferState,
    targetOpacity: number,
    isVisible: boolean,
  ) => {
    const oldFront = state.front
    if (state.back) {
      // Reveal the back buffer at the target opacity
      try {
        if (m.getLayer(state.back.lyrId)) {
          m.setPaintProperty(state.back.lyrId, 'raster-opacity', isVisible ? targetOpacity : 0)
        }
      } catch { /* map destroyed */ }
      state.front = state.back
      state.back = null
    }
    // Remove old front
    if (oldFront) {
      removePair(m, oldFront.srcId, oldFront.lyrId)
    }
    // Activate basemap transparency now that weather is visible
    if (state.front && isVisible) {
      setWeatherOverlayOpacity(m, true)
    }
  }, [removePair])

  // Add or swap the raster overlay when runId/layer/forecastHour changes
  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded || !runId || !layer) return

    const newSrcId = sourceId(model, runId, layer, forecastHour)
    const newLyrId = layerId(model, runId, layer, forecastHour)
    const state = bufferRef.current

    // Already showing this exact combination as front — clean up any
    // orphaned back buffer (e.g. A→B→A where B never finished loading)
    if (state.front?.srcId === newSrcId) {
      if (state.back) {
        removePair(m, state.back.srcId, state.back.lyrId)
        state.back = null
      }
      return
    }

    // Increment generation to cancel any pending back-buffer load
    const gen = ++state.generation

    // Cancel existing back buffer if any
    if (state.back) {
      removePair(m, state.back.srcId, state.back.lyrId)
      state.back = null
    }

    // Remove stale source/layer with the same ID (e.g. rapid A->B->A toggling)
    removePair(m, newSrcId, newLyrId)

    // Use TileJSON: one small fetch to Weatherman, then all tile requests
    // go directly to TiTiler (bypassing the Weatherman proxy).
    const tileJsonUrl = buildTileJsonUrl(apiBase, model, runId, layer, forecastHour)

    m.addSource(newSrcId, {
      type: 'raster',
      url: tileJsonUrl,
      tileSize,
    })

    // Insert weather below basemap fills (Windy.com layering) — invisible (back buffer)
    const beforeLayer = weatherInsertBeforeId(m)
    m.addLayer(
      {
        id: newLyrId,
        type: 'raster',
        source: newSrcId,
        paint: {
          'raster-opacity': 0,
          'raster-fade-duration': 0,
          'raster-resampling': 'nearest',
        },
      },
      beforeLayer,
    )

    state.back = { srcId: newSrcId, lyrId: newLyrId }

    // Wait for all visible tiles to load via sourcedata events.
    // Unlike 'idle', this won't fire prematurely before tile fetches begin.
    const onSourceData = (e: maplibregl.MapSourceDataEvent) => {
      if (e.sourceId !== newSrcId) return
      if (!m.isSourceLoaded(newSrcId)) return
      // All visible tiles for this source are loaded — safe to swap
      m.off('sourcedata', onSourceData)
      clearTimeout(timeout)
      if (gen !== bufferRef.current.generation) return
      performSwap(m, bufferRef.current, opacityRef.current, visibleRef.current)
    }

    // Fallback: force swap after timeout (e.g. tile server errors)
    const timeout = setTimeout(() => {
      m.off('sourcedata', onSourceData)
      if (gen !== bufferRef.current.generation) return
      performSwap(m, bufferRef.current, opacityRef.current, visibleRef.current)
    }, TILE_LOAD_TIMEOUT_MS)

    m.on('sourcedata', onSourceData)

    return () => {
      m.off('sourcedata', onSourceData)
      clearTimeout(timeout)
    }
  }, [map, isLoaded, runId, layer, forecastHour, model, apiBase, tileSize, removePair, performSwap])

  // Update opacity on the visible (front) layer when it changes.
  // Also toggle basemap fill transparency so fills are semi-transparent
  // when weather is showing (Windy.com look) and opaque when hidden.
  // Only activates transparency when the front buffer exists — avoids
  // a washed-out basemap while weather tiles are still loading.
  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded) return
    const front = bufferRef.current.front
    const weatherShowing = visible && front != null
    setWeatherOverlayOpacity(m, weatherShowing)
    if (front && m.getLayer(front.lyrId)) {
      m.setPaintProperty(front.lyrId, 'raster-opacity', visible ? opacity : 0)
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

  // Cleanup on unmount — remove both front and back buffers
  useEffect(() => {
    const activeMap = map.current
    return () => {
      const state = bufferRef.current
      if (activeMap) {
        if (state.front) removePair(activeMap, state.front.srcId, state.front.lyrId)
        if (state.back) removePair(activeMap, state.back.srcId, state.back.lyrId)
      }
      state.front = null
      state.back = null
    }
  }, [map, removePair])
}
