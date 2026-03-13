/**
 * React hook for the WebGL data-tile weather rendering pipeline.
 *
 * Alternative to the raster TileJSON pipeline in useWeatherLayer.
 * Creates a WeatherGLLayer (MapLibre custom layer) that owns a TileManager
 * for fetching data-encoded PNG tiles and rendering them via WebGL2.
 *
 * Activated by VITE_USE_WEBGL_WEATHER=true feature flag.
 */

import { useEffect, useRef } from 'react'
import type maplibregl from 'maplibre-gl'
import { setWeatherOverlayOpacity } from '@/utils/basemap-style'
import { WeatherGLLayer } from '@/layers/WeatherGLLayer'
import type { UseWeatherLayerOptions } from './useWeatherLayer'

/**
 * Find the layer ID to insert weather before in the MapLibre stack.
 * Same logic as the raster pipeline — inserts before the first fill layer
 * for the Windy.com "weather below semi-transparent basemap" look.
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

export function useWebGLWeatherLayer({
  map,
  isLoaded,
  layer,
  model,
  runId,
  forecastHour = 0,
  opacity = 0.7,
  visible = true,
  forecastHourNext,
  temporalMix = 0,
}: UseWeatherLayerOptions): React.RefObject<WeatherGLLayer | null> {
  const apiBase = import.meta.env.VITE_API_BASE_URL || ''
  const layerRef = useRef<WeatherGLLayer | null>(null)

  // Create and add the WebGL custom layer when the map is ready.
  // Only depends on map/isLoaded/apiBase — dataset config is updated via methods.
  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded) return

    const tileFormat = import.meta.env.VITE_USE_FLOAT16_TILES === 'true' ? 'f16' as const : 'png' as const
    const glLayer = new WeatherGLLayer({
      id: 'weather-gl',
      apiBase,
      tileFormat,
    })
    layerRef.current = glLayer

    const beforeId = weatherInsertBeforeId(m)
    m.addLayer(glLayer as maplibregl.CustomLayerInterface, beforeId)
    console.info(`[useWebGLWeatherLayer] Added weather-gl layer (before: ${beforeId ?? 'end'})`)

    return () => {
      layerRef.current = null
      try {
        if (m.getLayer(glLayer.id)) {
          m.removeLayer(glLayer.id)
        }
      } catch {
        // Map may have been destroyed
      }
      setWeatherOverlayOpacity(m, false)
    }
  }, [map, isLoaded, apiBase])

  // Update dataset config when model/run/layer/forecastHour change.
  // Calls setConfig which clears the tile cache if params changed.
  // NOTE: isLoaded is included in deps even though the effect doesn't read it
  // directly — without it, if API data (runId, layer) arrives before the map
  // loads, this effect fires while layerRef is still null, and never re-runs
  // when the creation effect finally sets it (since data deps are unchanged).
  useEffect(() => {
    const glLayer = layerRef.current
    if (!glLayer || !runId || !layer) return
    console.info(`[useWebGLWeatherLayer] setConfig: ${model}/${runId}/${layer}/fh${forecastHour}`)
    glLayer.setConfig(model, runId, layer, forecastHour)
  }, [model, runId, layer, forecastHour, isLoaded])

  // Update temporal blend when forecastHourNext/temporalMix change.
  useEffect(() => {
    const glLayer = layerRef.current
    if (!glLayer) return
    glLayer.setTemporalBlend(forecastHourNext ?? -1, temporalMix)
  }, [forecastHourNext, temporalMix, isLoaded])

  // Update opacity and basemap transparency when visibility/opacity change.
  useEffect(() => {
    const m = map.current
    const glLayer = layerRef.current
    if (!m || !isLoaded || !glLayer) return

    const effectiveOpacity = visible ? opacity : 0
    console.info(`[useWebGLWeatherLayer] setOpacity(${effectiveOpacity}), setWeatherOverlayOpacity(active=${visible}, layer=${layer})`)
    glLayer.setOpacity(effectiveOpacity)
    setWeatherOverlayOpacity(m, visible, layer)
  }, [map, isLoaded, opacity, visible, layer])

  return layerRef
}
