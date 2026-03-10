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
}: UseWeatherLayerOptions): void {
  const apiBase = import.meta.env.VITE_API_BASE_URL || ''
  const layerRef = useRef<WeatherGLLayer | null>(null)

  // Create and add the WebGL custom layer when the map is ready.
  // Only depends on map/isLoaded/apiBase — dataset config is updated via methods.
  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded) return

    const glLayer = new WeatherGLLayer({
      id: 'weather-gl',
      apiBase,
    })
    layerRef.current = glLayer

    const beforeId = weatherInsertBeforeId(m)
    m.addLayer(glLayer as maplibregl.CustomLayerInterface, beforeId)

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
  useEffect(() => {
    const glLayer = layerRef.current
    if (!glLayer || !runId || !layer) return
    glLayer.setConfig(model, runId, layer, forecastHour)
  }, [model, runId, layer, forecastHour])

  // Update opacity and basemap transparency when visibility/opacity change.
  useEffect(() => {
    const m = map.current
    const glLayer = layerRef.current
    if (!m || !isLoaded || !glLayer) return

    glLayer.setOpacity(visible ? opacity : 0)
    setWeatherOverlayOpacity(m, visible)
  }, [map, isLoaded, opacity, visible])
}
