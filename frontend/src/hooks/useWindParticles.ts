/**
 * React hook for the GPU wind-particle animation layer.
 *
 * Creates a WindParticleLayer (MapLibre custom layer) that renders
 * GPU-advected particles using wind U/V data tiles. Particle count
 * is auto-adapted to the device GPU (16K-262K particles).
 * Particles are advected by the actual wind field and leave fading trails.
 *
 * Only active when the current weather layer is wind_speed.
 */

import { useEffect, useRef } from 'react'
import type maplibregl from 'maplibre-gl'
import { WindParticleLayer } from '@/layers/WindParticleLayer'
import { COLOR_RAMPS } from '@/layers/color-ramps'

export interface UseWindParticlesOptions {
  map: React.RefObject<maplibregl.Map | null>
  isLoaded: boolean
  /** Current weather layer ID (particles only active for 'wind_speed'). */
  layer: string
  model: string
  runId: string | null
  forecastHour: number
  /** Whether the weather overlay is visible. */
  visible?: boolean
  /** When true, skip the config effect to avoid nuking tile caches during playback. */
  isPlaying?: boolean
}

export interface WindParticleHandle {
  setTemporalBlend?(forecastHourT1: number, mix: number): void
  advanceForecastHour?(newHour: number): void
  isT1Ready?(): boolean
}

export function useWindParticles({
  map,
  isLoaded,
  layer,
  model,
  runId,
  forecastHour,
  visible = true,
  isPlaying = false,
}: UseWindParticlesOptions): WindParticleHandle {
  const apiBase = import.meta.env.VITE_API_BASE_URL || ''
  const layerRef = useRef<WindParticleLayer | null>(null)
  const isWindLayer = layer === 'wind_speed'
  const isActive = isWindLayer && visible

  // Create the particle layer once when the map is ready.
  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded) return

    const tileFormat = import.meta.env.VITE_USE_FLOAT16_TILES === 'true' ? 'f16' as const : 'png' as const
    const particleLayer = new WindParticleLayer({
      id: 'wind-particles',
      opacity: 0.6,
      apiBase,
      tileFormat,
    })
    layerRef.current = particleLayer

    // Add particles above the weather overlay
    m.addLayer(particleLayer as maplibregl.CustomLayerInterface)

    return () => {
      layerRef.current = null
      try {
        if (m.getLayer(particleLayer.id)) {
          m.removeLayer(particleLayer.id)
        }
      } catch { /* map may be destroyed */ }
    }
  }, [map, isLoaded, apiBase])

  useEffect(() => {
    const pl = layerRef.current
    if (!pl) return
    pl.setActive(isActive)
    pl.setOpacity(isActive ? 0.6 : 0)
  }, [isActive, isLoaded])

  // Update wind config when dataset changes or layer is (re)created.
  // During playback, skip — the RAF loop drives temporal blending imperatively
  // and calling setWindConfig would nuke the tile cache via TileManager.setLayer().
  useEffect(() => {
    if (isPlaying) return
    const pl = layerRef.current
    if (!pl || !runId || !isWindLayer) return

    const ramp = COLOR_RAMPS['wind_speed']
    const max = ramp?.valueMax ?? 50
    pl.setWindConfig(model, runId, forecastHour, -max, max)
  }, [model, runId, forecastHour, isWindLayer, isLoaded, isPlaying])

  // Return imperative handle for playback integration
  const handle: WindParticleHandle = {
    setTemporalBlend(forecastHourT1: number, mix: number) {
      if (!isActive) return
      layerRef.current?.setTemporalBlend(forecastHourT1, mix)
    },
    advanceForecastHour(newHour: number) {
      if (!isActive) return
      layerRef.current?.advanceForecastHour(newHour)
    },
    isT1Ready() {
      if (!isActive) return true
      return layerRef.current?.isT1Ready() ?? true
    },
  }

  return handle
}
