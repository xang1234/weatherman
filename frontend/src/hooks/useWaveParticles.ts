/**
 * React hook for the GPU wave animation layer.
 *
 * Creates a WaveParticleLayer (MapLibre custom layer) that renders a
 * stateless, world-anchored dash field from wave height, period, and
 * direction-vector data tiles. This avoids the density-clumping artifact
 * from the previous long-lived tracer model.
 *
 * Only active when the current weather layer is wave_height.
 */

import { useEffect, useRef } from 'react'
import type maplibregl from 'maplibre-gl'
import { WaveParticleLayer } from '@/layers/WaveParticleLayer'

export interface UseWaveParticlesOptions {
  map: React.RefObject<maplibregl.Map | null>
  isLoaded: boolean
  /** Current weather layer ID (particles only active for 'wave_height'). */
  layer: string
  model: string
  runId: string | null
  forecastHour: number
  /** Whether the weather overlay is visible. */
  visible?: boolean
  /** When true, skip the config effect to avoid nuking tile caches during playback. */
  isPlaying?: boolean
}

export interface WaveParticleHandle {
  setTemporalBlend?(forecastHourT1: number, mix: number): void
  advanceForecastHour?(newHour: number): void
  isT1Ready?(): boolean
}

export function useWaveParticles({
  map,
  isLoaded,
  layer,
  model,
  runId,
  forecastHour,
  visible = true,
  isPlaying = false,
}: UseWaveParticlesOptions): WaveParticleHandle {
  const apiBase = import.meta.env.VITE_API_BASE_URL || ''
  const layerRef = useRef<WaveParticleLayer | null>(null)
  const isWaveLayer = layer === 'wave_height'
  const isActive = isWaveLayer && visible

  // Create the particle layer once when the map is ready.
  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded) return

    const tileFormat = import.meta.env.VITE_USE_FLOAT16_TILES === 'true' ? 'f16' as const : 'png' as const
    const particleLayer = new WaveParticleLayer({
      id: 'wave-particles',
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
    pl.setOpacity(isActive ? 0.8 : 0)
  }, [isActive, isLoaded])

  // Update wave config when dataset changes or layer is (re)created.
  // During playback, skip — the RAF loop drives temporal blending imperatively
  // and calling setWaveConfig would nuke the tile cache via TileManager.setLayer().
  useEffect(() => {
    if (isPlaying) return
    const pl = layerRef.current
    if (!pl || !runId || !isWaveLayer) return

    pl.setWaveConfig(model, runId, forecastHour)
  }, [model, runId, forecastHour, isWaveLayer, isLoaded, isPlaying])

  // Return imperative handle for playback integration
  const handle: WaveParticleHandle = {
    setTemporalBlend(forecastHourT1: number, mix: number) {
      layerRef.current?.setTemporalBlend(forecastHourT1, mix)
    },
    advanceForecastHour(newHour: number) {
      layerRef.current?.advanceForecastHour(newHour)
    },
    isT1Ready() {
      return layerRef.current?.isT1Ready() ?? true
    },
  }

  return handle
}
