/**
 * React hook for the GPU wave animation layer.
 *
 * Creates a WaveParticleLayer (MapLibre custom layer) that renders a
 * stateless, world-anchored dash field from wave height, period, and
 * direction-vector data tiles. This avoids the density-clumping artifact
 * from the previous long-lived tracer model.
 *
 * An overlay: drawn over whichever colour layer is selected, while enabled.
 */

import { useEffect, useRef } from 'react'
import type maplibregl from 'maplibre-gl'
import { WaveParticleLayer } from '@/layers/WaveParticleLayer'
import { particleInsertBeforeId } from './useWebGLWeatherLayer'

export interface UseWaveParticlesOptions {
  map: React.RefObject<maplibregl.Map | null>
  isLoaded: boolean
  /** Whether the dashes are shown. */
  enabled: boolean
  /** Dash opacity 0-1. */
  opacity: number
  model: string
  runId: string | null
  forecastHour: number
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
  enabled,
  opacity,
  model,
  runId,
  forecastHour,
  isPlaying = false,
}: UseWaveParticlesOptions): WaveParticleHandle {
  const apiBase = import.meta.env.VITE_API_BASE_URL || ''
  const layerRef = useRef<WaveParticleLayer | null>(null)
  const configuredRunRef = useRef<string | null>(null)
  const isActive = enabled

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
    configuredRunRef.current = null

    // Vector basemap: directly above the weather overlay (added first, same
    // insertion point) and below the fills, lines, labels and AIS — the
    // opaque land mask of ocean-only layers then also hides particles over
    // land. Raster fallback: on top, as the raster would cover them.
    m.addLayer(particleLayer as maplibregl.CustomLayerInterface, particleInsertBeforeId(m))

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
    pl.setOpacity(isActive ? opacity : 0)
  }, [isActive, opacity, isLoaded])

  // Update wave config when dataset changes or layer is (re)created.
  // During playback the RAF loop drives the hour imperatively and calling
  // setWaveConfig each step would nuke the tile cache via TileManager.setLayer(),
  // so it only runs then if this run was never configured (enabled mid-play).
  useEffect(() => {
    const pl = layerRef.current
    if (!pl || !runId || !enabled) return
    if (isPlaying && configuredRunRef.current === runId) return

    pl.setWaveConfig(model, runId, forecastHour)
    configuredRunRef.current = runId
  }, [model, runId, forecastHour, enabled, isLoaded, isPlaying])

  // Return imperative handle for playback integration
  const handle: WaveParticleHandle = {
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
