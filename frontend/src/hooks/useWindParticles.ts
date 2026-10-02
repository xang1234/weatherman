/**
 * React hook for the GPU wind-particle animation layer.
 *
 * Creates a WindParticleLayer (MapLibre custom layer) that renders
 * GPU-advected particles using wind U/V data tiles. Particle count
 * is auto-adapted to the device GPU (16K-262K particles).
 * Particles are advected by the actual wind field and leave fading trails.
 *
 * An overlay: drawn over whichever colour layer is selected, while enabled.
 */

import { useEffect, useRef } from 'react'
import type maplibregl from 'maplibre-gl'
import { WindParticleLayer } from '@/layers/WindParticleLayer'
import { particleInsertBeforeId } from './layer-order'
import { COLOR_RAMPS } from '@/layers/color-ramps'
import { useColorRamps } from './useColorRamps'

export interface UseWindParticlesOptions {
  map: React.RefObject<maplibregl.Map | null>
  isLoaded: boolean
  /** Whether the particles are shown. */
  enabled: boolean
  /** Particle opacity 0-1. */
  opacity: number
  model: string
  runId: string | null
  forecastHour: number
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
  enabled,
  opacity,
  model,
  runId,
  forecastHour,
  isPlaying = false,
}: UseWindParticlesOptions): WindParticleHandle {
  const apiBase = import.meta.env.VITE_API_BASE_URL || ''
  const layerRef = useRef<WindParticleLayer | null>(null)
  const configuredRunRef = useRef<string | null>(null)
  const rampsReady = useColorRamps()
  const isActive = enabled

  // Create the particle layer once when the map is ready.
  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded) return

    const tileFormat = import.meta.env.VITE_USE_FLOAT16_TILES === 'true' ? 'f16' as const : 'png' as const
    const particleLayer = new WindParticleLayer({
      id: 'wind-particles',
      opacity: 0.85,
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

  // Update wind config when dataset changes or layer is (re)created.
  // During playback the RAF loop drives the hour imperatively and calling
  // setWindConfig each step would nuke the tile cache via TileManager.setLayer(),
  // so it only runs then if this run was never configured (enabled mid-play).
  useEffect(() => {
    const pl = layerRef.current
    if (!enabled) {
      // Re-enabling must configure again: the hour has moved on meanwhile.
      configuredRunRef.current = null
      return
    }
    if (!pl || !runId || !rampsReady) return
    if (isPlaying && configuredRunRef.current === runId) return

    // The range the U/V tiles were encoded with.
    const { valueMin, valueMax } = COLOR_RAMPS['wind_u']
    pl.setWindConfig(model, runId, forecastHour, valueMin, valueMax)
    configuredRunRef.current = runId
  }, [model, runId, forecastHour, enabled, isLoaded, isPlaying, rampsReady])

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
