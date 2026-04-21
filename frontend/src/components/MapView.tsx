import { useEffect, useRef, useState } from 'react'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useMap } from '@/hooks/useMap'
import { useDataAge } from '@/hooks/useDataAge'
import { useLatestAISDate } from '@/hooks/useLatestAISDate'
import { useManifest } from '@/hooks/useManifest'
import { useWeatherInspector } from '@/hooks/useWeatherInspector'
import { useHoverProbe } from '@/hooks/useHoverProbe'
import { useWeatherLayer, type WeatherLayerHandle } from '@/hooks/useWeatherLayer'
import { useAISLayer } from '@/hooks/useAISLayer'
import { useVesselPopup } from '@/hooks/useVesselPopup'
import { useVesselTrack } from '@/hooks/useVesselTrack'
import { useWindParticles, type WindParticleHandle } from '@/hooks/useWindParticles'
import { useWaveParticles, type WaveParticleHandle } from '@/hooks/useWaveParticles'
import { useVoyageRoute } from '@/hooks/useVoyageRoute'
import { useVoyageCorridor } from '@/hooks/useVoyageCorridor'
import { useSSE } from '@/hooks/useSSE'
import { DataAgeIndicator } from '@/components/DataAgeIndicator'
import { ForecastControls } from '@/components/ForecastControls'
import { LayerPanel } from '@/components/LayerPanel'
import { ModelSelector, type ModelId } from '@/components/ModelSelector'
import { VoyageDrawButton } from '@/components/VoyageDrawButton'
import { VoyageWeatherPanel } from '@/components/VoyageWeatherPanel'
import { WeatherInspector } from '@/components/WeatherInspector'
import { WeatherHoverHud } from '@/components/WeatherHoverHud'
import type { LayerConfig } from '@/types/manifest'

const EMPTY_LAYERS: LayerConfig[] = []
const EMPTY_FORECAST_HOURS: number[] = []

function forecastHourFromUrl(forecastHours: number[]): number | null {
  const params = new URLSearchParams(window.location.search)
  const raw = params.get('fh')
  if (!raw) return forecastHours[0] ?? null
  const value = Number(raw)
  return forecastHours.includes(value) ? value : (forecastHours[0] ?? null)
}

export function MapView() {
  const containerRef = useRef<HTMLDivElement>(null)
  const { map, isLoaded } = useMap({ container: containerRef })
  const [model, setModel] = useState<ModelId>('gfs')
  const sse = useSSE()
  const latestAISDate = useLatestAISDate()
  const dataAge = useDataAge({ model, version: sse.weatherVersion })
  const opacity = 0.7
  const [activeLayerId, setActiveLayerId] = useState<string | null>(null)
  const [selectedForecastHour, setSelectedForecastHour] = useState<number | null>(() =>
    forecastHourFromUrl([]),
  )
  const [isPlaying, setIsPlaying] = useState(false)

  const runId = dataAge?.runId ?? null
  const manifest = useManifest({ model, runId })

  // Auto-select first layer when manifest loads (or if active layer is no longer available)
  const layers = manifest?.layers ?? EMPTY_LAYERS
  const resolvedLayerId =
    activeLayerId && layers.some((l) => l.id === activeLayerId)
      ? activeLayerId
      : layers[0]?.id ?? null
  const forecastHours = manifest?.forecast_hours ?? EMPTY_FORECAST_HOURS
  const forecastHour = (
    selectedForecastHour != null && forecastHours.includes(selectedForecastHour)
      ? selectedForecastHour
      : forecastHourFromUrl(forecastHours)
  )

  useEffect(() => {
    if (forecastHour == null) return
    const url = new URL(window.location.href)
    url.searchParams.set('fh', String(forecastHour))
    window.history.replaceState({}, '', url)
  }, [forecastHour])

  useEffect(() => {
    function onPopState() {
      if (forecastHours.length === 0) return
      setSelectedForecastHour(forecastHourFromUrl(forecastHours))
    }
    window.addEventListener('popstate', onPopState)
    return () => window.removeEventListener('popstate', onPopState)
  }, [forecastHours])

  const forecastIndex = forecastHour == null ? -1 : forecastHours.indexOf(forecastHour)
  const forecastHourNext = forecastIndex >= 0
    ? forecastHours[(forecastIndex + 1) % forecastHours.length]
    : undefined
  const prefetchForecastHours = forecastIndex >= 0
    ? [
        forecastHours[forecastIndex - 1],
        forecastHours[forecastIndex + 1],
      ].filter((hour): hour is number => hour != null)
    : []

  // During playback the RAF loop drives temporal blending imperatively —
  // suppress prop-driven forecastHourNext/temporalMix to avoid conflicting
  // setTemporalBlend calls (the prop-driven useEffect would snap to mix=0
  // on every React re-render, causing a one-frame visual glitch).
  const weatherHandle = useWeatherLayer({
    map,
    isLoaded,
    layer: resolvedLayerId ?? '',
    model,
    runId,
    forecastHour: forecastHour ?? 0,
    opacity,
    visible: resolvedLayerId !== null,
    prefetchForecastHours,
    forecastHourNext: isPlaying ? undefined : forecastHourNext,
    temporalMix: isPlaying ? undefined : 0,
  })

  // Keep handle and forecastHours in refs so the RAF callback always reads the
  // latest values without being listed as dependencies (avoids tearing down the
  // animation loop on every render or manifest re-fetch).
  const handleRef = useRef<WeatherLayerHandle>(weatherHandle)
  handleRef.current = weatherHandle
  const forecastHoursRef = useRef(forecastHours)
  forecastHoursRef.current = forecastHours
  const forecastIndexRef = useRef(forecastIndex)
  forecastIndexRef.current = forecastIndex
  const playbackIdxRef = useRef(0)

  const PLAYBACK_STEP_MS = 1200

  useEffect(() => {
    if (!isPlaying || forecastHours.length < 2) return

    // Seed the playback index from the latest forecastIndex ref.
    // Using a ref (not the dep) avoids restarting the RAF loop on every
    // step advance — setSelectedForecastHour changes forecastIndex each
    // step, and restarting would fire the cleanup snap (setTemporalBlend(-1,0)).
    playbackIdxRef.current = forecastIndexRef.current >= 0 ? forecastIndexRef.current : 0

    let rafId: number
    let startTime = performance.now()

    function tick() {
      const hours = forecastHoursRef.current
      if (hours.length < 2) return

      const elapsed = performance.now() - startTime
      const mix = Math.min(1, elapsed / PLAYBACK_STEP_MS)
      const idx = playbackIdxRef.current
      const nextIdx = (idx + 1) % hours.length

      handleRef.current.setTemporalBlend?.(hours[nextIdx], mix)
      windParticlesRef.current.setTemporalBlend?.(hours[nextIdx], mix)
      waveParticlesRef.current.setTemporalBlend?.(hours[nextIdx], mix)

      if (mix >= 1) {
        // Gate advance on T1 tile readiness — if T1 tiles haven't loaded
        // yet, hold at mix=1 and keep requesting frames until ready.
        const windT1Ready = windParticlesRef.current.isT1Ready?.() ?? true
        const waveT1Ready = waveParticlesRef.current.isT1Ready?.() ?? true
        const t1Ready = windT1Ready && waveT1Ready
        if (!t1Ready) {
          rafId = requestAnimationFrame(tick)
          return
        }
        // Step complete — advance the hour.
        // Swap T0↔T1 BEFORE reconfiguring T1 for the next-next hour.
        // This must be synchronous — if deferred to React effects,
        // the next RAF tick's setTemporalBlend destroys T1's tiles.
        playbackIdxRef.current = nextIdx
        handleRef.current.advanceForecastHour?.(hours[nextIdx])
        windParticlesRef.current.advanceForecastHour?.(hours[nextIdx])
        waveParticlesRef.current.advanceForecastHour?.(hours[nextIdx])
        setSelectedForecastHour(hours[nextIdx])
        startTime = performance.now()
        handleRef.current.setTemporalBlend?.(
          hours[(nextIdx + 1) % hours.length], 0,
        )
        windParticlesRef.current.setTemporalBlend?.(
          hours[(nextIdx + 1) % hours.length], 0,
        )
        waveParticlesRef.current.setTemporalBlend?.(
          hours[(nextIdx + 1) % hours.length], 0,
        )
      }

      rafId = requestAnimationFrame(tick)
    }

    rafId = requestAnimationFrame(tick)
    return () => {
      cancelAnimationFrame(rafId)
      handleRef.current.setTemporalBlend?.(-1, 0) // snap clean on pause
      windParticlesRef.current.setTemporalBlend?.(-1, 0)
      waveParticlesRef.current.setTemporalBlend?.(-1, 0)
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isPlaying])

  const aisDate = sse.aisDate ?? latestAISDate

  useAISLayer({
    map,
    isLoaded,
    snapshotDate: aisDate,
  })

  const windParticles = useWindParticles({
    map,
    isLoaded,
    layer: resolvedLayerId ?? '',
    model,
    runId: runId ?? '',
    forecastHour: forecastHour ?? 0,
    visible: resolvedLayerId === 'wind_speed',
    isPlaying,
  })

  // Keep particle handles in refs for the RAF loop
  const windParticlesRef = useRef<WindParticleHandle>(windParticles)
  windParticlesRef.current = windParticles

  const waveParticles = useWaveParticles({
    map,
    isLoaded,
    layer: resolvedLayerId ?? '',
    model,
    runId: runId ?? '',
    forecastHour: forecastHour ?? 0,
    visible: resolvedLayerId === 'wave_height',
    isPlaying,
  })

  const waveParticlesRef = useRef<WaveParticleHandle>(waveParticles)
  waveParticlesRef.current = waveParticles

  useVesselPopup({ map, isLoaded })
  useVesselTrack({ map, isLoaded, snapshotDate: aisDate })
  const voyageRoute = useVoyageRoute({ map, isLoaded })
  const voyageCorridor = useVoyageCorridor({
    lineString: voyageRoute.lineString,
    model,
    runId,
  })
  const inspector = useWeatherInspector({
    map,
    isLoaded,
    model,
    runId,
    disabled: voyageRoute.isDrawing,
  })
  const hoverProbe = useHoverProbe({
    map,
    isLoaded,
    model,
    runId,
    disabled: voyageRoute.isDrawing || inspector.point !== null,
  })

  return (
    <div style={{ position: 'relative', width: '100%', height: '100%' }}>
      <div ref={containerRef} style={{ width: '100%', height: '100%' }} />
      <ModelSelector model={model} onChange={setModel} />
      {dataAge && <DataAgeIndicator state={dataAge} />}
      <WeatherInspector inspector={inspector} forecastHour={forecastHour} />
      <WeatherHoverHud
        probe={hoverProbe}
        forecastHour={forecastHour}
        activeVariable={resolvedLayerId}
      />
      <VoyageDrawButton route={voyageRoute} />
      {voyageRoute.lineString && (
        <VoyageWeatherPanel
          corridor={voyageCorridor}
          route={voyageRoute}
          forecastHour={forecastHour}
          layers={layers}
          onClose={voyageRoute.clearRoute}
        />
      )}
      {layers.length > 0 && (
        <LayerPanel
          layers={layers}
          activeLayerId={resolvedLayerId}
          onSelect={setActiveLayerId}
        />
      )}
      <ForecastControls
        cycleTime={manifest?.cycle_time ?? null}
        forecastHours={forecastHours}
        forecastHour={forecastHour}
        isPlaying={isPlaying}
        onChange={setSelectedForecastHour}
        onTogglePlay={() => setIsPlaying((playing) => !playing)}
      />
      {!isLoaded && (
        <div
          style={{
            position: 'absolute',
            inset: 0,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            background: '#f0f0f0',
            color: '#374151',
          }}
        >
          Loading map...
        </div>
      )}
    </div>
  )
}
