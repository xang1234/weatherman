import { useEffect, useRef, useState } from 'react'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useMap } from '@/hooks/useMap'
import { useDataAge } from '@/hooks/useDataAge'
import { useLatestAISSnapshot } from '@/hooks/useLatestAISSnapshot'
import { useManifest } from '@/hooks/useManifest'
import { useWeatherInspector } from '@/hooks/useWeatherInspector'
import { useHoverProbe } from '@/hooks/useHoverProbe'
import { useWeatherLayer, type WeatherLayerHandle } from '@/hooks/useWeatherLayer'
import { useAISLayer } from '@/hooks/useAISLayer'
import { useVesselPopup } from '@/hooks/useVesselPopup'
import { useVesselTrack } from '@/hooks/useVesselTrack'
import { useWindParticles, type WindParticleHandle } from '@/hooks/useWindParticles'
import { useIsobars } from '@/hooks/useIsobars'
import { useWaveParticles, type WaveParticleHandle } from '@/hooks/useWaveParticles'
import { useVoyageRoute } from '@/hooks/useVoyageRoute'
import { useVoyageCorridor } from '@/hooks/useVoyageCorridor'
import { useSSE } from '@/hooks/useSSE'
import { DataAgeIndicator } from '@/components/DataAgeIndicator'
import { ForecastControls } from '@/components/ForecastControls'
import { LayerPanel, type OverlayId, type OverlayState } from '@/components/LayerPanel'
import { ModelSelector, type ModelId } from '@/components/ModelSelector'
import { VoyageDrawButton } from '@/components/VoyageDrawButton'
import { VoyageWeatherPanel } from '@/components/VoyageWeatherPanel'
import { WeatherInspector } from '@/components/WeatherInspector'
import { WeatherHoverHud } from '@/components/WeatherHoverHud'
import type { DataRanges, LayerConfig } from '@/types/manifest'

const EMPTY_LAYERS: LayerConfig[] = []
const EMPTY_FORECAST_HOURS: number[] = []
const NO_DATA_RANGES: DataRanges = {}

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
  const latestAIS = useLatestAISSnapshot()
  const dataAge = useDataAge({ model, version: sse.weatherVersion })
  const [opacity, setOpacity] = useState(0.9)
  const [activeLayerId, setActiveLayerId] = useState<string | null>(null)
  // Overlays drawn over the colour layer. `on: null` follows the colour layer
  // (wind particles with wind speed, dashes with wave height; isobars off)
  // until toggled.
  const [overlays, setOverlays] = useState<Record<OverlayId, OverlayState>>({
    wind: { on: null, opacity: 0.85 },
    waves: { on: null, opacity: 0.8 },
    isobars: { on: null, opacity: 0.8 },
  })
  const [selectedForecastHour, setSelectedForecastHour] = useState<number | null>(() =>
    forecastHourFromUrl([]),
  )
  const [isPlaying, setIsPlaying] = useState(false)

  const runId = dataAge?.runId ?? null
  const manifest = useManifest({ model, runId })
  // This run's tile encoding ranges, once its manifest is in (#83). Runs
  // tiled before have none: the layers then use the current ones.
  const dataRanges = manifest && manifest.run_id === runId ? manifest.data_ranges ?? NO_DATA_RANGES : undefined

  // Auto-select first layer when manifest loads (or if active layer is no longer available)
  const layers = manifest?.layers ?? EMPTY_LAYERS
  const resolvedLayerId =
    activeLayerId && layers.some((l) => l.id === activeLayerId)
      ? activeLayerId
      : layers[0]?.id ?? null
  // A particle overlay needs its layer in the run's manifest: the pipeline
  // leaves out a layer whose data failed its checks (#71).
  const windAvailable = layers.some((l) => l.id === 'wind_speed')
  const wavesAvailable = layers.some((l) => l.id === 'wave_height')
  const windOn = windAvailable && (overlays.wind.on ?? resolvedLayerId === 'wind_speed')
  const wavesOn = wavesAvailable && (overlays.waves.on ?? resolvedLayerId === 'wave_height')
  const isobarsOn = overlays.isobars.on ?? false
  const updateOverlay = (id: OverlayId, change: Partial<OverlayState>) =>
    setOverlays((current) => ({ ...current, [id]: { ...current[id], ...change } }))
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
  // While the slider is dragged: its fractional position, shown as the hour
  // before it blended towards the hour after (#24).
  const [scrubPosition, setScrubPosition] = useState<number | null>(null)
  const scrubMix = scrubPosition == null || forecastIndex < 0
    ? 0
    : Math.min(1, Math.max(0, scrubPosition - forecastIndex))
  const handleScrub = (position: number | null) => {
    setScrubPosition(position)
    if (position == null) return
    setIsPlaying(false)
    const hour = forecastHours[Math.min(Math.floor(position), forecastHours.length - 1)]
    if (hour !== forecastHour) setSelectedForecastHour(hour)
  }
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
    temporalMix: isPlaying ? undefined : scrubMix,
    dataRanges,
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
      // Wrapping from the last hour to the first: snap rather than morph
      // across the whole forecast range (#36).
      const blendMix = nextIdx === 0 ? 0 : mix

      handleRef.current.setTemporalBlend?.(hours[nextIdx], blendMix)
      windParticlesRef.current.setTemporalBlend?.(hours[nextIdx], blendMix)
      waveParticlesRef.current.setTemporalBlend?.(hours[nextIdx], blendMix)

      if (mix >= 1) {
        // Gate advance on every layer having the next hour for the whole
        // viewport — hold at the end of the step until they do.
        const t1Ready =
          (handleRef.current.isT1Ready?.() ?? true) &&
          (windParticlesRef.current.isT1Ready?.() ?? true) &&
          (waveParticlesRef.current.isT1Ready?.() ?? true)
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

  const ais = sse.aisSnapshot ?? latestAIS
  const aisDate = ais?.date ?? null

  useAISLayer({
    map,
    isLoaded,
    snapshotDate: aisDate,
    revision: ais?.revision ?? 0,
  })

  const windParticles = useWindParticles({
    map,
    isLoaded,
    enabled: windOn,
    opacity: overlays.wind.opacity,
    model,
    runId: runId ?? '',
    forecastHour: forecastHour ?? 0,
    isPlaying,
    dataRanges,
  })

  // Keep particle handles in refs for the RAF loop
  const windParticlesRef = useRef<WindParticleHandle>(windParticles)
  windParticlesRef.current = windParticles

  const waveParticles = useWaveParticles({
    map,
    isLoaded,
    enabled: wavesOn,
    opacity: overlays.waves.opacity,
    model,
    runId: runId ?? '',
    forecastHour: forecastHour ?? 0,
    isPlaying,
    dataRanges,
  })

  const waveParticlesRef = useRef<WaveParticleHandle>(waveParticles)
  waveParticlesRef.current = waveParticles

  // Dragging the slider blends the particle fields too; otherwise (paused)
  // they show the selected hour alone, as before.
  const scrubbing = scrubPosition != null
  useEffect(() => {
    if (isPlaying) return
    const next = scrubbing && forecastHourNext != null ? forecastHourNext : -1
    windParticlesRef.current.setTemporalBlend?.(next, scrubbing ? scrubMix : 0)
    waveParticlesRef.current.setTemporalBlend?.(next, scrubbing ? scrubMix : 0)
  }, [isPlaying, scrubbing, forecastHourNext, scrubMix])

  useIsobars({
    map,
    isLoaded,
    enabled: isobarsOn,
    opacity: overlays.isobars.opacity,
    model,
    runId,
    forecastHour,
    forecastHours,
  })

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
      <WeatherInspector inspector={inspector} forecastHour={forecastHour} cycleTime={manifest?.cycle_time ?? null} />
      <WeatherHoverHud
        probe={hoverProbe}
        forecastHour={forecastHour}
        cycleTime={manifest?.cycle_time ?? null}
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
          opacity={opacity}
          onOpacityChange={setOpacity}
          overlays={[
            ...(windAvailable ? [{ id: 'wind' as const, label: 'Wind particles', on: windOn, opacity: overlays.wind.opacity }] : []),
            ...(wavesAvailable ? [{ id: 'waves' as const, label: 'Wave dashes', on: wavesOn, opacity: overlays.waves.opacity }] : []),
            { id: 'isobars', label: 'Isobars', on: isobarsOn, opacity: overlays.isobars.opacity },
          ]}
          onOverlayChange={updateOverlay}
        />
      )}
      <ForecastControls
        cycleTime={manifest?.cycle_time ?? null}
        forecastHours={forecastHours}
        forecastHour={forecastHour}
        isPlaying={isPlaying}
        onChange={setSelectedForecastHour}
        onTogglePlay={() => setIsPlaying((playing) => !playing)}
        scrubPosition={scrubPosition}
        onScrub={handleScrub}
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
