import { useEffect, useRef, useState } from 'react'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useMap } from '@/hooks/useMap'
import { useDataAge } from '@/hooks/useDataAge'
import { useLatestAISDate } from '@/hooks/useLatestAISDate'
import { useManifest } from '@/hooks/useManifest'
import { useWeatherInspector } from '@/hooks/useWeatherInspector'
import { useWeatherLayer, type WeatherLayerHandle } from '@/hooks/useWeatherLayer'
import { useAISLayer } from '@/hooks/useAISLayer'
import { useVesselPopup } from '@/hooks/useVesselPopup'
import { useSSE } from '@/hooks/useSSE'
import { DataAgeIndicator } from '@/components/DataAgeIndicator'
import { ForecastControls } from '@/components/ForecastControls'
import { LayerPanel } from '@/components/LayerPanel'
import { WeatherInspector } from '@/components/WeatherInspector'
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
  const sse = useSSE()
  const latestAISDate = useLatestAISDate()
  const dataAge = useDataAge({ model: 'gfs', version: sse.weatherVersion })
  const opacity = 0.7
  const [activeLayerId, setActiveLayerId] = useState<string | null>(null)
  const [selectedForecastHour, setSelectedForecastHour] = useState<number | null>(() =>
    forecastHourFromUrl([]),
  )
  const [isPlaying, setIsPlaying] = useState(false)

  const runId = dataAge?.runId ?? null
  const manifest = useManifest({ model: 'gfs', runId })

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
    model: 'gfs',
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
  const playbackIdxRef = useRef(0)

  const PLAYBACK_STEP_MS = 1200

  useEffect(() => {
    if (!isPlaying || forecastHours.length < 2) return

    // Seed the playback index from React state, but only on effect start —
    // subsequent steps are tracked via the ref to survive effect restarts.
    playbackIdxRef.current = forecastIndex >= 0 ? forecastIndex : 0

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

      if (mix >= 1) {
        // Step complete — advance the hour via React state
        playbackIdxRef.current = nextIdx
        setSelectedForecastHour(hours[nextIdx])
        startTime = performance.now()
        handleRef.current.setTemporalBlend?.(
          hours[(nextIdx + 1) % hours.length], 0,
        )
      }

      rafId = requestAnimationFrame(tick)
    }

    rafId = requestAnimationFrame(tick)
    return () => {
      cancelAnimationFrame(rafId)
      handleRef.current.setTemporalBlend?.(-1, 0) // snap clean on pause
    }
  }, [isPlaying, forecastIndex])

  const aisDate = sse.aisDate ?? latestAISDate

  useAISLayer({
    map,
    isLoaded,
    snapshotDate: aisDate,
  })

  useVesselPopup({ map, isLoaded })
  const inspector = useWeatherInspector({
    map,
    isLoaded,
    model: 'gfs',
    runId,
  })

  return (
    <div style={{ position: 'relative', width: '100%', height: '100%' }}>
      <div ref={containerRef} style={{ width: '100%', height: '100%' }} />
      {dataAge && <DataAgeIndicator state={dataAge} />}
      <WeatherInspector inspector={inspector} forecastHour={forecastHour} />
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
