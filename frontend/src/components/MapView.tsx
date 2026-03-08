import { useEffect, useRef, useState } from 'react'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useMap } from '@/hooks/useMap'
import { useDataAge } from '@/hooks/useDataAge'
import { useLatestAISDate } from '@/hooks/useLatestAISDate'
import { useManifest } from '@/hooks/useManifest'
import { useWeatherInspector } from '@/hooks/useWeatherInspector'
import { useWeatherLayer } from '@/hooks/useWeatherLayer'
import { useAISLayer } from '@/hooks/useAISLayer'
import { useVesselPopup } from '@/hooks/useVesselPopup'
import { useSSE } from '@/hooks/useSSE'
import { DataAgeIndicator } from '@/components/DataAgeIndicator'
import { ForecastControls } from '@/components/ForecastControls'
import { OpacitySlider } from '@/components/OpacitySlider'
import { LayerSelector } from '@/components/LayerSelector'
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
  const [opacity, setOpacity] = useState(0.7)
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

  useEffect(() => {
    if (!isPlaying || forecastHours.length < 2 || forecastHour == null) return
    const id = setInterval(() => {
      setSelectedForecastHour((prev) => {
        if (prev == null) return forecastHours[0]
        const index = forecastHours.indexOf(prev)
        if (index < 0) return forecastHours[0]
        return forecastHours[(index + 1) % forecastHours.length]
      })
    }, 1200)
    return () => clearInterval(id)
  }, [isPlaying, forecastHour, forecastHours])

  const forecastIndex = forecastHour == null ? -1 : forecastHours.indexOf(forecastHour)
  const prefetchForecastHours = forecastIndex >= 0
    ? [
        forecastHours[forecastIndex - 1],
        forecastHours[forecastIndex + 1],
      ].filter((hour): hour is number => hour != null)
    : []

  useWeatherLayer({
    map,
    isLoaded,
    layer: resolvedLayerId ?? '',
    model: 'gfs',
    runId,
    forecastHour: forecastHour ?? 0,
    opacity,
    visible: resolvedLayerId !== null,
    prefetchForecastHours,
  })

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
        <LayerSelector
          layers={layers}
          activeLayerId={resolvedLayerId}
          onSelect={setActiveLayerId}
        />
      )}
      <ForecastControls
        forecastHours={forecastHours}
        forecastHour={forecastHour}
        isPlaying={isPlaying}
        onChange={setSelectedForecastHour}
        onTogglePlay={() => setIsPlaying((playing) => !playing)}
      />
      <OpacitySlider value={opacity} onChange={setOpacity} />
      {!isLoaded && (
        <div
          style={{
            position: 'absolute',
            inset: 0,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            background: '#1a1a2e',
            color: '#fff',
          }}
        >
          Loading map...
        </div>
      )}
    </div>
  )
}
