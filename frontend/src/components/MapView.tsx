import { useRef, useState } from 'react'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useMap } from '@/hooks/useMap'
import { useDataAge } from '@/hooks/useDataAge'
import { useManifest } from '@/hooks/useManifest'
import { useWeatherLayer } from '@/hooks/useWeatherLayer'
import { DataAgeIndicator } from '@/components/DataAgeIndicator'
import { OpacitySlider } from '@/components/OpacitySlider'
import { LayerSelector } from '@/components/LayerSelector'

export function MapView() {
  const containerRef = useRef<HTMLDivElement>(null)
  const { map, isLoaded } = useMap({ container: containerRef })
  const dataAge = useDataAge({ model: 'gfs' })
  const [opacity, setOpacity] = useState(0.7)
  const [activeLayerId, setActiveLayerId] = useState<string | null>(null)

  const runId = dataAge?.runId ?? null
  const manifest = useManifest({ model: 'gfs', runId })

  // Auto-select first layer when manifest loads (or if active layer is no longer available)
  const layers = manifest?.layers ?? []
  const resolvedLayerId =
    activeLayerId && layers.some((l) => l.id === activeLayerId)
      ? activeLayerId
      : layers[0]?.id ?? null

  useWeatherLayer({
    map,
    isLoaded,
    layer: resolvedLayerId ?? '',
    model: 'gfs',
    runId,
    opacity,
    visible: resolvedLayerId !== null,
  })

  return (
    <div style={{ position: 'relative', width: '100%', height: '100%' }}>
      <div ref={containerRef} style={{ width: '100%', height: '100%' }} />
      {dataAge && <DataAgeIndicator state={dataAge} />}
      {layers.length > 0 && (
        <LayerSelector
          layers={layers}
          activeLayerId={resolvedLayerId}
          onSelect={setActiveLayerId}
        />
      )}
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
