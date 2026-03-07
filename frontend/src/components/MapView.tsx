import { useRef } from 'react'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useMap } from '@/hooks/useMap'
import { useDataAge } from '@/hooks/useDataAge'
import { DataAgeIndicator } from '@/components/DataAgeIndicator'

export function MapView() {
  const containerRef = useRef<HTMLDivElement>(null)
  const { isLoaded } = useMap({ container: containerRef })
  const dataAge = useDataAge({ model: 'gfs' })

  return (
    <div style={{ position: 'relative', width: '100%', height: '100%' }}>
      <div ref={containerRef} style={{ width: '100%', height: '100%' }} />
      {dataAge && <DataAgeIndicator state={dataAge} />}
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
