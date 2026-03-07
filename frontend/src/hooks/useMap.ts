import { useEffect, useRef, useState } from 'react'
import maplibregl from 'maplibre-gl'
import { addPmtilesProtocol } from '@/utils/pmtiles'
import { darkBasemapStyle } from '@/utils/basemap-style'

export interface UseMapOptions {
  container: React.RefObject<HTMLDivElement | null>
  center?: [number, number]
  zoom?: number
  style?: maplibregl.StyleSpecification | string
}

export function useMap({ container, center, zoom, style }: UseMapOptions) {
  const mapRef = useRef<maplibregl.Map | null>(null)
  const initializedRef = useRef(false)
  const [isLoaded, setIsLoaded] = useState(false)

  useEffect(() => {
    if (!container.current || initializedRef.current) return
    initializedRef.current = true

    addPmtilesProtocol()

    const map = new maplibregl.Map({
      container: container.current,
      style: style ?? darkBasemapStyle,
      center: center ?? [-30, 30],
      zoom: zoom ?? 3,
    })

    map.addControl(new maplibregl.NavigationControl(), 'top-right')

    map.on('load', () => setIsLoaded(true))

    mapRef.current = map

    return () => {
      map.remove()
      mapRef.current = null
      setIsLoaded(false)
      initializedRef.current = false
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  return { map: mapRef, isLoaded }
}
