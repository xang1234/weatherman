import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import type maplibregl from 'maplibre-gl'

const AIS_LAYERS = ['ais-vessel-arrows', 'ais-vessel-circles']
const ROUTE_SOURCE = 'voyage-route'
const ROUTE_LINE_LAYER = 'voyage-route-line'
const ROUTE_POINTS_LAYER = 'voyage-route-points'

export interface VoyageRouteState {
  isDrawing: boolean
  waypoints: [number, number][]
  lineString: GeoJSON.Feature<GeoJSON.LineString> | null
  startDrawing: () => void
  finishDrawing: () => void
  clearRoute: () => void
  undoLastWaypoint: () => void
}

export interface UseVoyageRouteOptions {
  map: React.RefObject<maplibregl.Map | null>
  isLoaded: boolean
}

function waypointsToLineString(
  waypoints: [number, number][],
): GeoJSON.Feature<GeoJSON.LineString> | null {
  if (waypoints.length < 2) return null
  return {
    type: 'Feature',
    geometry: { type: 'LineString', coordinates: waypoints },
    properties: {},
  }
}

function waypointsToPoints(
  waypoints: [number, number][],
): GeoJSON.FeatureCollection<GeoJSON.Point> {
  return {
    type: 'FeatureCollection',
    features: waypoints.map((wp, i) => ({
      type: 'Feature' as const,
      geometry: { type: 'Point' as const, coordinates: wp },
      properties: { index: i },
    })),
  }
}

function emptyFC(): GeoJSON.FeatureCollection {
  return { type: 'FeatureCollection', features: [] }
}

function removeMapLayers(m: maplibregl.Map) {
  try {
    if (m.getLayer(ROUTE_POINTS_LAYER)) m.removeLayer(ROUTE_POINTS_LAYER)
    if (m.getLayer(ROUTE_LINE_LAYER)) m.removeLayer(ROUTE_LINE_LAYER)
    const ptsSourceId = ROUTE_SOURCE + '-pts'
    if (m.getSource(ptsSourceId)) m.removeSource(ptsSourceId)
    if (m.getSource(ROUTE_SOURCE)) m.removeSource(ROUTE_SOURCE)
  } catch { /* map destroyed */ }
}

export function useVoyageRoute({
  map,
  isLoaded,
}: UseVoyageRouteOptions): VoyageRouteState {
  const [isDrawing, setIsDrawing] = useState(false)
  const [waypoints, setWaypoints] = useState<[number, number][]>([])
  const isDrawingRef = useRef(false)

  const lineString = useMemo(() => waypointsToLineString(waypoints), [waypoints])

  // Sync ref for use in event handlers
  useEffect(() => {
    isDrawingRef.current = isDrawing
  }, [isDrawing])

  // Map click handler for adding waypoints
  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded) return

    function onClick(e: maplibregl.MapMouseEvent) {
      if (!isDrawingRef.current) return

      // Skip AIS vessel clicks
      const layers = AIS_LAYERS.filter((id) => m!.getLayer(id))
      if (layers.length > 0) {
        const features = m!.queryRenderedFeatures(e.point, { layers })
        if (features.length > 0) return
      }

      const coord: [number, number] = [e.lngLat.lng, e.lngLat.lat]
      setWaypoints((prev) => [...prev, coord])
    }

    function onDblClick(e: maplibregl.MapMouseEvent) {
      if (!isDrawingRef.current) return
      e.preventDefault()
      // Remove the duplicate waypoint added by the second click event
      // (MapLibre fires click, click, dblclick in sequence)
      setWaypoints((prev) => prev.slice(0, -1))
      setIsDrawing(false)
    }

    m.on('click', onClick)
    m.on('dblclick', onDblClick)
    return () => {
      m.off('click', onClick)
      m.off('dblclick', onDblClick)
    }
  }, [map, isLoaded])

  // Keyboard undo
  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      if (!isDrawingRef.current) return
      if ((e.metaKey || e.ctrlKey) && e.key === 'z') {
        e.preventDefault()
        setWaypoints((prev) => prev.slice(0, -1))
      }
      if (e.key === 'Backspace') {
        e.preventDefault()
        setWaypoints((prev) => prev.slice(0, -1))
      }
    }

    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [])

  // Update map layers when waypoints change
  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded) return

    const source = m.getSource(ROUTE_SOURCE) as maplibregl.GeoJSONSource | undefined
    if (!source) {
      // First time: add source + layers
      m.addSource(ROUTE_SOURCE, {
        type: 'geojson',
        data: lineString ?? (emptyFC() as GeoJSON.GeoJSON),
      })
      m.addLayer({
        id: ROUTE_LINE_LAYER,
        type: 'line',
        source: ROUTE_SOURCE,
        paint: {
          'line-color': '#f0b429',
          'line-width': 3,
          'line-dasharray': [2, 2],
          'line-opacity': 0.9,
        },
        layout: { 'line-cap': 'round', 'line-join': 'round' },
      })
      // Points source
      const pointsSourceId = ROUTE_SOURCE + '-pts'
      m.addSource(pointsSourceId, {
        type: 'geojson',
        data: waypointsToPoints(waypoints),
      })
      m.addLayer({
        id: ROUTE_POINTS_LAYER,
        type: 'circle',
        source: pointsSourceId,
        paint: {
          'circle-radius': 5,
          'circle-color': '#ffffff',
          'circle-stroke-color': '#f0b429',
          'circle-stroke-width': 2,
        },
      })
    } else {
      source.setData(lineString ?? (emptyFC() as GeoJSON.GeoJSON))
      const ptsSource = m.getSource(ROUTE_SOURCE + '-pts') as maplibregl.GeoJSONSource | undefined
      ptsSource?.setData(waypointsToPoints(waypoints))
    }
  }, [map, isLoaded, waypoints])

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      const m = map.current
      if (m) removeMapLayers(m)
    }
  }, [map])

  // Cursor style during drawing
  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded) return
    m.getCanvas().style.cursor = isDrawing ? 'crosshair' : ''
    return () => {
      if (m.getCanvas()) m.getCanvas().style.cursor = ''
    }
  }, [map, isLoaded, isDrawing])

  const startDrawing = useCallback(() => {
    setWaypoints([])
    setIsDrawing(true)
  }, [])

  const finishDrawing = useCallback(() => {
    setIsDrawing(false)
  }, [])

  const clearRoute = useCallback(() => {
    setWaypoints([])
    setIsDrawing(false)
  }, [])

  const undoLastWaypoint = useCallback(() => {
    setWaypoints((prev) => prev.slice(0, -1))
  }, [])

  return {
    isDrawing,
    waypoints,
    lineString,
    startDrawing,
    finishDrawing,
    clearRoute,
    undoLastWaypoint,
  }
}
