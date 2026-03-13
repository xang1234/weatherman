/**
 * Track playback visualization — fetches and renders a vessel's
 * historical track as a LineString on the map when a vessel is clicked.
 *
 * Listens for vessel popup open events, fetches the track from
 * /ais/tracks/{mmsi}, and renders it as a GeoJSON line layer.
 */

import { useEffect, useRef } from 'react'
import type maplibregl from 'maplibre-gl'

export interface UseVesselTrackOptions {
  map: React.RefObject<maplibregl.Map | null>
  isLoaded: boolean
  snapshotDate: string | null
}

const TRACK_SOURCE_ID = 'vessel-track'
const TRACK_LINE_LAYER_ID = 'vessel-track-line'
const TRACK_POINT_LAYER_ID = 'vessel-track-points'

/** The same AIS layers from useVesselPopup that trigger on click. */
const AIS_LAYERS = ['ais-vessel-arrows', 'ais-vessel-circles'] as const

export function useVesselTrack({
  map,
  isLoaded,
  snapshotDate,
}: UseVesselTrackOptions): void {
  const apiBase = import.meta.env.VITE_API_BASE_URL || ''
  const abortRef = useRef<AbortController | null>(null)

  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded) return

    async function fetchAndShowTrack(mmsi: number) {
      // Abort previous fetch
      abortRef.current?.abort()
      const controller = new AbortController()
      abortRef.current = controller

      // Remove existing track
      removeTrack(m!)

      try {
        const url = `${apiBase}/ais/tracks/${mmsi}`
        const resp = await fetch(url, { signal: controller.signal })
        if (!resp.ok) return
        const geojson = await resp.json()

        if (controller.signal.aborted) return
        if (!m!.getSource(TRACK_SOURCE_ID)) {
          m!.addSource(TRACK_SOURCE_ID, {
            type: 'geojson',
            data: geojson,
          })
        }

        // Track line layer — gradient from blue (old) to cyan (recent)
        m!.addLayer({
          id: TRACK_LINE_LAYER_ID,
          type: 'line',
          source: TRACK_SOURCE_ID,
          paint: {
            'line-color': '#00bcd4',
            'line-width': 2,
            'line-opacity': 0.8,
          },
          layout: {
            'line-cap': 'round',
            'line-join': 'round',
          },
        })

        // Track start/end points
        const coords: [number, number][] = geojson.geometry?.coordinates ?? []
        if (coords.length > 0) {
          const pointFeatures: GeoJSON.Feature[] = []

          // Start point (green)
          pointFeatures.push({
            type: 'Feature',
            geometry: { type: 'Point', coordinates: coords[0] },
            properties: { role: 'start' },
          })

          // End point (red)
          if (coords.length > 1) {
            pointFeatures.push({
              type: 'Feature',
              geometry: { type: 'Point', coordinates: coords[coords.length - 1] },
              properties: { role: 'end' },
            })
          }

          const pointSourceId = TRACK_SOURCE_ID + '-points'
          m!.addSource(pointSourceId, {
            type: 'geojson',
            data: { type: 'FeatureCollection', features: pointFeatures },
          })

          m!.addLayer({
            id: TRACK_POINT_LAYER_ID,
            type: 'circle',
            source: pointSourceId,
            paint: {
              'circle-radius': 5,
              'circle-color': [
                'match', ['get', 'role'],
                'start', '#4caf50',
                'end', '#f44336',
                '#ffffff',
              ],
              'circle-stroke-width': 1.5,
              'circle-stroke-color': '#ffffff',
            },
          })
        }
      } catch (e) {
        if ((e as Error).name === 'AbortError') return
        console.warn('[useVesselTrack] Failed to fetch track:', e)
      }
    }

    function onClick(e: maplibregl.MapLayerMouseEvent) {
      if (!e.features || e.features.length === 0) return
      const props = e.features[0].properties ?? {}
      const mmsi = props.mmsi as number
      if (!mmsi) return
      fetchAndShowTrack(mmsi)
    }

    // Click on empty map area removes track
    function onMapClick(e: maplibregl.MapMouseEvent) {
      if (!m) return
      // Only remove if the click was NOT on an AIS layer
      const features = m.queryRenderedFeatures(e.point, {
        layers: [...AIS_LAYERS],
      })
      if (features.length === 0) {
        removeTrack(m)
      }
    }

    for (const layerId of AIS_LAYERS) {
      m.on('click', layerId, onClick)
    }
    m.on('click', onMapClick)

    return () => {
      abortRef.current?.abort()
      for (const layerId of AIS_LAYERS) {
        m.off('click', layerId, onClick)
      }
      m.off('click', onMapClick)
      removeTrack(m)
    }
  }, [map, isLoaded, snapshotDate, apiBase])
}

function removeTrack(m: maplibregl.Map): void {
  try {
    if (m.getLayer(TRACK_LINE_LAYER_ID)) m.removeLayer(TRACK_LINE_LAYER_ID)
    if (m.getLayer(TRACK_POINT_LAYER_ID)) m.removeLayer(TRACK_POINT_LAYER_ID)
    if (m.getSource(TRACK_SOURCE_ID)) m.removeSource(TRACK_SOURCE_ID)
    const pointSourceId = TRACK_SOURCE_ID + '-points'
    if (m.getSource(pointSourceId)) m.removeSource(pointSourceId)
  } catch {
    // Map may be destroyed
  }
}
