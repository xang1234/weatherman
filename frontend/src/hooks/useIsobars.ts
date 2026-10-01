/**
 * Isobar overlay: mean-sea-level pressure contours with H/L centres.
 *
 * Fetches one GeoJSON per forecast hour from /api/contours, keeps the
 * recent ones, and prefetches the next hour so stepping and playback swap
 * instantly. Drawn as plain MapLibre line and symbol layers above the
 * weather and particles, below the basemap's place labels.
 */

import { useEffect, useRef } from 'react'
import type maplibregl from 'maplibre-gl'

const SOURCE_ID = 'isobars'
const LINE_LAYER = 'isobars-line'
const LABEL_LAYER = 'isobars-label'
const CENTRE_LAYER = 'isobars-centres'
const LAYER_IDS = [LINE_LAYER, LABEL_LAYER, CENTRE_LAYER]
const EMPTY: GeoJSON.FeatureCollection = { type: 'FeatureCollection', features: [] }
/** Hours of contours kept in memory (about 170 KB each). */
const CACHE_SIZE = 12

export interface UseIsobarsOptions {
  map: React.RefObject<maplibregl.Map | null>
  isLoaded: boolean
  enabled: boolean
  opacity: number
  model: string
  runId: string | null
  forecastHour: number | null
  forecastHours: number[]
}

export function useIsobars({
  map,
  isLoaded,
  enabled,
  opacity,
  model,
  runId,
  forecastHour,
  forecastHours,
}: UseIsobarsOptions): void {
  const apiBase = import.meta.env.VITE_API_BASE_URL || ''
  const cacheRef = useRef(new Map<string, Promise<GeoJSON.FeatureCollection>>())

  // Source and layers, once per map.
  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded) return
    m.addSource(SOURCE_ID, { type: 'geojson', data: EMPTY })
    // Below the basemap's place labels, above everything drawn under them.
    const beforeId = m.getStyle().layers.find((l) => l.type === 'symbol')?.id
    m.addLayer({
      id: LINE_LAYER,
      type: 'line',
      source: SOURCE_ID,
      filter: ['==', ['get', 'kind'], 'isobar'],
      layout: { visibility: 'none', 'line-join': 'round' },
      paint: { 'line-color': '#ffffff', 'line-width': 1 },
    }, beforeId)
    m.addLayer({
      id: LABEL_LAYER,
      type: 'symbol',
      source: SOURCE_ID,
      filter: ['==', ['get', 'kind'], 'isobar'],
      layout: {
        visibility: 'none',
        'symbol-placement': 'line',
        'symbol-spacing': 350,
        'text-field': ['to-string', ['get', 'hpa']],
        'text-font': ['Noto Sans Regular'],
        'text-size': 10,
      },
      paint: { 'text-color': '#ffffff', 'text-halo-color': 'rgba(0, 0, 0, 0.55)', 'text-halo-width': 1.2 },
    }, beforeId)
    m.addLayer({
      id: CENTRE_LAYER,
      type: 'symbol',
      source: SOURCE_ID,
      filter: ['!=', ['get', 'kind'], 'isobar'],
      layout: {
        visibility: 'none',
        'text-field': [
          'format',
          ['match', ['get', 'kind'], 'high', 'H', 'L'], { 'font-scale': 1.6 },
          '\n', {},
          ['to-string', ['get', 'hpa']], { 'font-scale': 0.75 },
        ],
        'text-font': ['Noto Sans Medium'],
        'text-size': 14,
        'text-allow-overlap': true,
      },
      paint: {
        'text-color': ['match', ['get', 'kind'], 'high', '#ffd2c2', '#c8dcff'],
        'text-halo-color': 'rgba(0, 0, 0, 0.55)',
        'text-halo-width': 1.4,
      },
    }, beforeId)

    return () => {
      try {
        for (const id of LAYER_IDS) if (m.getLayer(id)) m.removeLayer(id)
        if (m.getSource(SOURCE_ID)) m.removeSource(SOURCE_ID)
      } catch { /* map may be destroyed */ }
    }
  }, [map, isLoaded])

  // Visibility and opacity.
  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded || !m.getLayer(LINE_LAYER)) return
    for (const id of LAYER_IDS) m.setLayoutProperty(id, 'visibility', enabled ? 'visible' : 'none')
    m.setPaintProperty(LINE_LAYER, 'line-opacity', opacity * 0.85)
    m.setPaintProperty(LABEL_LAYER, 'text-opacity', opacity)
    m.setPaintProperty(CENTRE_LAYER, 'text-opacity', opacity)
  }, [map, isLoaded, enabled, opacity])

  // The current hour's contours, plus a prefetch of the next.
  useEffect(() => {
    const m = map.current
    if (!m || !isLoaded || !enabled || !runId || forecastHour == null) return
    const cache = cacheRef.current
    const load = (hour: number) => {
      const key = `${model}|${runId}|${hour}`
      let entry = cache.get(key)
      if (!entry) {
        entry = fetch(`${apiBase}/api/contours/${model}/${runId}/prmsl/${hour}`)
          // A 404 (no pressure field in this run) stays cached; any other
          // failure is dropped so the hour is fetched again next time.
          .then((res) => {
            if (res.ok) return res.json() as Promise<GeoJSON.FeatureCollection>
            if (res.status === 404) return EMPTY
            throw new Error(`HTTP ${res.status}`)
          })
          .catch(() => {
            cache.delete(key)
            return EMPTY
          })
        cache.set(key, entry)
        // ponytail: FIFO eviction; an LRU only matters with far more hours.
        if (cache.size > CACHE_SIZE) cache.delete(cache.keys().next().value!)
      }
      return entry
    }

    let current = true
    load(forecastHour).then((data) => {
      if (current) (m.getSource(SOURCE_ID) as maplibregl.GeoJSONSource | undefined)?.setData(data)
    })
    const next = forecastHours[forecastHours.indexOf(forecastHour) + 1]
    if (next != null) void load(next)
    return () => { current = false }
  }, [map, isLoaded, enabled, model, runId, forecastHour, forecastHours, apiBase])
}
