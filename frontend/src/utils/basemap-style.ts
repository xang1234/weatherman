import type maplibregl from 'maplibre-gl'

/**
 * Default PMTiles basemap URL (Protomaps daily build).
 * Override via VITE_BASEMAP_URL env var for local/offline use.
 *
 * NOTE: Protomaps daily builds expire after ~7 days. Update this date
 * periodically, or self-host the PMTiles file for stability.
 */
const RAW_BASEMAP_URL =
  import.meta.env.VITE_BASEMAP_URL ||
  'https://build.protomaps.com/20260311.pmtiles'

const PMTILES_SOURCE = RAW_BASEMAP_URL.startsWith('pmtiles://')
  ? RAW_BASEMAP_URL
  : `pmtiles://${RAW_BASEMAP_URL}`

/**
 * Whether to use PMTiles (production) or a raster tile fallback (local dev).
 * Set VITE_BASEMAP_URL to a valid PMTiles URL to use vector tiles.
 */
const USE_PMTILES =
  RAW_BASEMAP_URL.endsWith('.pmtiles') ||
  RAW_BASEMAP_URL.startsWith('pmtiles://')

/**
 * Raster basemap opacity when weather overlay is active. Higher than the fill
 * dim because raster tiles carry labels baked-in; over-dimming makes labels
 * unreadable. Tradeoff is inherent to raster fallback.
 */
const WEATHER_RASTER_OPACITY = 0.55

/** Label layers whose colours flip to white-on-dark when weather is showing. */
const LABEL_LAYERS = ['places_country', 'places_city']

/** Weather layers that only have data over ocean — need opaque earth mask. */
const OCEAN_ONLY_LAYERS = new Set(['wave_height'])

/**
 * Light basemap style optimized for weather overlay readability.
 *
 * Uses Protomaps vector tiles via PMTiles protocol when available,
 * falling back to CartoDB light raster tiles for local development.
 *
 * Starts as a plain light map. When weather is showing,
 * setWeatherOverlayOpacity() hides the fills and darkens the background so
 * the overlay keeps its full colour and the basemap contributes only thin
 * coastlines, borders and white labels — the Windy.com look.
 */
export const darkBasemapStyle: maplibregl.StyleSpecification = USE_PMTILES
  ? {
      version: 8,
      glyphs:
        'https://protomaps.github.io/basemaps-assets/fonts/{fontstack}/{range}.pbf',
      sources: {
        protomaps: {
          type: 'vector',
          url: PMTILES_SOURCE,
          attribution:
            '<a href="https://protomaps.com">Protomaps</a> | <a href="https://openstreetmap.org">OSM</a>',
        },
      },
      layers: [
        {
          id: 'background',
          type: 'background',
          paint: { 'background-color': '#f0f0f0' },
        },
        {
          id: 'water',
          type: 'fill',
          source: 'protomaps',
          'source-layer': 'water',
          paint: { 'fill-color': '#b8d4e8' },
        },
        {
          id: 'earth',
          type: 'fill',
          source: 'protomaps',
          'source-layer': 'earth',
          paint: { 'fill-color': '#e8e8e8' },
        },
        {
          id: 'earth_outline',
          type: 'line',
          source: 'protomaps',
          'source-layer': 'earth',
          paint: {
            'line-color': 'rgba(10, 14, 22, 0.75)',
            'line-width': ['interpolate', ['linear'], ['zoom'], 0, 0.7, 4, 1.2, 8, 1.6, 14, 2.2],
          },
        },
        {
          id: 'boundaries',
          type: 'line',
          source: 'protomaps',
          'source-layer': 'boundaries',
          paint: {
            'line-color': 'rgba(10, 14, 22, 0.45)',
            'line-width': ['interpolate', ['linear'], ['zoom'], 1, 0.5, 6, 0.9, 10, 1.3],
          },
        },
        {
          id: 'places_country',
          type: 'symbol',
          source: 'protomaps',
          'source-layer': 'places',
          filter: ['==', 'kind', 'country'],
          layout: {
            'text-field': ['get', 'name'],
            'text-font': ['Noto Sans Regular'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 1, 10, 5, 14],
            'text-max-width': 6,
            'text-transform': 'uppercase',
            'text-letter-spacing': 0.1,
          },
          paint: {
            'text-color': '#1f2937',
            'text-halo-color': 'rgba(255, 255, 255, 0.85)',
            'text-halo-width': 1.5,
            'text-halo-blur': 0.5,
          },
          minzoom: 1,
        },
        {
          id: 'places_city',
          type: 'symbol',
          source: 'protomaps',
          'source-layer': 'places',
          filter: ['==', 'kind', 'locality'],
          layout: {
            'text-field': ['get', 'name'],
            'text-font': ['Noto Sans Regular'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 3, 10, 8, 14, 14, 18],
            'text-max-width': 8,
          },
          paint: {
            'text-color': '#1f2937',
            'text-halo-color': 'rgba(255, 255, 255, 0.85)',
            'text-halo-width': 1.5,
            'text-halo-blur': 0.5,
          },
          minzoom: 3,
        },
      ],
    }
  : {
      // Raster tile fallback for local development
      version: 8,
      sources: {
        carto: {
          type: 'raster',
          tiles: [
            'https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png',
            'https://b.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png',
            'https://c.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png',
          ],
          tileSize: 256,
          attribution:
            '&copy; <a href="https://carto.com">CARTO</a> | &copy; <a href="https://openstreetmap.org">OSM</a>',
        },
      },
      layers: [
        {
          id: 'carto-light',
          type: 'raster',
          source: 'carto',
          minzoom: 0,
          maxzoom: 19,
          paint: {},
        },
      ],
    }

/**
 * Switch the basemap between its plain look and its weather-overlay look.
 *
 * With weather showing, the water and earth fills are hidden and the
 * background goes dark, so the overlay (inserted below the fills) keeps its
 * full colour; labels turn white with a dark halo. Ocean-only layers keep an
 * opaque land mask, since their data is only valid over water.
 *
 * For raster basemaps (CartoDB fallback), the single raster layer is dimmed
 * instead so weather — inserted *below* it by weatherInsertBeforeId — shows
 * through. Ocean-only weather layers keep the raster fully opaque.
 */
export function setWeatherOverlayOpacity(
  map: maplibregl.Map,
  active: boolean,
  layerName?: string,
): void {
  const style = map.getStyle()
  if (!style?.layers) return
  const oceanOnly = active && layerName != null && OCEAN_ONLY_LAYERS.has(layerName)

  const paint: Array<[layerId: string, property: string, value: unknown]> = [
    ['background', 'background-color', active ? '#0b1018' : '#f0f0f0'],
    ['water', 'fill-opacity', active ? 0 : 1],
    ['earth', 'fill-color', oceanOnly ? '#3b4150' : '#e8e8e8'],
    ['earth', 'fill-opacity', active && !oceanOnly ? 0 : 1],
  ]
  for (const id of LABEL_LAYERS) {
    paint.push(
      [id, 'text-color', active ? '#ffffff' : '#1f2937'],
      [id, 'text-halo-color', active ? 'rgba(0, 0, 0, 0.6)' : 'rgba(255, 255, 255, 0.85)'],
    )
  }
  for (const [id, property, value] of paint) {
    if (map.getLayer(id)) map.setPaintProperty(id, property, value)
  }

  // Raster fallback: weather renders beneath, so dim the raster to let it
  // show through. Ocean-only layers stay opaque (raster carries land).
  for (const layer of style.layers) {
    if (layer.type === 'raster') {
      map.setPaintProperty(layer.id, 'raster-opacity', !active || oceanOnly ? 1 : WEATHER_RASTER_OPACITY)
    }
  }
}
