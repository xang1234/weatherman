import type maplibregl from 'maplibre-gl'

/**
 * Default PMTiles basemap URL (Protomaps daily build).
 * Override via VITE_BASEMAP_URL env var for local/offline use.
 */
const RAW_BASEMAP_URL =
  import.meta.env.VITE_BASEMAP_URL ||
  'https://build.protomaps.com/20260301.pmtiles'

const PMTILES_SOURCE = RAW_BASEMAP_URL.startsWith('pmtiles://')
  ? RAW_BASEMAP_URL
  : `pmtiles://${RAW_BASEMAP_URL}`

/**
 * Whether to use PMTiles (production) or a raster tile fallback (local dev).
 * Set VITE_BASEMAP_URL to a valid PMTiles URL to use vector tiles.
 */
const USE_PMTILES = !!import.meta.env.VITE_BASEMAP_URL

/** Fill-layer opacity when weather overlay is active (Windy.com style). */
const WEATHER_FILL_OPACITY = 0.3

/** Layer IDs whose fill-opacity should be reduced for weather transparency. */
const TRANSPARENT_FILL_LAYERS = new Set(['water', 'earth'])

/**
 * Light basemap style optimized for weather overlay readability.
 *
 * Uses Protomaps vector tiles via PMTiles protocol when available,
 * falling back to CartoDB light raster tiles for local development.
 *
 * Fill layers (earth, water) start opaque but are dynamically reduced to
 * ~30% opacity via setWeatherOverlayOpacity() when weather data is visible —
 * the Windy.com "semi-transparent basemap" look. Labels use stronger halos
 * for readability over weather colors.
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
            'line-color': '#000000',
            'line-width': ['interpolate', ['linear'], ['zoom'], 0, 0.8, 4, 1.5, 8, 2, 14, 3],
          },
        },
        {
          id: 'boundaries',
          type: 'line',
          source: 'protomaps',
          'source-layer': 'boundaries',
          paint: {
            'line-color': '#1a1a1a',
            'line-width': ['interpolate', ['linear'], ['zoom'], 1, 0.8, 6, 2, 10, 2.5],
            'line-dasharray': [3, 2],
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
            'text-halo-width': 2.5,
            'text-halo-blur': 1,
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
            'text-halo-width': 2.5,
            'text-halo-blur': 1,
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
 * Set the fill-layer opacity for weather-transparent layers at runtime.
 *
 * Call this when toggling weather overlay on/off — restores full opacity
 * when weather is hidden so the basemap looks normal without weather.
 * Raster basemaps (CartoDB fallback) stay at full opacity since weather
 * renders on top — dimming pre-rendered tiles would reduce label readability.
 */
export function setWeatherOverlayOpacity(
  map: maplibregl.Map,
  active: boolean,
): void {
  const style = map.getStyle()
  if (!style?.layers) return
  for (const layer of style.layers) {
    if (layer.type === 'fill' && TRANSPARENT_FILL_LAYERS.has(layer.id)) {
      map.setPaintProperty(layer.id, 'fill-opacity', active ? WEATHER_FILL_OPACITY : 1)
    }
  }
}
