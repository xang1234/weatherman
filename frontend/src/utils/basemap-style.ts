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

/**
 * Dark basemap style optimized for weather overlay readability.
 *
 * Uses Protomaps vector tiles via PMTiles protocol when available,
 * falling back to CartoDB dark_all raster tiles for local development.
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
          paint: { 'background-color': '#0d1117' },
        },
        {
          id: 'water',
          type: 'fill',
          source: 'protomaps',
          'source-layer': 'water',
          paint: { 'fill-color': '#081a30' },
        },
        {
          id: 'earth',
          type: 'fill',
          source: 'protomaps',
          'source-layer': 'earth',
          paint: { 'fill-color': '#1c2333' },
        },
        {
          id: 'boundaries',
          type: 'line',
          source: 'protomaps',
          'source-layer': 'boundaries',
          paint: {
            'line-color': '#4a5568',
            'line-width': ['interpolate', ['linear'], ['zoom'], 1, 0.5, 6, 1.5],
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
            'text-color': '#7a8494',
            'text-halo-color': '#0d1117',
            'text-halo-width': 2,
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
            'text-color': '#9ca3b0',
            'text-halo-color': '#0d1117',
            'text-halo-width': 2,
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
            'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png',
            'https://b.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png',
            'https://c.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png',
          ],
          tileSize: 256,
          attribution:
            '&copy; <a href="https://carto.com">CARTO</a> | &copy; <a href="https://openstreetmap.org">OSM</a>',
        },
      },
      layers: [
        {
          id: 'carto-dark',
          type: 'raster',
          source: 'carto',
          minzoom: 0,
          maxzoom: 19,
        },
      ],
    }
