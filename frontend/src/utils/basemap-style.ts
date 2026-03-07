import type maplibregl from 'maplibre-gl'

/**
 * Default PMTiles basemap URL (Protomaps daily build).
 * Override via VITE_BASEMAP_URL env var for local/offline use.
 */
const RAW_BASEMAP_URL =
  import.meta.env.VITE_BASEMAP_URL ||
  'https://build.protomaps.com/20250311.pmtiles'

const PMTILES_SOURCE = RAW_BASEMAP_URL.startsWith('pmtiles://')
  ? RAW_BASEMAP_URL
  : `pmtiles://${RAW_BASEMAP_URL}`

/**
 * Dark basemap style optimized for weather overlay readability.
 *
 * Uses Protomaps vector tiles via PMTiles protocol. The dark palette
 * ensures colorful weather layers (wind, pressure, precip) have
 * strong contrast against the base.
 */
export const darkBasemapStyle: maplibregl.StyleSpecification = {
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
    // Background
    {
      id: 'background',
      type: 'background',
      paint: { 'background-color': '#0d1117' },
    },
    // Water (oceans, seas, lakes)
    {
      id: 'water',
      type: 'fill',
      source: 'protomaps',
      'source-layer': 'water',
      paint: { 'fill-color': '#0a1628' },
    },
    // Land
    {
      id: 'earth',
      type: 'fill',
      source: 'protomaps',
      'source-layer': 'earth',
      paint: { 'fill-color': '#151b23' },
    },
    // Landuse (parks, forests, etc.)
    {
      id: 'landuse_park',
      type: 'fill',
      source: 'protomaps',
      'source-layer': 'landuse',
      filter: [
        'any',
        ['==', 'pmap:kind', 'park'],
        ['==', 'pmap:kind', 'forest'],
        ['==', 'pmap:kind', 'nature_reserve'],
      ],
      paint: { 'fill-color': '#121a14', 'fill-opacity': 0.6 },
    },
    // Boundaries (country borders)
    {
      id: 'boundaries',
      type: 'line',
      source: 'protomaps',
      'source-layer': 'boundaries',
      filter: ['==', 'pmap:kind', 'country'],
      paint: {
        'line-color': '#2d333b',
        'line-width': 1,
        'line-dasharray': [3, 2],
      },
    },
    // Major roads (minimal, for context only)
    {
      id: 'roads_major',
      type: 'line',
      source: 'protomaps',
      'source-layer': 'roads',
      filter: [
        'any',
        ['==', 'pmap:kind', 'highway'],
        ['==', 'pmap:kind', 'major_road'],
      ],
      paint: {
        'line-color': '#1c2128',
        'line-width': ['interpolate', ['linear'], ['zoom'], 5, 0.5, 12, 2],
      },
      minzoom: 6,
    },
    // Place labels (cities, towns)
    {
      id: 'places_city',
      type: 'symbol',
      source: 'protomaps',
      'source-layer': 'places',
      filter: ['==', 'pmap:kind', 'locality'],
      layout: {
        'text-field': '{name}',
        'text-font': ['Noto Sans Regular'],
        'text-size': ['interpolate', ['linear'], ['zoom'], 3, 10, 8, 14],
        'text-max-width': 8,
      },
      paint: {
        'text-color': '#6e7681',
        'text-halo-color': '#0d1117',
        'text-halo-width': 1.5,
      },
      minzoom: 3,
    },
    // Country labels
    {
      id: 'places_country',
      type: 'symbol',
      source: 'protomaps',
      'source-layer': 'places',
      filter: ['==', 'pmap:kind', 'country'],
      layout: {
        'text-field': '{name}',
        'text-font': ['Noto Sans Regular'],
        'text-size': ['interpolate', ['linear'], ['zoom'], 1, 10, 5, 14],
        'text-max-width': 6,
        'text-transform': 'uppercase',
        'text-letter-spacing': 0.1,
      },
      paint: {
        'text-color': '#484f58',
        'text-halo-color': '#0d1117',
        'text-halo-width': 1.5,
      },
      minzoom: 1,
    },
  ],
}
