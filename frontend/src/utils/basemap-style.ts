import type maplibregl from 'maplibre-gl'

/**
 * Basemap PMTiles: a low-zoom extract the backend serves same-origin from
 * <data dir>/basemap/ (fetched by scripts/fetch_basemap.py, #70).
 * VITE_BASEMAP_URL can point at another PMTiles archive; any other value
 * (such as the old "raster") is ignored.
 */
const DEFAULT_BASEMAP_URL = '/basemap/basemap.pmtiles'
const CONFIGURED = import.meta.env.VITE_BASEMAP_URL as string | undefined
const RAW_BASEMAP_URL =
  CONFIGURED && (CONFIGURED.endsWith('.pmtiles') || CONFIGURED.startsWith('pmtiles://'))
    ? CONFIGURED
    : DEFAULT_BASEMAP_URL

const PMTILES_SOURCE = RAW_BASEMAP_URL.startsWith('pmtiles://')
  ? RAW_BASEMAP_URL
  : `pmtiles://${RAW_BASEMAP_URL}`

/** Fonts for map text: basemap labels and isobar labels. */
const GLYPHS = 'https://protomaps.github.io/basemaps-assets/fonts/{fontstack}/{range}.pbf'

/** Label layers whose colours flip to white-on-dark when weather is showing. */
const LABEL_LAYERS = ['places_country', 'places_city']

/** Weather layers that only have data over ocean — need opaque earth mask. */
const OCEAN_ONLY_LAYERS = new Set(['wave_height'])

/**
 * Light basemap style optimized for weather overlay readability.
 *
 * Protomaps vector tiles through the PMTiles protocol.
 *
 * Starts as a plain light map. When weather is showing,
 * setWeatherOverlayOpacity() hides the fills and darkens the background so
 * the overlay keeps its full colour and the basemap contributes only thin
 * coastlines, borders and white labels — the Windy.com look.
 */
export const darkBasemapStyle: maplibregl.StyleSpecification = {
      version: 8,
      glyphs: GLYPHS,
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

/**
 * Switch the basemap between its plain look and its weather-overlay look.
 *
 * With weather showing, the water and earth fills are hidden and the
 * background goes dark, so the overlay (inserted below the fills) keeps its
 * full colour; labels turn white with a dark halo. Ocean-only layers keep an
 * opaque land mask, since their data is only valid over water.
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
    // Borders a little stronger over the colour field, so they hold against it.
    ['boundaries', 'line-color', active ? 'rgba(10, 14, 22, 0.6)' : 'rgba(10, 14, 22, 0.45)'],
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
}
