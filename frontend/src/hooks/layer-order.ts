/** Where the GL layers go in the MapLibre stack. */

import type maplibregl from 'maplibre-gl'

const PARTICLE_LAYER_IDS = new Set(['wind-particles', 'wave-particles'])

/**
 * Find the layer ID to insert weather before in the MapLibre stack.
 * Same logic as the raster pipeline — inserts before the first fill layer
 * for the Windy.com "weather below semi-transparent basemap" look. Falls back
 * to raster basemap layers, then symbol layers, so weather renders below the
 * basemap even when PMTiles is unavailable.
 *
 * Particle layers may already be there (this layer waits for the colour
 * ramps; they don't): weather goes below them too.
 */
export function weatherInsertBeforeId(map: maplibregl.Map): string | undefined {
  const layers = map.getStyle()?.layers
  if (!layers) return undefined
  let firstRaster: string | undefined
  let firstSymbol: string | undefined
  for (const l of layers) {
    if (PARTICLE_LAYER_IDS.has(l.id)) return firstRaster ?? l.id
    if (l.type === 'fill') return l.id
    if (l.type === 'raster' && !firstRaster) firstRaster = l.id
    if (l.type === 'symbol' && !firstSymbol) firstSymbol = l.id
  }
  return firstRaster ?? firstSymbol
}

/**
 * Insertion point for the particle layers. With the vector basemap they go
 * below the first fill, i.e. directly above the weather layer and under the
 * land mask, lines and labels. The raster fallback has no fills and stays
 * opaque for ocean-only layers, so there particles are appended on top.
 */
export function particleInsertBeforeId(map: maplibregl.Map): string | undefined {
  return map.getStyle()?.layers?.find((l) => l.type === 'fill')?.id
}
