/** Where the weather layer goes in the MapLibre stack. */
import { expect, test } from '@playwright/test'
import type maplibregl from 'maplibre-gl'
import { weatherInsertBeforeId } from '../src/hooks/layer-order'

const mapWith = (...layers: Array<[string, string]>) =>
  ({ getStyle: () => ({ layers: layers.map(([id, type]) => ({ id, type })) }) }) as unknown as maplibregl.Map

test('weather goes below the first fill', () => {
  expect(weatherInsertBeforeId(mapWith(['bg', 'background'], ['land', 'fill'], ['labels', 'symbol']))).toBe('land')
})

test('weather added after the particles still goes below them', () => {
  // It waits for the colour ramps; the particle layers don't (#82).
  const map = mapWith(['bg', 'background'], ['wind-particles', 'custom'], ['wave-particles', 'custom'], ['land', 'fill'])
  expect(weatherInsertBeforeId(map)).toBe('wind-particles')
})

test('with the raster fallback, weather stays below the raster basemap', () => {
  // There the particles are appended on top.
  const map = mapWith(['carto', 'raster'], ['labels', 'symbol'], ['wind-particles', 'custom'])
  expect(weatherInsertBeforeId(map)).toBe('carto')
})
