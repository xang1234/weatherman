/** Shared mock data and route helpers for e2e tests. */

import type { Page } from '@playwright/test'

export const RUN_ID = '20260310_00z'
export const CYCLE_TIME = '2026-03-10T00:00:00Z'
export const FORECAST_HOURS = [0, 3, 6, 9, 12]
export const AIS_DATE = '2026-03-10'

export const CATALOG_RESPONSE = {
  model: 'gfs',
  current_run_id: RUN_ID,
  runs: [
    {
      run_id: RUN_ID,
      status: 'published',
      published_at: '2026-03-10T02:30:00Z',
    },
  ],
}

export const MANIFEST_RESPONSE = {
  schema_version: 1,
  model: 'gfs',
  run_id: RUN_ID,
  cycle_time: CYCLE_TIME,
  published_at: '2026-03-10T02:30:00Z',
  resolution_km: 25,
  layers: [
    {
      id: 'temperature_2m',
      display_name: 'Temperature (2m)',
      unit: 'K',
      palette_name: 'temperature',
      value_range: { min: 220, max: 320 },
    },
    {
      id: 'wind_speed_10m',
      display_name: 'Wind Speed (10m)',
      unit: 'm/s',
      palette_name: 'wind',
      value_range: { min: 0, max: 50 },
    },
  ],
  forecast_hours: FORECAST_HOURS,
  tile_url_template: '/tiles/gfs/{run_id}/{layer}/{forecast_hour}/{z}/{x}/{y}.png',
}

export const AIS_LATEST_RESPONSE = {
  snapshot_date: AIS_DATE,
}

export function makeCoverageJSON(lon: number, lat: number) {
  return {
    domain: {
      axes: {
        x: { values: [lon] },
        y: { values: [lat] },
        t: { values: FORECAST_HOURS },
      },
    },
    parameters: {
      temperature_2m: {
        observedProperty: { label: { en: 'Temperature (2m)' } },
        unit: { symbol: 'K' },
      },
      wind_speed_10m: {
        observedProperty: { label: { en: 'Wind Speed (10m)' } },
        unit: { symbol: 'm/s' },
      },
    },
    ranges: {
      temperature_2m: {
        values: [280.5, 281.2, 282.0, 281.5, 280.8],
      },
      wind_speed_10m: {
        values: [5.3, 6.1, 7.8, 8.2, 6.5],
      },
    },
  }
}

/**
 * Install route mocks for all API endpoints the app calls.
 * Call before navigating to the page.
 */
export async function mockApiRoutes(page: Page) {
  // Catalog
  await page.route('**/api/catalog/gfs', (route) =>
    route.fulfill({ json: CATALOG_RESPONSE }),
  )

  // Manifest
  await page.route(`**/api/manifest/gfs/${RUN_ID}`, (route) =>
    route.fulfill({ json: MANIFEST_RESPONSE }),
  )

  // AIS latest
  await page.route('**/ais/tiles/latest', (route) =>
    route.fulfill({ json: AIS_LATEST_RESPONSE }),
  )

  // AIS tile requests — return empty PBF
  await page.route(/\/ais\/tiles\/\d{4}-\d{2}-\d{2}\/\d+\//, (route) =>
    route.fulfill({ body: Buffer.alloc(0), contentType: 'application/x-protobuf' }),
  )

  // Weather TileJSON — return a minimal tile spec
  await page.route('**/tiles/gfs/**/tilejson.json*', (route) => {
    const origin = new URL(route.request().url()).origin
    return route.fulfill({
      json: {
        tilejson: '3.0.0',
        tiles: [`${origin}/tiles/gfs/mock/{z}/{x}/{y}.png`],
        minzoom: 0,
        maxzoom: 8,
      },
    })
  })

  // Weather tile images — return 1x1 transparent PNG
  await page.route(/\/tiles\/gfs\/.*\/\d+\/\d+\/\d+\.png/, (route) =>
    route.fulfill({
      body: TRANSPARENT_PNG,
      contentType: 'image/png',
    }),
  )

  // EDR position endpoint (weather inspector)
  await page.route('**/v1/edr/collections/gfs/instances/*/position*', (route) => {
    const url = new URL(route.request().url())
    const coords = url.searchParams.get('coords') ?? ''
    const match = coords.match(/POINT\(([^ ]+) ([^ ]+)\)/)
    const lon = match ? parseFloat(match[1]) : 0
    const lat = match ? parseFloat(match[2]) : 0
    return route.fulfill({ json: makeCoverageJSON(lon, lat) })
  })

  // SSE — return an open response that stays connected
  await page.route('**/events/stream', (route) =>
    route.fulfill({
      status: 200,
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
      },
      body: ':ok\n\n',
    }),
  )
}

// 1x1 transparent PNG (67 bytes)
const TRANSPARENT_PNG = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVQI12NgAAIABQAB' +
    'Nl7BcQAAAABJRU5ErkJggg==',
  'base64',
)
