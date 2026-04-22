import { fileURLToPath } from 'node:url'
import { expect, test, type Page } from '@playwright/test'

const RUN_ID = '20260310T00Z'

const MANIFEST_RESPONSE = {
  schema_version: 1,
  model: 'gfs',
  run_id: RUN_ID,
  cycle_time: '2026-03-10T00:00:00Z',
  published_at: '2026-03-10T02:30:00Z',
  resolution_km: 25,
  layers: [
    {
      id: 'temperature',
      display_name: 'Temperature',
      unit: 'C',
      palette_name: 'temperature',
      value_range: { min: -55, max: 55 },
    },
    {
      id: 'wind_speed',
      display_name: 'Wind Speed',
      unit: 'm/s',
      palette_name: 'wind_speed',
      value_range: { min: 0, max: 50 },
    },
    {
      id: 'wave_height',
      display_name: 'Wave Height',
      unit: 'm',
      palette_name: 'wave_height',
      value_range: { min: 0, max: 15 },
    },
  ],
  forecast_hours: [0, 3, 6],
  tile_url_template: '/tiles/gfs/{run_id}/{layer}/{forecast_hour}/{z}/{x}/{y}.png',
}

const TILE_FIXTURE_PATH = fileURLToPath(new URL('./fixtures/transparent-256.png', import.meta.url))

interface PerformanceRouteOptions {
  delayedWindForecastHour?: number
  delayedWindResponseMs?: number
}

async function mockPerformanceRoutes(page: Page, options: PerformanceRouteOptions = {}) {
  await page.route('**/api/catalog/gfs', (route) =>
    route.fulfill({
      json: {
        model: 'gfs',
        current_run_id: RUN_ID,
        runs: [
          {
            run_id: RUN_ID,
            status: 'published',
            published_at: '2026-03-10T02:30:00Z',
          },
        ],
      },
    }),
  )

  await page.route(`**/api/manifest/gfs/${RUN_ID}`, (route) =>
    route.fulfill({ json: MANIFEST_RESPONSE }),
  )

  await page.route('**/ais/tiles/latest', (route) =>
    route.fulfill({ json: { snapshot_date: '2026-03-10' } }),
  )

  await page.route(/\/ais\/tiles\/\d{4}-\d{2}-\d{2}\/\d+\//, (route) =>
    route.fulfill({ body: Buffer.alloc(0), contentType: 'application/x-protobuf' }),
  )

  await page.route(/\/tiles\/gfs\/.*\/data\/\d+\/\d+\/\d+\.(png|bin)/, async (route) => {
    const url = new URL(route.request().url())
    const [, , , , layer, forecastHour] = url.pathname.split('/')
    if (
      options.delayedWindForecastHour != null &&
      options.delayedWindResponseMs != null &&
      forecastHour === String(options.delayedWindForecastHour) &&
      (layer === 'wind_u' || layer === 'wind_v')
    ) {
      await new Promise((resolve) => setTimeout(resolve, options.delayedWindResponseMs))
    }
    await route.fulfill({
      path: TILE_FIXTURE_PATH,
    })
  })

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

async function waitForWindSettled(page: Page) {
  await page.waitForFunction(() => {
    const debugState = (window as unknown as { __weathermanDebug?: Record<string, unknown> }).__weathermanDebug
    const wind = debugState?.wind as {
      mounts?: number
      atlasClears?: number
      atlasFlushes?: number
      pendingDirtyTiles?: number
    } | undefined
    return Boolean(
      wind &&
      wind.mounts === 1 &&
      ((wind.atlasClears ?? 0) > 0 || (wind.atlasFlushes ?? 0) > 0) &&
      (wind.pendingDirtyTiles ?? 0) === 0,
    )
  })
}

test('wind layer stays mounted and atlas blits stop on a steady viewport', async ({ page }) => {
  await mockPerformanceRoutes(page)
  await page.goto('/')
  await expect(page.locator('button').filter({ hasText: 'Wind Speed' })).toBeVisible({ timeout: 10_000 })

  await page.locator('button').filter({ hasText: 'Wind Speed' }).click()
  await waitForWindSettled(page)

  const initialCounters = await page.evaluate(() => {
    const debugState = (window as unknown as { __weathermanDebug: Record<string, unknown> }).__weathermanDebug
    const wind = debugState.wind as { atlasClears: number; atlasFlushes: number }
    return { atlasClears: wind.atlasClears, atlasFlushes: wind.atlasFlushes }
  })

  await page.waitForTimeout(300)

  const laterCounters = await page.evaluate(() => {
    const debugState = (window as unknown as { __weathermanDebug: Record<string, unknown> }).__weathermanDebug
    const wind = debugState.wind as { atlasClears: number; atlasFlushes: number }
    return { atlasClears: wind.atlasClears, atlasFlushes: wind.atlasFlushes }
  })

  expect(laterCounters).toEqual(initialCounters)

  await page.locator('button').filter({ hasText: 'Temperature' }).click()
  await page.locator('button').filter({ hasText: 'Wind Speed' }).click()

  const mounts = await page.evaluate(() => {
    const debugState = (window as unknown as { __weathermanDebug: Record<string, unknown> }).__weathermanDebug
    const wind = debugState.wind as { mounts: number }
    return wind.mounts
  })

  expect(mounts).toBe(1)
})

test('wave layer stays mounted across visibility toggles', async ({ page }) => {
  await mockPerformanceRoutes(page)
  await page.goto('/')
  await expect(page.locator('button').filter({ hasText: 'Wave Height' })).toBeVisible({ timeout: 10_000 })

  await page.locator('button').filter({ hasText: 'Wave Height' }).click()
  await page.waitForFunction(() => {
    const debugState = (window as unknown as { __weathermanDebug?: Record<string, unknown> }).__weathermanDebug
    const wave = debugState?.wave as { mounts?: number; pendingDirtyTiles?: number } | undefined
    return Boolean(wave && wave.mounts === 1 && (wave.pendingDirtyTiles ?? 0) === 0)
  })

  await page.locator('button').filter({ hasText: 'Temperature' }).click()
  await page.locator('button').filter({ hasText: 'Wave Height' }).click()

  const mounts = await page.evaluate(() => {
    const debugState = (window as unknown as { __weathermanDebug: Record<string, unknown> }).__weathermanDebug
    const wave = debugState.wave as { mounts: number }
    return wave.mounts
  })

  expect(mounts).toBe(1)
})

test('temperature playback advances while particle layers are inactive', async ({ page }) => {
  await mockPerformanceRoutes(page)
  await page.goto('/')
  await expect(page.locator('button').filter({ hasText: 'Temperature' })).toBeVisible({ timeout: 10_000 })

  await page.locator('button').filter({ hasText: 'Temperature' }).click()
  await page.locator('button').filter({ hasText: '▶' }).click()

  await page.waitForFunction(() => {
    const slider = document.querySelector('input[type="range"]') as HTMLInputElement | null
    return slider?.value === '1'
  }, undefined, { timeout: 5_000 })

  await page.locator('button').filter({ hasText: '⏸' }).click()
})

test('wind atlas ignores delayed loads from a scrubbed-away forecast hour', async ({ page }) => {
  await mockPerformanceRoutes(page, {
    delayedWindForecastHour: 3,
    delayedWindResponseMs: 750,
  })
  await page.goto('/')
  await expect(page.locator('button').filter({ hasText: 'Wind Speed' })).toBeVisible({ timeout: 10_000 })

  await page.locator('button').filter({ hasText: 'Wind Speed' }).click()
  await waitForWindSettled(page)

  const initialCounters = await page.evaluate(() => {
    const debugState = (window as unknown as { __weathermanDebug: Record<string, unknown> }).__weathermanDebug
    const wind = debugState.wind as { atlasClears: number; atlasFlushes: number }
    return { atlasClears: wind.atlasClears, atlasFlushes: wind.atlasFlushes }
  })

  const slider = page.locator('input[type="range"]')
  await expect(slider).toHaveValue('0')

  await page.locator('button').filter({ hasText: '❯' }).click()
  await expect(slider).toHaveValue('1')
  await page.locator('button').filter({ hasText: '❮' }).click()
  await expect(slider).toHaveValue('0')
  await page.waitForFunction((expectedAtlasClears) => {
    const debugState = (window as unknown as { __weathermanDebug?: Record<string, unknown> }).__weathermanDebug
    const wind = debugState?.wind as {
      atlasClears?: number
      pendingDirtyTiles?: number
    } | undefined
    return Boolean(
      wind &&
      (wind.atlasClears ?? 0) >= expectedAtlasClears &&
      (wind.pendingDirtyTiles ?? 0) === 0,
    )
  }, initialCounters.atlasClears + 2)

  const countersBeforeLateLoads = await page.evaluate(() => {
    const debugState = (window as unknown as { __weathermanDebug: Record<string, unknown> }).__weathermanDebug
    const wind = debugState.wind as { atlasClears: number; atlasFlushes: number }
    return { atlasClears: wind.atlasClears, atlasFlushes: wind.atlasFlushes }
  })

  await page.waitForTimeout(1_000)

  const countersAfterLateLoads = await page.evaluate(() => {
    const debugState = (window as unknown as { __weathermanDebug: Record<string, unknown> }).__weathermanDebug
    const wind = debugState.wind as { atlasClears: number; atlasFlushes: number }
    return { atlasClears: wind.atlasClears, atlasFlushes: wind.atlasFlushes }
  })

  expect(countersAfterLateLoads).toEqual(countersBeforeLateLoads)
})
