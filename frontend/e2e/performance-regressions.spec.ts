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

test('data tiles are never requested above the pre-generated max zoom', async ({ page }) => {
  await mockPerformanceRoutes(page)
  const zooms = new Set<number>()
  page.on('request', (request) => {
    const match = request.url().match(/\/data\/(\d+)\/\d+\/\d+\.(png|bin)/)
    if (match) zooms.add(Number(match[1]))
  })

  await page.goto('/')
  await expect(page.locator('button').filter({ hasText: 'Wind Speed' })).toBeVisible({ timeout: 10_000 })
  await page.locator('button').filter({ hasText: 'Wind Speed' }).click()
  await waitForWindSettled(page)

  // Map starts at zoom 3; keyboard-zoom to 8, well past the z5 data-tile cap.
  await page.locator('.maplibregl-canvas').focus()
  for (let i = 0; i < 5; i++) {
    await page.keyboard.press('Equal')
    await page.waitForTimeout(500)
  }

  expect(Math.max(...zooms)).toBe(5)
})

test('weather layer draws stand-in tiles while a new zoom level loads', async ({ page }) => {
  await mockPerformanceRoutes(page)
  // Registered last, so it wins for z4 tiles: hold them back long enough to observe the gap.
  await page.route(/\/tiles\/gfs\/.*\/data\/4\/\d+\/\d+\.png/, async (route) => {
    await new Promise((resolve) => setTimeout(resolve, 1_500))
    await route.fulfill({ path: TILE_FIXTURE_PATH })
  })

  const weatherDebug = () => page.evaluate(() => {
    const debugState = (window as unknown as { __weathermanDebug?: Record<string, unknown> }).__weathermanDebug
    return debugState?.weather as { drawn: number; fallback: number } | undefined
  })

  await page.goto('/')
  await expect(page.locator('button').filter({ hasText: 'Temperature' })).toBeVisible({ timeout: 10_000 })
  await page.locator('button').filter({ hasText: 'Temperature' }).click()
  await expect.poll(async () => (await weatherDebug())?.drawn ?? 0, { timeout: 15_000 }).toBeGreaterThan(0)
  expect((await weatherDebug())?.fallback).toBe(0)

  // Zoom 3 → 4: the z4 tiles are still in flight, so every quad must come from a z3 ancestor.
  await page.locator('.maplibregl-canvas').focus()
  await page.keyboard.press('Equal')
  await expect.poll(async () => (await weatherDebug())?.fallback ?? 0).toBeGreaterThan(0)
  const duringGap = await weatherDebug()
  expect(duringGap?.drawn).toBe(duringGap?.fallback)

  // Once the z4 tiles arrive the stand-ins are replaced.
  await expect.poll(async () => (await weatherDebug())?.fallback, { timeout: 10_000 }).toBe(0)
  expect((await weatherDebug())?.drawn).toBeGreaterThan(0)
})

test('zooming out two levels draws cached grandchildren', async ({ page }) => {
  await mockPerformanceRoutes(page)
  // Only z5 ever loads, so after 5 → 3 the nearest cached tiles are two levels down.
  await page.route(/\/tiles\/gfs\/.*\/data\/[34]\/\d+\/\d+\.png/, (route) => route.abort())

  const weatherDebug = () => page.evaluate(() => {
    const debugState = (window as unknown as { __weathermanDebug?: Record<string, unknown> }).__weathermanDebug
    return debugState?.weather as { drawn: number; fallback: number } | undefined
  })

  await page.goto('/')
  await expect(page.locator('button').filter({ hasText: 'Temperature' })).toBeVisible({ timeout: 10_000 })
  await page.locator('button').filter({ hasText: 'Temperature' }).click()

  const canvas = page.locator('.maplibregl-canvas')
  await canvas.focus()
  await page.keyboard.press('Equal')
  await page.waitForTimeout(700)
  await page.keyboard.press('Equal')
  await expect.poll(async () => {
    const state = await weatherDebug()
    return state != null && state.drawn > 0 && state.fallback === 0
  }, { timeout: 15_000 }).toBe(true)

  await page.keyboard.press('Minus')
  await page.waitForTimeout(700)
  await page.keyboard.press('Minus')
  await page.waitForTimeout(1_500)

  const atZoom3 = await weatherDebug()
  expect(atZoom3?.drawn).toBeGreaterThan(0)
  expect(atZoom3?.fallback).toBe(atZoom3?.drawn)
})

test('a failed tile is fetched again and drawn', async ({ page }) => {
  await mockPerformanceRoutes(page)
  // Registered last, so it wins: every data tile fails once, then loads.
  const failedOnce = new Set<string>()
  await page.route(/\/tiles\/gfs\/.*\/data\/\d+\/\d+\/\d+\.png/, async (route) => {
    const url = route.request().url()
    if (failedOnce.has(url)) return route.fallback()
    failedOnce.add(url)
    await route.fulfill({ status: 503, body: 'unavailable' })
  })

  await page.goto('/')
  await expect(page.locator('button').filter({ hasText: 'Temperature' })).toBeVisible({ timeout: 10_000 })
  await page.locator('button').filter({ hasText: 'Temperature' }).click()

  await expect.poll(() => page.evaluate(() => {
    const debugState = (window as unknown as { __weathermanDebug?: Record<string, unknown> }).__weathermanDebug
    return (debugState?.weather as { drawn: number } | undefined)?.drawn ?? 0
  }), { timeout: 15_000 }).toBeGreaterThan(0)
})

test('tile fetches preempted by higher-priority ones are still delivered', async ({ page }) => {
  await mockPerformanceRoutes(page)
  // Slow tiles keep every fetch slot busy, so the wind layer's visible tiles
  // preempt the next-hour tiles already in flight for the initial layer.
  await page.route(/\/tiles\/gfs\/.*\/data\/\d+\/\d+\/\d+\.png/, async (route) => {
    await new Promise((resolve) => setTimeout(resolve, 1_000))
    await route.fulfill({ path: TILE_FIXTURE_PATH }).catch(() => undefined) // aborted meanwhile
  })
  const requested = new Set<string>()
  const delivered = new Set<string>()
  page.on('request', (request) => {
    if (request.url().includes('/data/')) requested.add(request.url())
  })
  page.on('requestfinished', (request) => {
    if (request.url().includes('/data/')) delivered.add(request.url())
  })

  await page.goto('/')
  await expect(page.locator('button').filter({ hasText: 'Wind Speed' })).toBeVisible({ timeout: 10_000 })
  await page.locator('button').filter({ hasText: 'Wind Speed' }).click()
  await waitForWindSettled(page)

  await expect.poll(() => [...requested].filter((url) => !delivered.has(url)).length, { timeout: 20_000 }).toBe(0)
  expect(requested.size).toBeGreaterThan(12)
})
