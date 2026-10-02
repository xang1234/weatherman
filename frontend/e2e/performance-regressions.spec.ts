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
  /** Hold back wind U/V data tiles for one forecast hour. */
  delayedForecastHour?: number
  delayedResponseMs?: number
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
      options.delayedForecastHour != null &&
      options.delayedResponseMs != null &&
      forecastHour === String(options.delayedForecastHour) &&
      (layer === 'wind_u' || layer === 'wind_v')
    ) {
      await new Promise((resolve) => setTimeout(resolve, options.delayedResponseMs))
    }
    await route.fulfill({
      path: TILE_FIXTURE_PATH,
    })
  })

  // No basemap: weather draws without it, and the tests stay off the network.
  await page.route('**/basemap/**', (route) => route.fulfill({ status: 404 }))

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

  const atlasCounters = () => page.evaluate(() => {
    const debugState = (window as unknown as { __weathermanDebug: Record<string, unknown> }).__weathermanDebug
    const wind = debugState.wind as { atlasClears: number; atlasFlushes: number }
    return { atlasClears: wind.atlasClears, atlasFlushes: wind.atlasFlushes }
  })

  // Tiles arrive over several frames, each one flushing into the atlas, so
  // "settled" can be true between two arrivals. The blits must stop: wait for
  // a 300 ms window in which neither counter moves.
  await expect.poll(async () => {
    const before = await atlasCounters()
    await page.waitForTimeout(300)
    return JSON.stringify(await atlasCounters()) === JSON.stringify(before)
  }, { timeout: 10_000 }).toBe(true)

  await page.locator('button').filter({ hasText: 'Temperature' }).click()
  await page.locator('button').filter({ hasText: 'Wind Speed' }).click()

  const mounts = await page.evaluate(() => {
    const debugState = (window as unknown as { __weathermanDebug: Record<string, unknown> }).__weathermanDebug
    const wind = debugState.wind as { mounts: number }
    return wind.mounts
  })

  expect(mounts).toBe(1)
})

test('the weather tile pass does not rerun on a steady view while particles animate', async ({ page }) => {
  await mockPerformanceRoutes(page)
  await page.goto('/')
  await expect(page.locator('button').filter({ hasText: 'Wind Speed' })).toBeVisible({ timeout: 10_000 })
  await page.locator('button').filter({ hasText: 'Wind Speed' }).click()
  await waitForWindSettled(page)

  const weatherCounters = () => page.evaluate(() => {
    const debugState = (window as unknown as { __weathermanDebug: Record<string, unknown> }).__weathermanDebug
    const weather = debugState.weather as { tilePasses: number; composites: number }
    return { tilePasses: weather.tilePasses, composites: weather.composites }
  })

  // Wait out the tile arrivals: a 300 ms window with no tile pass.
  await expect.poll(async () => {
    const before = await weatherCounters()
    await page.waitForTimeout(300)
    return (await weatherCounters()).tilePasses === before.tilePasses
  }, { timeout: 10_000 }).toBe(true)

  // The particles keep the map repainting; the colour layer only composites (#42).
  const steadyStart = await weatherCounters()
  await page.waitForTimeout(1_000)
  const steadyEnd = await weatherCounters()
  expect(steadyEnd.composites - steadyStart.composites).toBeGreaterThan(5)
  expect(steadyEnd.tilePasses).toBe(steadyStart.tilePasses)

  // Moving the map redraws the tiles.
  const canvas = page.locator('canvas.maplibregl-canvas')
  await canvas.hover()
  await page.mouse.down()
  await page.mouse.move(400, 300, { steps: 5 })
  await page.mouse.up()
  await expect.poll(async () => (await weatherCounters()).tilePasses).toBeGreaterThan(steadyEnd.tilePasses)
})

test('weather is drawn while the basemap is still loading', async ({ page }) => {
  await mockPerformanceRoutes(page)
  // Basemap tiles that never arrive used to hold the whole app behind
  // "Loading map..." (#51). Covers each basemap the build can use: the default
  // Protomaps PMTiles, the dev proxy to it, and the CARTO raster fallback.
  await page.route(/build\.protomaps\.com|\/basemap\/|basemaps\.cartocdn\.com/, () => new Promise(() => {}))
  await page.goto('/')

  await page.waitForFunction(() => {
    const debugState = (window as unknown as { __weathermanDebug?: Record<string, unknown> }).__weathermanDebug
    return ((debugState?.weather as { drawn?: number } | undefined)?.drawn ?? 0) > 0
  }, undefined, { timeout: 10_000 })
  await expect(page.getByText('Loading map...')).toHaveCount(0)
})

test('wind particle count follows the viewport area, not the pixel density', async ({ browser }) => {
  // One particle per 250 CSS px² (#40). Small viewports keep SwiftShader's
  // low-tier cap (2,304) out of the way.
  const drawn = async (width: number, height: number, deviceScaleFactor: number) => {
    const { baseURL } = test.info().project.use
    const page = await browser.newPage({ baseURL, viewport: { width, height }, deviceScaleFactor })
    await mockPerformanceRoutes(page)
    await page.goto('/')
    await expect(page.locator('button').filter({ hasText: 'Wind Speed' })).toBeVisible({ timeout: 10_000 })
    await page.locator('button').filter({ hasText: 'Wind Speed' }).click()
    const handle = await page.waitForFunction(() => {
      const debugState = (window as unknown as { __weathermanDebug?: Record<string, unknown> }).__weathermanDebug
      return (debugState?.wind as { drawnParticles?: number } | undefined)?.drawnParticles
    })
    const count = await handle.jsonValue()
    await page.close()
    return count
  }

  expect(await drawn(640, 400, 1)).toBe(1024)
  expect(await drawn(640, 400, 2)).toBe(1024)
  expect(await drawn(400, 320, 1)).toBe(512)
})

test('overlays: wind particles over temperature, each layer with its own opacity', async ({ page }) => {
  await mockPerformanceRoutes(page)
  await page.goto('/')
  await expect(page.locator('button').filter({ hasText: 'Temperature' })).toBeVisible({ timeout: 10_000 })
  await page.locator('button').filter({ hasText: 'Temperature' }).click()

  const debug = () => page.evaluate(() => {
    const state = (window as unknown as { __weathermanDebug?: Record<string, { active?: boolean; opacity?: number }> }).__weathermanDebug
    return { wind: state?.wind, waves: state?.wave, weather: state?.weather }
  })
  const windToggle = page.getByLabel('Wind particles', { exact: true })
  const wavesToggle = page.getByLabel('Wave dashes', { exact: true })

  // By default an overlay follows the colour layer: none over temperature (#20).
  await expect(windToggle).not.toBeChecked()
  await expect(wavesToggle).not.toBeChecked()
  await expect.poll(async () => (await debug()).wind?.active).toBe(false)

  // Wind particles stacked over the temperature field, with their own opacity.
  await windToggle.check()
  await expect.poll(async () => (await debug()).wind?.active).toBe(true)
  await page.getByLabel('Wind particles opacity').fill('0.3')
  await expect.poll(async () => (await debug()).wind?.opacity).toBeCloseTo(0.3)

  // The colour layer's opacity is separate.
  await page.getByLabel('Opacity', { exact: true }).fill('0.5')
  await expect.poll(async () => (await debug()).weather?.opacity).toBeCloseTo(0.5)
  expect((await debug()).wind?.opacity).toBeCloseTo(0.3)

  // A toggled overlay stays as set when the colour layer changes.
  await page.locator('button').filter({ hasText: 'Wave Height' }).click()
  await expect(windToggle).toBeChecked()
  await expect(wavesToggle).toBeChecked() // still following: dashes with wave height
  await expect.poll(async () => (await debug()).waves?.active).toBe(true)

  await windToggle.uncheck()
  await expect.poll(async () => (await debug()).wind?.active).toBe(false)
})

test('an overlay re-enabled during playback picks up the current hour', async ({ page }) => {
  // Hours 3 and 6 are held back so playback parks on hour 3.
  await mockPerformanceRoutes(page)
  const release: Record<string, () => void> = {}
  const released = Object.fromEntries(['3', '6'].map((hour) =>
    [hour, new Promise<void>((resolve) => { release[hour] = resolve })]))
  await page.route(/\/tiles\/gfs\/[^/]+\/[^/]+\/(3|6)\/data\//, async (route) => {
    await released[new URL(route.request().url()).pathname.split('/')[5]]
    await route.fulfill({ path: TILE_FIXTURE_PATH })
  })
  await page.goto('/')
  await expect(page.locator('button').filter({ hasText: 'Temperature' })).toBeVisible({ timeout: 10_000 })
  await page.locator('button').filter({ hasText: 'Temperature' }).click()

  const windHour = () => page.evaluate(() => {
    const state = (window as unknown as { __weathermanDebug?: Record<string, { hour?: number }> }).__weathermanDebug
    return state?.wind?.hour
  })
  const windToggle = page.getByLabel('Wind particles', { exact: true })
  await windToggle.check()
  await expect.poll(windHour).toBe(0)
  await windToggle.uncheck()

  await page.locator('button').filter({ hasText: '▶' }).click()
  release['3']()
  await expect(page.locator('input[aria-label="Forecast hour"]')).toHaveValue('1', { timeout: 10_000 })

  // Parked on hour 3 (hour 6 is held). Wind comes back on: it must show
  // hour 3, not the hour 0 it had when switched off.
  await windToggle.check()
  await expect.poll(windHour, { timeout: 5_000 }).toBe(3)

  await page.locator('button').filter({ hasText: '⏸' }).click()
  release['6']()
})

test('isobars overlay loads the shown hour and prefetches the next', async ({ page }) => {
  await mockPerformanceRoutes(page)
  const requested: string[] = []
  await page.route(/\/api\/contours\/gfs\/[^/]+\/prmsl\/\d+$/, (route) => {
    requested.push(new URL(route.request().url()).pathname.split('/').pop()!)
    return route.fulfill({
      json: {
        type: 'FeatureCollection',
        features: [
          { type: 'Feature', geometry: { type: 'LineString', coordinates: [[-40, 30], [-20, 35]] }, properties: { kind: 'isobar', hpa: 1012 } },
          { type: 'Feature', geometry: { type: 'Point', coordinates: [-30, 40] }, properties: { kind: 'low', hpa: 996 } },
        ],
      },
    })
  })
  await page.goto('/')
  const isobars = page.getByLabel('Isobars', { exact: true })
  await expect(isobars).toBeVisible({ timeout: 10_000 })

  // Off by default: nothing fetched (#23).
  await expect(isobars).not.toBeChecked()
  await page.waitForTimeout(500)
  expect(requested).toEqual([])

  await isobars.check()
  await expect.poll(() => [...requested].sort()).toEqual(['0', '3'])

  await page.locator('button').filter({ hasText: '❯' }).click()
  await expect.poll(() => [...requested].sort()).toEqual(['0', '3', '6'])
})

test('isobars on the last hour prefetch the first, where playback wraps to', async ({ page }) => {
  await mockPerformanceRoutes(page)
  const requested: string[] = []
  await page.route(/\/api\/contours\/gfs\/[^/]+\/prmsl\/\d+$/, (route) => {
    requested.push(new URL(route.request().url()).pathname.split('/').pop()!)
    return route.fulfill({ json: { type: 'FeatureCollection', features: [] } })
  })
  await page.goto('/?fh=6')
  const isobars = page.getByLabel('Isobars', { exact: true })
  await expect(isobars).toBeVisible({ timeout: 10_000 })
  await expect(page.locator('input[aria-label="Forecast hour"]')).toHaveValue('2')

  await isobars.check()
  await expect.poll(() => [...requested].sort()).toEqual(['0', '6'])
})

test('isobars are cleared while a jumped-to hour loads', async ({ page }) => {
  await mockPerformanceRoutes(page)
  let releaseHour6!: () => void
  const hour6Released = new Promise<void>((resolve) => { releaseHour6 = resolve })
  const line = (hpa: number) => ({
    type: 'Feature', geometry: { type: 'LineString', coordinates: [[-40, 30], [-20, 35]] }, properties: { kind: 'isobar', hpa },
  })
  await page.route(/\/api\/contours\/gfs\/[^/]+\/prmsl\/\d+$/, async (route) => {
    const hour = new URL(route.request().url()).pathname.split('/').pop()!
    if (hour === '6') await hour6Released
    // Hour 0 has two isobars, the others one, so the counts tell them apart.
    return route.fulfill({ json: { type: 'FeatureCollection', features: hour === '0' ? [line(1012), line(1016)] : [line(1008)] } })
  })
  await page.goto('/')
  const isobars = page.getByLabel('Isobars', { exact: true })
  await expect(isobars).toBeVisible({ timeout: 10_000 })
  const shown = () => page.evaluate(() =>
    (window as unknown as { __weathermanDebug?: { isobars?: { features: number } } }).__weathermanDebug?.isobars?.features)

  await isobars.check()
  await expect.poll(shown).toBe(2)

  // Jump past the prefetched hour 3 to hour 6, which is held back: hour 0's
  // isobars must not stay up under hour 6's label.
  await page.locator('input[aria-label="Forecast hour"]').fill('2')
  await expect.poll(shown).toBe(0)
  releaseHour6()
  await expect.poll(shown).toBe(1)
})

test('isobars retry an hour that failed with a server error', async ({ page }) => {
  await mockPerformanceRoutes(page)
  const requested: string[] = []
  await page.route(/\/api\/contours\/gfs\/[^/]+\/prmsl\/\d+$/, (route) => {
    const hour = new URL(route.request().url()).pathname.split('/').pop()!
    requested.push(hour)
    // The first request for hour 0 fails; a 503 must not be remembered as "no isobars".
    if (hour === '0' && requested.filter((h) => h === '0').length === 1) return route.fulfill({ status: 503 })
    return route.fulfill({ json: { type: 'FeatureCollection', features: [] } })
  })
  await page.goto('/')
  const isobars = page.getByLabel('Isobars', { exact: true })
  await expect(isobars).toBeVisible({ timeout: 10_000 })

  await isobars.check()
  await expect.poll(() => requested.filter((h) => h === '0').length).toBe(1)
  await isobars.uncheck()
  await isobars.check()
  await expect.poll(() => requested.filter((h) => h === '0').length).toBe(2)
})

test('wind colour layer and particles fetch each tile once between them', async ({ page }) => {
  await mockPerformanceRoutes(page)
  await page.goto('/')
  await expect(page.locator('button').filter({ hasText: 'Wind Speed' })).toBeVisible({ timeout: 10_000 })
  await page.locator('button').filter({ hasText: 'Wind Speed' }).click()
  await waitForWindSettled(page)

  // Counted where fetches start, in the shared tile store: network events
  // from the tile worker can be missed when the page starts fast, and the
  // worker may abort a prefetch and send it again (preemption, not a
  // duplicate fetch).
  const fetches = await page.evaluate(() =>
    (window as unknown as { __weathermanDebug: { tiles?: { fetches: Record<string, number> } } })
      .__weathermanDebug.tiles?.fetches ?? {})
  const wind = Object.entries(fetches).filter(([url]) => /\/wind_[uv]\/\d+\/data\//.test(url))

  // Both layers draw wind U/V tiles; they used to fetch, decode and upload
  // each one separately (#43).
  expect(wind.length).toBeGreaterThan(0)
  expect(wind.filter(([, count]) => count > 1)).toEqual([])
})

test('dragging the forecast slider blends between hours, and settles on release', async ({ page }) => {
  await mockPerformanceRoutes(page)
  await page.goto('/')
  await expect(page.locator('button').filter({ hasText: 'Temperature' })).toBeVisible({ timeout: 10_000 })
  await page.locator('button').filter({ hasText: 'Temperature' }).click()
  const weather = () => page.evaluate(() => {
    const state = (window as unknown as { __weathermanDebug?: Record<string, { hour?: number; mix?: number }> }).__weathermanDebug
    return { hour: state?.weather?.hour, mix: state?.weather?.mix ?? 0 }
  })
  await expect.poll(async () => (await weather()).hour).toBe(0)

  const slider = page.locator('input[aria-label="Forecast hour"]')
  const label = page.getByText(/^\w{3}, \w{3} \d{2}, \d{2}:\d{2}$/)
  const box = (await slider.boundingBox())!
  const thumb = 8 // half the thumb: the track's ends are inset by it
  const xAt = (position: number) => box.x + thumb + (position / 2) * (box.width - 2 * thumb)

  // Hours are [0, 3, 6]. Drag to 40 % of the way from hour 0 to hour 3 (#24).
  await page.mouse.move(xAt(0), box.y + box.height / 2)
  await page.mouse.down()
  await page.mouse.move(xAt(0.4), box.y + box.height / 2, { steps: 5 })
  await expect.poll(async () => (await weather()).mix).toBeGreaterThan(0.2)
  const during = await weather()
  expect(during.hour).toBe(0)
  expect(during.mix).toBeLessThan(0.6)
  // The label shows the time in between (about 01:12), not either hour.
  await expect(label).not.toHaveText(/00:00$|03:00$/)

  await page.mouse.up()
  await expect(slider).toHaveValue('0')
  await expect.poll(async () => (await weather()).mix).toBe(0)
  await expect(label).toHaveText(/00:00$/)

  // Arrow keys still step whole hours.
  await slider.focus()
  await page.keyboard.press('ArrowRight')
  await expect(slider).toHaveValue('1')
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

test('wave dashes cover a viewport with more grid cells than dash slots', async ({ page }) => {
  // SwiftShader is the low tier: 1,600 slots. At 24 px a 1920x1080 viewport
  // needs 80x45 = 3,600 cells, which used to leave the bottom rows empty (#35).
  await page.setViewportSize({ width: 1920, height: 1080 })
  await mockPerformanceRoutes(page)
  await page.goto('/')
  await expect(page.locator('button').filter({ hasText: 'Wave Height' })).toBeVisible({ timeout: 10_000 })
  await page.locator('button').filter({ hasText: 'Wave Height' }).click()

  await page.waitForFunction(() => {
    const debugState = (window as unknown as { __weathermanDebug?: Record<string, unknown> }).__weathermanDebug
    const wave = debugState?.wave as { mounts?: number; gridTruncated?: boolean } | undefined
    return Boolean(wave && wave.mounts === 1 && wave.gridTruncated !== undefined)
  })
  const truncated = await page.evaluate(() => {
    const debugState = (window as unknown as { __weathermanDebug: Record<string, unknown> }).__weathermanDebug
    return (debugState.wave as { gridTruncated: boolean }).gridTruncated
  })
  expect(truncated).toBe(false)
})

test('temperature playback advances while particle layers are inactive', async ({ page }) => {
  await mockPerformanceRoutes(page)
  await page.goto('/')
  await expect(page.locator('button').filter({ hasText: 'Temperature' })).toBeVisible({ timeout: 10_000 })

  await page.locator('button').filter({ hasText: 'Temperature' }).click()
  await page.locator('button').filter({ hasText: '▶' }).click()

  await page.waitForFunction(() => {
    const slider = document.querySelector('input[aria-label="Forecast hour"]') as HTMLInputElement | null
    return slider?.value === '1'
  }, undefined, { timeout: 5_000 })

  await page.locator('button').filter({ hasText: '⏸' }).click()
})

test('pausing after a slow step shows the hour on the slider', async ({ page }) => {
  // Hours 3 and 6 are held back until the test releases them. Playback used
  // to step the slider to hour 3 after 1.2 s anyway, leaving the map on hour
  // 0 if paused there (#36).
  await mockPerformanceRoutes(page)
  const release: Record<string, () => void> = {}
  const released = Object.fromEntries(['3', '6'].map((hour) =>
    [hour, new Promise<void>((resolve) => { release[hour] = resolve })]))
  await page.route(/\/tiles\/gfs\/.*\/temperature\/(3|6)\/data\//, async (route) => {
    await released[new URL(route.request().url()).pathname.split('/')[5]]
    await route.fulfill({ path: TILE_FIXTURE_PATH })
  })
  await page.goto('/')
  await expect(page.locator('button').filter({ hasText: 'Temperature' })).toBeVisible({ timeout: 10_000 })
  await page.locator('button').filter({ hasText: 'Temperature' }).click()

  const weatherState = () => page.evaluate(() => {
    const debugState = (window as unknown as { __weathermanDebug?: Record<string, unknown> }).__weathermanDebug
    return (debugState?.weather ?? {}) as { hour?: number; tilePasses?: number }
  })
  await expect.poll(async () => (await weatherState()).hour).toBe(0)
  // Let hour 0 finish loading: a 300 ms window with no tile pass.
  await expect.poll(async () => {
    const before = (await weatherState()).tilePasses
    await page.waitForTimeout(300)
    return (await weatherState()).tilePasses === before
  }, { timeout: 10_000 }).toBe(true)

  await page.locator('button').filter({ hasText: '▶' }).click()

  // Waiting for hour 3, playback holds the slider and sets the blend every
  // frame, but nothing can be blended yet: the tile pass must not rerun each
  // time (#42).
  await page.waitForTimeout(300)
  const waitingStart = (await weatherState()).tilePasses ?? 0
  await page.waitForTimeout(1_500)
  expect(((await weatherState()).tilePasses ?? 0) - waitingStart).toBeLessThanOrEqual(2)
  await expect(page.locator('input[aria-label="Forecast hour"]')).toHaveValue('0')

  // Hour 3 arrives: playback steps to it, then waits again for hour 6.
  release['3']()
  await expect(page.locator('input[aria-label="Forecast hour"]')).toHaveValue('1', { timeout: 10_000 })
  await page.locator('button').filter({ hasText: '⏸' }).click()
  release['6']()

  await expect(page.locator('input[aria-label="Forecast hour"]')).toHaveValue('1')
  await expect.poll(async () => (await weatherState()).hour).toBe(3)
})

test('a failed next-hour tile does not stall playback', async ({ page }) => {
  await mockPerformanceRoutes(page)
  // Registered later, so it wins: every hour-3 temperature tile fails.
  await page.route(/\/tiles\/gfs\/.*\/temperature\/3\/data\//, (route) => route.fulfill({ status: 500 }))
  await page.goto('/')
  await expect(page.locator('button').filter({ hasText: 'Temperature' })).toBeVisible({ timeout: 10_000 })
  await page.locator('button').filter({ hasText: 'Temperature' }).click()
  await page.locator('button').filter({ hasText: '▶' }).click()

  await page.waitForFunction(() => {
    const slider = document.querySelector('input[aria-label="Forecast hour"]') as HTMLInputElement | null
    return slider?.value === '2'
  }, undefined, { timeout: 10_000 })
  await page.locator('button').filter({ hasText: '⏸' }).click()
})

test('wind atlas ignores delayed loads from a scrubbed-away forecast hour', async ({ page }) => {
  await mockPerformanceRoutes(page, {
    delayedForecastHour: 3,
    delayedResponseMs: 750,
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

  const slider = page.locator('input[aria-label="Forecast hour"]')
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

  // Require the requests to have actually gone out: "nothing undelivered" is
  // also true in the instant before the first tile is requested.
  await expect.poll(
    () => requested.size > 12 && [...requested].every((url) => delivered.has(url)),
    { timeout: 20_000 },
  ).toBe(true)
})
