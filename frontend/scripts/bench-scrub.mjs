/**
 * Time from the last step of a rapid forecast scrub to the chosen hour fully
 * drawn, and what the hours stepped past cost (#92).
 *
 * Opens a production build (vite preview of `dist`, no backend needed) with a
 * mocked 16-hour run whose data tiles are real ones from a local published
 * run (`--tiles`, hours 0/3/6 reused round-robin), each held back by a fixed
 * latency plus its size over a bandwidth. CPU is throttled 4x. Each session
 * starts fresh at hour 0 on Wind Speed (colour layer and particles), then:
 *
 *   forward: --samples scrubs of --steps hours each to hours not yet seen
 *   back:    as many scrubs back, onto hours already drawn once
 *
 * A sample is the ms from the last step's click until the colour layer has
 * every tile of the chosen hour (no stand-ins). Abandoned hours are the ones
 * stepped past: their requests, bytes delivered, and requests aborted.
 *
 *   node scripts/bench-scrub.mjs --url http://localhost:4173 \
 *     --tiles ../.data/models/gfs/runs/20260930T12Z/data_tiles --sessions 5
 */
import { chromium } from '@playwright/test'
import { readFileSync, existsSync } from 'node:fs'
import { parseArgs } from 'node:util'

const { values: args } = parseArgs({
  options: {
    url: { type: 'string', default: 'http://localhost:4173' },
    tiles: { type: 'string' },
    sessions: { type: 'string', default: '5' },
    samples: { type: 'string', default: '5' },
    steps: { type: 'string', default: '3' },
    stepMs: { type: 'string', default: '150' },
    latencyMs: { type: 'string', default: '300' },
    mbps: { type: 'string', default: '20' },
    cpu: { type: 'string', default: '4' },
    label: { type: 'string', default: '' },
  },
})

const RUN_ID = '20260310T00Z'
const HOURS = Array.from({ length: 16 }, (_, i) => i * 3)
const REAL_HOURS = ['000', '003', '006']
const colormaps = JSON.parse(readFileSync(new URL('../e2e/fixtures/colormaps.json', import.meta.url)))
const manifest = {
  schema_version: 1, model: 'gfs', run_id: RUN_ID,
  cycle_time: '2026-03-10T00:00:00Z', published_at: '2026-03-10T02:30:00Z', resolution_km: 25,
  layers: [
    { id: 'wind_speed', display_name: 'Wind Speed', unit: 'm/s', palette_name: 'wind_speed', value_range: { min: 0, max: 50 } },
  ],
  forecast_hours: HOURS,
  data_ranges: { wind_u: { min: -50, max: 50 }, wind_v: { min: -50, max: 50 }, wind_speed: { min: 0, max: 50 } },
  tile_url_template: '/tiles/gfs/{run_id}/{layer}/{forecast_hour}/{z}/{x}/{y}.png',
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

async function session(browser, index) {
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } })
  const cdp = await page.context().newCDPSession(page)
  await cdp.send('Emulation.setCPUThrottlingRate', { rate: Number(args.cpu) })

  // Per data-tile request: hour, and how it ended.
  const tiles = new Map()
  await page.route('**/api/catalog/gfs', (r) => r.fulfill({ json: { model: 'gfs', current_run_id: RUN_ID, runs: [{ run_id: RUN_ID, status: 'published', published_at: manifest.published_at }] } }))
  await page.route(`**/api/manifest/gfs/${RUN_ID}`, (r) => r.fulfill({ json: manifest }))
  await page.route('**/tiles/colormaps.json', (r) => r.fulfill({ json: colormaps }))
  await page.route('**/ais/**', (r) => r.fulfill({ status: 404 }))
  await page.route('**/basemap/**', (r) => r.fulfill({ status: 404 }))
  await page.route('**/events/stream', (r) => r.fulfill({ status: 200, headers: { 'Content-Type': 'text/event-stream' }, body: ':ok\n\n' }))
  await page.route(/\/tiles\/gfs\/.*\/data\/\d+\/\d+\/\d+\.png/, async (route) => {
    const [, , , , layer, hour, , z, x, y] = new URL(route.request().url()).pathname.split('/')
    const real = REAL_HOURS[HOURS.indexOf(Number(hour)) % REAL_HOURS.length]
    const file = `${args.tiles}/${layer}/${real}/${z}/${x}/${y}`
    const body = existsSync(file) ? readFileSync(file) : null
    const tile = { hour: Number(hour), size: body?.length ?? 0, bytes: 0, aborted: false, done: false }
    tiles.set(route.request(), tile)
    await sleep(Number(args.latencyMs) + tile.size / (Number(args.mbps) * 125))
    // Rejects, or resolves to no avail, if the page aborted the request meanwhile.
    await route.fulfill(body ? { body, contentType: 'image/png' } : { status: 404 }).catch(() => {})
  })
  // Bytes count only for a response the page actually received.
  page.on('requestfinished', (request) => { const t = tiles.get(request); if (t) Object.assign(t, { bytes: t.size, done: true }) })
  page.on('requestfailed', (request) => { const t = tiles.get(request); if (t) Object.assign(t, { aborted: true, done: true }) })

  await page.goto(args.url)
  await page.locator('button').filter({ hasText: 'Wind Speed' }).first().waitFor({ timeout: 30_000 })
  const complete = (hour) => page.waitForFunction((h) => {
    const w = globalThis.__weathermanDebug?.weather
    return w?.hour === h && w.drawn > 0 && w.fallback === 0
  }, hour, { timeout: 120_000, polling: 'raf' })
  await complete(0)

  const rows = []
  let index_ = 0
  const scrub = async (direction, kind) => {
    const button = page.locator('button').filter({ hasText: direction > 0 ? '❯' : '❮' })
    const steps = Number(args.steps)
    const passed = HOURS.slice(0).filter((_, i) => direction > 0 ? i > index_ && i < index_ + steps : i < index_ && i > index_ - steps)
    const before = new Set(tiles.keys())
    // Requests for the hours passed already under way (e.g. as the next hour).
    const carried = [...tiles.values()].filter((t) => !t.done && passed.includes(t.hour))
    let start = 0
    for (let s = 0; s < steps; s++) {
      if (s > 0) await sleep(Number(args.stepMs))
      start = Date.now()
      await button.click()
    }
    index_ += direction * steps
    const hour = HOURS[index_]
    await complete(hour)
    const ms = Date.now() - start
    await sleep(Number(args.latencyMs) * 2) // let abandoned responses land or fail
    const mine = [...tiles].filter(([req]) => !before.has(req)).map(([, t]) => t)
    const all = [...mine.filter((t) => passed.includes(t.hour)), ...carried]
    const withdrawn = await page.evaluate(() => globalThis.__weathermanDebug?.tiles?.withdrawn ?? 0)
    rows.push({
      label: args.label, session: index, kind, hour, ms,
      finalRequests: mine.filter((t) => t.hour === hour).length,
      abandonedRequests: all.length,
      abandonedMB: +(all.reduce((s, t) => s + t.bytes, 0) / 1e6).toFixed(2),
      abandonedAborted: all.filter((t) => t.aborted).length,
      withdrawnTotal: withdrawn,
    })
  }
  const samples = Number(args.samples)
  for (let i = 0; i < samples; i++) await scrub(+1, 'forward')
  for (let i = 0; i < samples; i++) await scrub(-1, 'back')
  await page.close()
  return rows
}

const browser = await chromium.launch({ headless: true, args: ['--enable-gpu', '--ignore-gpu-blocklist', '--use-angle=metal'] })
for (let i = 0; i < Number(args.sessions); i++) {
  for (const row of await session(browser, i)) console.log(JSON.stringify(row))
}
await browser.close()
