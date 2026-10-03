/**
 * Main-thread cost of hovering and of dragging the forecast slider (#94).
 *
 * Opens a production build (vite preview, proxied to a running backend with
 * a published run) in headless Chromium on the machine's GPU and runs two
 * journeys per session:
 *
 *   sweep: 5 s of pointer moves across the map, Temperature colour layer,
 *          AIS on (always is)
 *   drag:  5 s of dense back-and-forth on the forecast slider, Wave Height
 *          with wind particles and wave dashes, and a voyage panel open
 *
 * Each journey is preceded by the same scene left idle as long, as a
 * control: the particles alone cost main-thread time. For each it reports
 * React commits (a stub DevTools hook counts them; a
 * production build keeps no durations), main-thread task/script time (CDP),
 * input-to-frame latency of every input event (event time to the next
 * animation frame), and frame intervals.
 *
 *   node scripts/bench-interaction.mjs --url http://localhost:4173 --sessions 5 --label after
 */
import { chromium } from '@playwright/test'
import { parseArgs } from 'node:util'

const { values: args } = parseArgs({
  options: {
    url: { type: 'string', default: 'http://localhost:4173' },
    sessions: { type: 'string', default: '5' },
    seconds: { type: 'string', default: '5' },
    label: { type: 'string', default: '' },
  },
})
const SECONDS = Number(args.seconds)

/** Runs in the page before the app: counts commits, times input to frame. */
function instrument() {
  window.__bench = { commits: 0, latencies: [], frames: [], recording: false }
  const b = window.__bench
  window.__REACT_DEVTOOLS_GLOBAL_HOOK__ = {
    supportsFiber: true, renderers: new Map(), isDisabled: false,
    inject: () => 1, checkDCE() {}, onScheduleFiberRoot() {}, onCommitFiberUnmount() {}, onPostCommitFiberRoot() {},
    onCommitFiberRoot() { if (b.recording) b.commits++ },
  }
  let pending = []
  for (const type of ['pointermove', 'mousemove', 'input']) {
    addEventListener(type, (e) => { if (b.recording) pending.push(e.timeStamp) }, { capture: true, passive: true })
  }
  let last = 0
  const frame = (t) => {
    if (b.recording) {
      if (last) b.frames.push(t - last)
      for (const ts of pending) b.latencies.push(t - ts)
    }
    pending = []
    last = t
    requestAnimationFrame(frame)
  }
  requestAnimationFrame(frame)
}

/** The p-th percentile, rounded; null with no samples (an idle control has no inputs). */
const percentile = (xs, p) => {
  if (xs.length === 0) return null
  const s = [...xs].sort((a, b) => a - b)
  return +s[Math.min(s.length - 1, Math.floor(p * s.length))].toFixed(2)
}

async function measure(page, cdp, work) {
  const metrics = async () => Object.fromEntries((await cdp.send('Performance.getMetrics')).metrics.map(({ name, value }) => [name, value]))
  await page.evaluate(() => Object.assign(window.__bench, { commits: 0, latencies: [], frames: [], recording: true }))
  const m0 = await metrics()
  const inputs = await work()
  const m1 = await metrics()
  const b = await page.evaluate(() => { window.__bench.recording = false; return window.__bench })
  return {
    inputs,
    commits: b.commits,
    taskMs: +((m1.TaskDuration - m0.TaskDuration) * 1000).toFixed(1),
    scriptMs: +((m1.ScriptDuration - m0.ScriptDuration) * 1000).toFixed(1),
    latencyMedianMs: percentile(b.latencies, 0.5),
    latencyP95Ms: percentile(b.latencies, 0.95),
    latencySamples: b.latencies.length,
    frameMedianMs: percentile(b.frames, 0.5),
    frameP95Ms: percentile(b.frames, 0.95),
    framesOver33: b.frames.filter((f) => f > 33.4).length,
    frames: b.frames.length,
  }
}

const browser = await chromium.launch({ headless: true, args: ['--enable-gpu', '--ignore-gpu-blocklist', '--use-angle=metal'] })
for (let s = 0; s < Number(args.sessions); s++) {
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } })
  await page.addInitScript(instrument)
  const cdp = await page.context().newCDPSession(page)
  await cdp.send('Performance.enable')
  await page.goto(args.url)
  const button = (text) => page.locator('button').filter({ hasText: text }).first()
  await button('Temperature').waitFor({ timeout: 30_000 })
  await button('Temperature').click()
  await page.waitForFunction(() => (globalThis.__weathermanDebug?.weather?.drawn ?? 0) > 0, undefined, { timeout: 30_000 })
  await page.waitForTimeout(2_000)

  const idle = () => measure(page, cdp, async () => { await page.waitForTimeout(SECONDS * 1000); return 0 })
  console.log(JSON.stringify({ label: args.label, session: s, journey: 'sweep-idle', ...(await idle()) }))
  // Journey 1: pointer sweep over the map.
  const sweep = await measure(page, cdp, async () => {
    let n = 0
    const end = Date.now() + SECONDS * 1000
    while (Date.now() < end) {
      const t = n / 40
      await page.mouse.move(420 + 600 * (0.5 + 0.5 * Math.sin(t)), 250 + 400 * (0.5 + 0.5 * Math.sin(t * 1.7)))
      n++
    }
    return n
  })
  console.log(JSON.stringify({ label: args.label, session: s, journey: 'sweep', ...sweep }))

  // Journey 2: overlays plus a voyage panel, then a dense slider drag.
  await page.mouse.move(5, 5) // off the map: hover clears
  await button('Wave Height').click()
  await page.getByLabel('Wind particles', { exact: true }).check()
  await button('Draw Route').click()
  for (const [x, y] of [[500, 450], [750, 380], [1000, 470]]) {
    await page.mouse.click(x, y)
    await page.waitForTimeout(150)
  }
  await page.locator('button').filter({ hasText: /^Done/ }).click()
  await page.waitForFunction(() => document.querySelectorAll('svg rect').length > 100, undefined, { timeout: 30_000 })
  await page.waitForTimeout(2_000)
  console.log(JSON.stringify({ label: args.label, session: s, journey: 'drag-idle', ...(await idle()) }))
  const slider = page.locator('input[aria-label="Forecast hour"]')
  const box = (await slider.boundingBox())
  const drag = await measure(page, cdp, async () => {
    const y = box.y + box.height / 2
    await page.mouse.move(box.x + 4, y)
    await page.mouse.down()
    let n = 0
    const end = Date.now() + SECONDS * 1000
    while (Date.now() < end) {
      const t = (n % 200) / 100 // 0..2: across and back
      const f = t <= 1 ? t : 2 - t
      await page.mouse.move(box.x + 4 + f * (box.width - 8), y)
      n++
    }
    await page.mouse.up()
    return n
  })
  const rects = await page.evaluate(() => document.querySelectorAll('svg rect').length)
  console.log(JSON.stringify({ label: args.label, session: s, journey: 'drag', heatmapRects: rects, ...drag }))
  await page.close()
}
await browser.close()
