/**
 * Frame times of the particle overlays on a real GPU (#93).
 *
 * Opens a production build (vite preview, proxied to a running backend with
 * a published run) in headless Chromium on the machine's GPU, with vsync and
 * the frame-rate cap off so frame intervals show how long a frame takes
 * rather than the display rate. For each scenario and device pixel ratio it
 * records rAF intervals for --seconds after --warmup, the GPU time of each
 * frame's commands (EXT_disjoint_timer_query_webgl2, where the GPU offers
 * it: unlike the intervals, it holds up while the CPU is busy), main-thread
 * task and script time per frame (CDP Performance metrics), preparations
 * per second where the build counts them (#96), and the trail buffer size
 * from the debug state (or the canvas, before #93).
 *
 *   node scripts/bench-particle-frames.mjs --url http://localhost:4173 \
 *     --seconds 30 --reps 3 [--dpr 1,2] [--scenarios wind,both] [--cpu 4] [--label after] [--shots out-dir]
 *
 * Not for CI: SwiftShader frame times say nothing about a GPU.
 */
import { chromium } from '@playwright/test'
import { mkdirSync } from 'node:fs'
import { parseArgs } from 'node:util'

const { values: args } = parseArgs({
  options: {
    url: { type: 'string', default: 'http://localhost:4173' },
    seconds: { type: 'string', default: '30' },
    warmup: { type: 'string', default: '5' },
    reps: { type: 'string', default: '3' },
    dpr: { type: 'string', default: '1,2' },
    width: { type: 'string', default: '1440' },
    height: { type: 'string', default: '900' },
    label: { type: 'string', default: '' },
    scenarios: { type: 'string', default: 'colour-only,wind,waves,both' },
    cpu: { type: 'string', default: '1' },
    shots: { type: 'string' },
  },
})

/** Colour layer, and which overlays are on. */
const SCENARIOS = [
  { name: 'colour-only', layer: 'Wind Speed', wind: false, waves: false },
  { name: 'wind', layer: 'Wind Speed', wind: true, waves: false },
  { name: 'waves', layer: 'Wave Height', wind: false, waves: true },
  { name: 'both', layer: 'Wave Height', wind: true, waves: true },
]

const browser = await chromium.launch({
  headless: true,
  args: ['--enable-gpu', '--ignore-gpu-blocklist', '--use-angle=metal', '--disable-gpu-vsync', '--disable-frame-rate-limit'],
})

/**
 * Times the GPU work of every frame on MapLibre's context: a TIME_ELAPSED
 * query from the start of one frame's rAF callbacks to the next's. Runs in
 * the page before the app, so MapLibre's rAF goes through it.
 */
function gpuFrameTimer() {
  const raf = window.requestAnimationFrame.bind(window)
  const getContext = HTMLCanvasElement.prototype.getContext
  let gl = null, ext = null, open = null, lastTs = -1
  const pending = []
  window.__gpuFrames = []
  HTMLCanvasElement.prototype.getContext = function (type, attrs) {
    const ctx = getContext.call(this, type, attrs)
    if (type === 'webgl2' && ctx && !gl && this.classList.contains('maplibregl-canvas')) {
      gl = ctx
      ext = gl.getExtension('EXT_disjoint_timer_query_webgl2')
    }
    return ctx
  }
  window.requestAnimationFrame = (cb) => raf((ts) => {
    if (ext && window.__gpuTiming && ts !== lastTs) {
      lastTs = ts
      if (open) { gl.endQuery(ext.TIME_ELAPSED_EXT); pending.push(open) }
      const disjoint = gl.getParameter(ext.GPU_DISJOINT_EXT)
      while (pending.length && (disjoint || gl.getQueryParameter(pending[0], gl.QUERY_RESULT_AVAILABLE))) {
        const q = pending.shift()
        if (!disjoint) window.__gpuFrames.push(gl.getQueryParameter(q, gl.QUERY_RESULT) / 1e6)
        gl.deleteQuery(q)
      }
      open = gl.createQuery()
      gl.beginQuery(ext.TIME_ELAPSED_EXT, open)
    }
    cb(ts)
  })
}

const percentile = (sorted, p) => sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))]

async function run(scenario, dpr, rep) {
  const page = await browser.newPage({
    viewport: { width: Number(args.width), height: Number(args.height) },
    deviceScaleFactor: dpr,
  })
  await page.addInitScript(gpuFrameTimer)
  await page.goto(args.url)
  const layerButton = page.locator('button').filter({ hasText: scenario.layer })
  await layerButton.first().waitFor({ timeout: 30_000 })
  await layerButton.first().click()
  for (const [label, on] of [['Wind particles', scenario.wind], ['Wave dashes', scenario.waves]]) {
    const toggle = page.getByLabel(label, { exact: true })
    if (await toggle.count()) await toggle.setChecked(on)
  }
  // The weather layer has drawn, and every particle layer that should be on is.
  await page.waitForFunction(({ wind, waves }) => {
    const d = globalThis.__weathermanDebug ?? {}
    return (d.weather?.drawn ?? 0) > 0 && (!wind || d.wind?.active) && (!waves || d.wave?.active)
  }, scenario, { timeout: 30_000 })
  await page.waitForTimeout(Number(args.warmup) * 1000)

  const cdp = await page.context().newCDPSession(page)
  await cdp.send('Emulation.setCPUThrottlingRate', { rate: Number(args.cpu) })
  await cdp.send('Performance.enable')
  const metrics = async () => {
    const { metrics } = await cdp.send('Performance.getMetrics')
    const m = Object.fromEntries(metrics.map(({ name, value }) => [name, value]))
    const d = await page.evaluate(() => globalThis.__weathermanDebug ?? {})
    const prep = ['weather', 'wind', 'wave'].reduce((n, k) => n + (d[k]?.preparations ?? 0), 0)
    const counted = ['weather', 'wind', 'wave'].some((k) => d[k]?.preparations != null)
    return { task: m.TaskDuration, script: m.ScriptDuration, prep, counted }
  }
  const m0 = await metrics()
  const result = await page.evaluate((ms) => new Promise((resolve) => {
    const gl = document.querySelector('canvas.maplibregl-canvas').getContext('webgl2')
    const info = gl.getExtension('WEBGL_debug_renderer_info')
    const intervals = []
    let last = performance.now()
    const end = last + ms
    window.__gpuTiming = true
    const tick = (t) => {
      intervals.push(t - last)
      last = t
      if (t < end) requestAnimationFrame(tick)
      else {
        window.__gpuTiming = false
        const d = globalThis.__weathermanDebug ?? {}
        const trail = (s) => !s?.active ? null : s.trailWidth ? [s.trailWidth, s.trailHeight] : [gl.drawingBufferWidth, gl.drawingBufferHeight]
        resolve({
          renderer: info ? gl.getParameter(info.UNMASKED_RENDERER_WEBGL) : gl.getParameter(gl.RENDERER),
          canvas: [gl.drawingBufferWidth, gl.drawingBufferHeight],
          trails: { wind: trail(d.wind), wave: trail(d.wave) },
          intervals,
          gpu: window.__gpuFrames,
        })
      }
    }
    requestAnimationFrame(tick)
  }), Number(args.seconds) * 1000)

  const m1 = await metrics()
  if (args.shots && rep === 0) {
    mkdirSync(args.shots, { recursive: true })
    await page.screenshot({ path: `${args.shots}/${args.label || 'run'}-${scenario.name}-dpr${dpr}.png` })
  }
  await page.close()

  const sorted = [...result.intervals].sort((a, b) => a - b)
  const gpu = [...result.gpu].sort((a, b) => a - b)
  const trailBytes = Object.values(result.trails).filter(Boolean).reduce((sum, [w, h]) => sum + 2 * w * h * 4, 0)
  return {
    label: args.label, scenario: scenario.name, dpr, rep,
    frames: sorted.length,
    medianMs: +percentile(sorted, 0.5).toFixed(2),
    p95Ms: +percentile(sorted, 0.95).toFixed(2),
    slowFrames: sorted.filter((ms) => ms > 33.4).length,
    taskMsPerFrame: +((m1.task - m0.task) * 1000 / sorted.length).toFixed(3),
    scriptMsPerFrame: +((m1.script - m0.script) * 1000 / sorted.length).toFixed(3),
    preparationsPerSec: m1.counted ? +((m1.prep - m0.prep) / Number(args.seconds)).toFixed(2) : null,
    gpuFrames: gpu.length,
    gpuMedianMs: gpu.length ? +percentile(gpu, 0.5).toFixed(3) : null,
    gpuP95Ms: gpu.length ? +percentile(gpu, 0.95).toFixed(3) : null,
    canvas: result.canvas.join('x'),
    trails: Object.fromEntries(Object.entries(result.trails).filter(([, v]) => v).map(([k, v]) => [k, v.join('x')])),
    trailMiB: +(trailBytes / 2 ** 20).toFixed(1),
    renderer: result.renderer,
  }
}

for (let rep = 0; rep < Number(args.reps); rep++) {
  for (const dpr of args.dpr.split(',').map(Number)) {
    for (const scenario of SCENARIOS.filter((s) => args.scenarios.split(',').includes(s.name))) {
      console.log(JSON.stringify(await run(scenario, dpr, rep)))
    }
  }
}
await browser.close()
