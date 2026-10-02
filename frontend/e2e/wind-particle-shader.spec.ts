/**
 * Runs the wind particle update shader in a bare WebGL2 context — no map, no
 * tiles — to check the motion rules that are hard to see in a screenshot.
 */
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { expect, test } from '@playwright/test'
import { createTrailDecay, waveGridSpacingPx, windSpeedScale } from '../src/layers/particle-motion'
import { gpuTierForRenderer } from '../src/layers/gpu-tier'

const shader = (name: string) =>
  readFileSync(fileURLToPath(new URL(`../src/layers/shaders/${name}`, import.meta.url)), 'utf8')

const WORLD_SIZE = 512 * 2 ** 3

test('wind particles: speed per second, bulk respawn and missing data', async ({ page }) => {
  await page.goto('about:blank')

  const result = await page.evaluate(({ vert, frag, drawVert, drawFrag, worldSize, speedScale }) => {
    const gl = document.createElement('canvas').getContext('webgl2')!
    gl.getExtension('EXT_color_buffer_float')

    const link = (vertexSource: string, fragmentSource: string) => {
      const program = gl.createProgram()!
      for (const [type, source] of [[gl.VERTEX_SHADER, vertexSource], [gl.FRAGMENT_SHADER, fragmentSource]] as const) {
        const compiled = gl.createShader(type)!
        gl.shaderSource(compiled, source)
        gl.compileShader(compiled)
        if (!gl.getShaderParameter(compiled, gl.COMPILE_STATUS)) throw new Error(gl.getShaderInfoLog(compiled) ?? 'compile failed')
        gl.attachShader(program, compiled)
      }
      gl.linkProgram(program)
      if (!gl.getProgramParameter(program, gl.LINK_STATUS)) throw new Error(gl.getProgramInfoLog(program) ?? 'link failed')
      return program
    }
    const program = link(vert, frag)
    link(drawVert, drawFrag) // must at least compile

    const texture = (unit: number, internalFormat: number, width: number, height: number, format: number, type: number, data: ArrayBufferView | null) => {
      const tex = gl.createTexture()
      gl.activeTexture(gl.TEXTURE0 + unit)
      gl.bindTexture(gl.TEXTURE_2D, tex)
      gl.texImage2D(gl.TEXTURE_2D, 0, internalFormat, width, height, 0, format, type, data)
      for (const p of [gl.TEXTURE_MIN_FILTER, gl.TEXTURE_MAG_FILTER]) gl.texParameteri(gl.TEXTURE_2D, p, gl.NEAREST)
      for (const p of [gl.TEXTURE_WRAP_S, gl.TEXTURE_WRAP_T]) gl.texParameteri(gl.TEXTURE_2D, p, gl.CLAMP_TO_EDGE)
      return tex
    }
    // 4x4 PNG-encoded wind atlas holding one value everywhere (normalised to [0,1]; null = nodata)
    const atlas = (unit: number, normalised: number | null) => {
      const pixels = new Uint8Array(64)
      for (let i = 0; i < 16; i++) {
        const encoded = normalised == null ? 0 : Math.round(normalised * 65535)
        pixels.set([encoded & 255, encoded >> 8, normalised == null ? 255 : 0, 255], i * 4)
      }
      texture(unit, gl.RGBA8, 4, 4, gl.RGBA, gl.UNSIGNED_BYTE, pixels)
    }

    gl.bindVertexArray(gl.createVertexArray())
    const attribute = (location: number, data: number[]) => {
      gl.bindBuffer(gl.ARRAY_BUFFER, gl.createBuffer())
      gl.bufferData(gl.ARRAY_BUFFER, new Float32Array(data), gl.STATIC_DRAW)
      gl.enableVertexAttribArray(location)
      gl.vertexAttribPointer(location, 2, gl.FLOAT, false, 0, 0)
    }
    attribute(0, [-1, -1, 1, -1, -1, 1, -1, 1, 1, -1, 1, 1])
    attribute(1, [0, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 1])

    gl.useProgram(program)
    const uniform = (name: string) => gl.getUniformLocation(program, name)
    for (const [name, unit] of [['u_stateTex', 0], ['u_windU', 1], ['u_windV', 2], ['u_windUT1', 3], ['u_windVT1', 4]] as const) {
      gl.uniform1i(uniform(name), unit)
    }
    gl.uniform1i(uniform('u_hasData'), 1)
    gl.uniform1i(uniform('u_isFloat16'), 0)
    gl.uniform1f(uniform('u_temporalMix'), 0)
    gl.uniform1f(uniform('u_valueMin'), -50)
    gl.uniform1f(uniform('u_valueMax'), 50)
    gl.uniform1f(uniform('u_speedScale'), speedScale)
    gl.uniform4f(uniform('u_viewportBounds'), 0, 0, 1, 1)
    gl.uniform1f(uniform('u_atlasOriginX'), 0)
    gl.uniform1f(uniform('u_atlasOriginY'), 0)
    gl.uniform1f(uniform('u_atlasZoom'), 1)
    gl.uniform1f(uniform('u_atlasCols'), 1)
    gl.uniform1f(uniform('u_atlasRows'), 1)

    /** Run `steps` updates on a size×size state texture and read the result back. */
    const run = (size: number, initial: Float32Array, steps: number, dt: number) => {
      const textures = [
        texture(0, gl.RGBA32F, size, size, gl.RGBA, gl.FLOAT, initial),
        texture(0, gl.RGBA32F, size, size, gl.RGBA, gl.FLOAT, null),
      ]
      const framebuffers = textures.map((tex) => {
        const fbo = gl.createFramebuffer()
        gl.bindFramebuffer(gl.FRAMEBUFFER, fbo)
        gl.framebufferTexture2D(gl.FRAMEBUFFER, gl.COLOR_ATTACHMENT0, gl.TEXTURE_2D, tex, 0)
        return fbo
      })
      gl.viewport(0, 0, size, size)
      gl.uniform1f(uniform('u_dt'), dt)
      let read = 0
      for (let i = 0; i < steps; i++) {
        gl.uniform1f(uniform('u_seed'), (i * 7.31) % 1000)
        gl.activeTexture(gl.TEXTURE0)
        gl.bindTexture(gl.TEXTURE_2D, textures[read])
        gl.bindFramebuffer(gl.FRAMEBUFFER, framebuffers[1 - read])
        gl.drawArrays(gl.TRIANGLES, 0, 6)
        read = 1 - read
      }
      const state = new Float32Array(size * size * 4)
      gl.bindFramebuffer(gl.FRAMEBUFFER, framebuffers[read])
      gl.readPixels(0, 0, size, size, gl.RGBA, gl.FLOAT, state)
      return state
    }

    // 1. One particle in a uniform 10 m/s westerly, simulated for one second at three frame rates.
    atlas(1, 0.6) // U = +10 m/s
    atlas(2, 0.5) // V = 0
    const pixelsPerSecond = [30, 60, 120].map((fps) => {
      const state = run(1, new Float32Array([0.25, 0.5, 0, 0]), fps, 1 / fps)
      return (state[0] - 0.25) * worldSize
    })

    // 2. 1,024 particles, all off screen with the same age, updated once (what a pan or zoom does).
    const count = 32 * 32
    const offScreen = new Float32Array(count * 4)
    for (let i = 0; i < count; i++) offScreen.set([-1, -1, 0.3, 0], i * 4)
    const respawned = run(32, offScreen, 1, 1 / 60)
    const ages = Array.from({ length: count }, (_, i) => respawned[i * 4 + 2])
    const meanAge = ages.reduce((a, b) => a + b, 0) / count
    const ageSpread = Math.sqrt(ages.reduce((a, b) => a + (b - meanAge) ** 2, 0) / count)
    const hiddenOnRespawn = Array.from({ length: count }, (_, i) => respawned[i * 4 + 3]).filter((s) => s < 0).length

    // 3. The same particles on screen, but with no wind data anywhere.
    atlas(1, null)
    atlas(2, null)
    const onScreen = new Float32Array(count * 4)
    for (let i = 0; i < count; i++) onScreen.set([0.4, 0.6, 0.3, 5], i * 4)
    const noData = run(32, onScreen, 3, 1 / 60)
    const hiddenWithoutData = Array.from({ length: count }, (_, i) => noData[i * 4 + 3]).filter((s) => s < 0).length

    return { pixelsPerSecond, count, ageSpread, hiddenOnRespawn, hiddenWithoutData }
  }, {
    vert: shader('particle-update.vert.glsl'),
    frag: shader('particle-update.frag.glsl'),
    drawVert: shader('particle-draw.vert.glsl'),
    drawFrag: shader('particle-draw.frag.glsl'),
    worldSize: WORLD_SIZE,
    speedScale: windSpeedScale(WORLD_SIZE),
  })

  // Same distance per second whatever the frame rate (#32).
  const expectedPixels = windSpeedScale(WORLD_SIZE) * 10 * WORLD_SIZE // 10 m/s for one second
  for (const pixels of result.pixelsPerSecond) expect(pixels).toBeCloseTo(expectedPixels, 0)

  // Particles respawned together get different ages, so they do not expire together (#31).
  // A uniform spread over [0,1) has a standard deviation of 0.289.
  expect(result.ageSpread).toBeGreaterThan(0.2)
  expect(result.hiddenOnRespawn).toBe(result.count)

  // No wind data: nothing is drawn, rather than particles drifting at random (#34).
  expect(result.hiddenWithoutData).toBe(result.count)
})

test('wave dash grid never needs more cells than there are slots', () => {
  // Worst case for the layer's layout: origin snapped a whole cell before the
  // viewport, far edge rounded up (#35).
  const cellsNeeded = (w: number, h: number, s: number) => Math.ceil(w / s + 1) * Math.ceil(h / s + 1)

  expect(waveGridSpacingPx(24, 1280, 720, 14_400)).toBe(24) // plenty of slots: keep the preferred spacing
  for (const [w, h, slots] of [[1536, 864, 1600], [1920, 1080, 1600], [2560, 1440, 5184], [3840, 2160, 1600]]) {
    const s = waveGridSpacingPx(24, w, h, slots)
    expect(s).toBeGreaterThan(24)
    expect(cellsNeeded(w, h, s), `${w}x${h} / ${slots}`).toBeLessThanOrEqual(slots)
    expect(cellsNeeded(w, h, s * 0.97), `${w}x${h} / ${slots} is not wider than needed`).toBeGreaterThan(slots * 0.93)
  }
})

test('wind trail lasts the same time at any frame rate', () => {
  // Decay of a full-brightness trail pixel in an RGBA8 buffer, the way the
  // composite shader and the framebuffer do it: multiply, subtract, round.
  const lifetimeSeconds = (fps: number) => {
    const decay = createTrailDecay(0.97)
    let level = 255
    let frames = 0
    while (level > 0 && frames < fps * 10) {
      const { fade, epsilon } = decay(1 / fps)
      level = Math.round(Math.max(level * fade - epsilon * 255, 0))
      frames++
    }
    expect(level, `trail never clears at ${fps} fps`).toBe(0)
    return frames / fps
  }

  const at60 = lifetimeSeconds(60)
  for (const fps of [30, 90, 120, 144, 240]) {
    expect(Math.abs(lifetimeSeconds(fps) - at60) / at60, `${fps} fps vs 60 fps`).toBeLessThan(0.1)
  }
})

test('GPU tiers recognise Windows/ANGLE renderer names', () => {
  // ANGLE writes (R) marks that the patterns used to trip over (#40).
  expect(gpuTierForRenderer('ANGLE (Intel, Intel(R) UHD Graphics 620 Direct3D11 vs_5_0 ps_5_0, D3D11)')).toBe('medium')
  expect(gpuTierForRenderer('ANGLE (Intel, Intel(R) Iris(R) Xe Graphics Direct3D11 vs_5_0 ps_5_0, D3D11)')).toBe('medium')
  expect(gpuTierForRenderer('Intel UHD 630')).toBe('medium')
  expect(gpuTierForRenderer('Intel(R) HD Graphics 520')).toBe('low')
  expect(gpuTierForRenderer('Apple M2 Pro')).toBe('high')
  expect(gpuTierForRenderer('ANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0, D3D11)')).toBe('high')
  expect(gpuTierForRenderer('ANGLE (Google, Vulkan 1.3.0 (SwiftShader Device (Subzero)), SwiftShader driver)')).toBe('low')
})
