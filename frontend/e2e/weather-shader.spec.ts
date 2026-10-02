/**
 * Runs the weather tile shader in a bare WebGL2 context — no map — to check
 * how it samples at a tile's edge (#45) and which ranges it decodes with (#83).
 */
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { expect, test, type Page } from '@playwright/test'

const shader = (name: string) =>
  readFileSync(fileURLToPath(new URL(`../src/layers/shaders/${name}`, import.meta.url)), 'utf8')

interface Case {
  /** Tile side in texels: 258 with a gutter, 256 without. */
  side: number
  /** Normalised value of the tile's own texels, and of its outermost columns. */
  inner: number
  edge: number
  /** Range the tile was encoded with, and the range the colour ramp spans. */
  data?: [number, number]
  ramp?: [number, number]
}

/**
 * Draw each case's tile across one 512 px row through a grey ramp and read
 * back the ramp position (0-1) at its first, middle and last pixel.
 */
function rows(page: Page, cases: Case[]) {
  return page.evaluate(({ vert, frag, cases }) => {
    const gl = document.createElement('canvas').getContext('webgl2')!
    const program = gl.createProgram()!
    for (const [type, source] of [[gl.VERTEX_SHADER, vert], [gl.FRAGMENT_SHADER, frag]] as const) {
      const s = gl.createShader(type)!
      gl.shaderSource(s, source)
      gl.compileShader(s)
      if (!gl.getShaderParameter(s, gl.COMPILE_STATUS)) throw new Error(gl.getShaderInfoLog(s) ?? 'compile failed')
      gl.attachShader(program, s)
    }
    gl.linkProgram(program)
    gl.useProgram(program)
    const uniform = (name: string) => gl.getUniformLocation(program, name)

    const texture = (unit: number, width: number, height: number, data: Uint8Array, filter: number) => {
      gl.activeTexture(gl.TEXTURE0 + unit)
      gl.bindTexture(gl.TEXTURE_2D, gl.createTexture())
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA8, width, height, 0, gl.RGBA, gl.UNSIGNED_BYTE, data)
      for (const p of [gl.TEXTURE_MIN_FILTER, gl.TEXTURE_MAG_FILTER]) gl.texParameteri(gl.TEXTURE_2D, p, filter)
      for (const p of [gl.TEXTURE_WRAP_S, gl.TEXTURE_WRAP_T]) gl.texParameteri(gl.TEXTURE_2D, p, gl.CLAMP_TO_EDGE)
    }
    // Grey ramp: the output's red channel reads back the ramp position.
    const ramp = new Uint8Array(256 * 4)
    for (let i = 0; i < 256; i++) ramp.set([i, i, i, 255], i * 4)
    texture(1, 256, 1, ramp, gl.LINEAR)

    // The tile's quad covers clip space: mercator [0,1] → [-1,1].
    gl.uniformMatrix4fv(uniform('u_matrix'), false, [2, 0, 0, 0, 0, -2, 0, 0, 0, 0, 1, 0, -1, 1, 0, 1])
    gl.uniform2f(uniform('u_tileOffset'), 0, 0)
    gl.uniform2f(uniform('u_tileScale'), 1, 1)
    gl.uniform2f(uniform('u_uvOffset'), 0, 0)
    gl.uniform2f(uniform('u_uvScale'), 1, 1)
    gl.uniform1i(uniform('u_dataTile'), 0)
    gl.uniform1i(uniform('u_colorRamp'), 1)
    gl.uniform1f(uniform('u_opacity'), 1)
    gl.bindVertexArray(gl.createVertexArray())
    gl.bindBuffer(gl.ARRAY_BUFFER, gl.createBuffer())
    gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([0, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 1]), gl.STATIC_DRAW)
    gl.enableVertexAttribArray(1)
    gl.vertexAttribPointer(1, 2, gl.FLOAT, false, 0, 0)

    // One row, two output pixels per inner texel.
    const width = 512
    gl.activeTexture(gl.TEXTURE2) // its own unit: 0 and 1 hold the tile and the ramp
    gl.bindTexture(gl.TEXTURE_2D, gl.createTexture())
    gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA8, width, 1, 0, gl.RGBA, gl.UNSIGNED_BYTE, null)
    gl.bindFramebuffer(gl.FRAMEBUFFER, gl.createFramebuffer())
    gl.framebufferTexture2D(gl.FRAMEBUFFER, gl.COLOR_ATTACHMENT0, gl.TEXTURE_2D, gl.getParameter(gl.TEXTURE_BINDING_2D), 0)
    gl.viewport(0, 0, width, 1)

    return cases.map(({ side, inner, edge, data = [0, 1], ramp = [0, 1] }) => {
      // A PNG-encoded data tile: `inner` everywhere, `edge` in the outermost columns.
      const px = new Uint8Array(side * side * 4)
      for (let y = 0; y < side; y++) {
        for (let x = 0; x < side; x++) {
          const v = Math.round((x === 0 || x === side - 1 ? edge : inner) * 65535)
          px.set([v & 255, v >> 8, 0, 255], (y * side + x) * 4)
        }
      }
      texture(0, side, side, px, gl.NEAREST)
      gl.uniform1f(uniform('u_dataMin'), data[0])
      gl.uniform1f(uniform('u_dataMax'), data[1])
      gl.uniform1f(uniform('u_rampMin'), ramp[0])
      gl.uniform1f(uniform('u_rampMax'), ramp[1])
      gl.drawArrays(gl.TRIANGLES, 0, 6)
      const out = new Uint8Array(width * 4)
      gl.readPixels(0, 0, width, 1, gl.RGBA, gl.UNSIGNED_BYTE, out)
      return { first: out[0] / 255, middle: out[(width / 2) * 4] / 255, last: out[(width - 1) * 4] / 255 }
    })
  }, { vert: shader('weather.vert.glsl'), frag: shader('weather.frag.glsl'), cases })
}

test('a tile interpolates into its gutter at the edge; an old 256 px tile clamps', async ({ page }) => {
  await page.goto('about:blank')
  const [gutter, plain] = await rows(page, [
    { side: 258, inner: 0.2, edge: 0.8 }, // gutter (the neighbours' pixels) = 0.8
    { side: 256, inner: 0.2, edge: 0.2 }, // no gutter: the edge texels are the tile's own
  ])

  // The outermost output pixel sits a quarter texel past the last texel
  // centre: 0.75 * 0.2 + 0.25 * 0.8 = 0.35 — on its way to the neighbour.
  expect(gutter.middle).toBeCloseTo(0.2, 1)
  expect(gutter.first).toBeCloseTo(0.35, 1)
  expect(gutter.last).toBeCloseTo(0.35, 1)
  // Without a gutter the shader clamps at the edge, as before.
  expect(plain.last).toBeCloseTo(0.2, 1)
})

test('a tile decodes with the range it was encoded with, not the ramp\'s (#83)', async ({ page }) => {
  await page.goto('about:blank')
  // The same texels (0.5 of the encoding range) from two runs, one tiled
  // when the layer's range was 0-100, one when it was 0-50. The ramp spans 0-100.
  const [wide, narrow] = await rows(page, [
    { side: 258, inner: 0.5, edge: 0.5, data: [0, 100], ramp: [0, 100] }, // 50
    { side: 258, inner: 0.5, edge: 0.5, data: [0, 50], ramp: [0, 100] }, // 25
  ])
  expect(wide.middle).toBeCloseTo(0.5, 1)
  expect(narrow.middle).toBeCloseTo(0.25, 1)
})
