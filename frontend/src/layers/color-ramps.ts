/**
 * Color ramp definitions and WebGL texture generation.
 *
 * Loads each weather layer's color stops from the backend (colormaps.py)
 * and interpolates them into 1024-entry RGBA arrays for upload as
 * 1024x1 GL textures.
 * Interpolation is performed in OKLAB perceptual color space to
 * eliminate muddy intermediate colors in RGB gradients.
 *
 * The fragment shader samples this 1D texture to map decoded float
 * values to colors — essentially a GPU-native color lookup table.
 */

/** A color stop: position in [0,1] and RGB values in [0,255]. */
export interface ColorStop {
  position: number
  color: [number, number, number]
  alpha?: number  // 0-255, defaults to 255
}

/** Color ramp definition for a weather layer. */
export interface ColorRampDef {
  name: string
  unit: string
  valueMin: number
  valueMax: number
  stops: ColorStop[]
}

// ── Color ramp definitions ───────────────────────────────────────
// Served by the backend from colormaps.py — the same ranges the tile
// encoder normalised with, so they must not be copied here.

/** Ramps by layer name, filled in by loadColorRamps(). */
export const COLOR_RAMPS: Record<string, ColorRampDef> = {}

let loading: Promise<void> | null = null

/** Fetch the ramps into COLOR_RAMPS, once; a failed fetch is retried on the next call. */
export function loadColorRamps(apiBase: string): Promise<void> {
  loading ??= fetch(`${apiBase}/tiles/colormaps.json`)
    .then((res) => {
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      return res.json()
    })
    .then((ramps: Record<string, ColorRampDef>) => { Object.assign(COLOR_RAMPS, ramps) })
    .catch((err) => {
      loading = null
      throw err
    })
  return loading
}

// ── OKLAB color space conversions ────────────────────────────────
// OKLAB is a perceptually uniform color space where linear
// interpolation produces visually smooth, non-muddy gradients.

/** sRGB [0,255] channel → linear [0,1] using the sRGB transfer function. */
function srgbToLinear(c: number): number {
  const s = c / 255
  return s <= 0.04045 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4
}

/** Linear [0,1] → sRGB [0,255], clamped. */
function linearToSrgb(c: number): number {
  const s = c <= 0.0031308 ? 12.92 * c : 1.055 * c ** (1 / 2.4) - 0.055
  return Math.round(Math.max(0, Math.min(255, s * 255)))
}

/** Linear RGB → OKLAB [L, a, b]. */
function linearRgbToOklab(r: number, g: number, b: number): [number, number, number] {
  const l = 0.4122214708 * r + 0.5363325363 * g + 0.0514459929 * b
  const m = 0.2119034982 * r + 0.6806995451 * g + 0.1073969566 * b
  const s = 0.0883024619 * r + 0.2817188376 * g + 0.6299787005 * b

  const l_ = Math.cbrt(l)
  const m_ = Math.cbrt(m)
  const s_ = Math.cbrt(s)

  return [
    0.2104542553 * l_ + 0.7936177850 * m_ - 0.0040720468 * s_,
    1.9779984951 * l_ - 2.4285922050 * m_ + 0.4505937099 * s_,
    0.0259040371 * l_ + 0.7827717662 * m_ - 0.8086757660 * s_,
  ]
}

/** OKLAB [L, a, b] → linear RGB. */
function oklabToLinearRgb(L: number, a: number, b: number): [number, number, number] {
  const l_ = L + 0.3963377774 * a + 0.2158037573 * b
  const m_ = L - 0.1055613458 * a - 0.0638541728 * b
  const s_ = L - 0.0894841775 * a - 1.2914855480 * b

  const l = l_ * l_ * l_
  const m = m_ * m_ * m_
  const s = s_ * s_ * s_

  return [
    +4.0767416621 * l - 3.3077115913 * m + 0.2309699292 * s,
    -1.2684380046 * l + 2.6097574011 * m - 0.3413193965 * s,
    -0.0041960863 * l - 0.7034186147 * m + 1.7076147010 * s,
  ]
}

// ── Interpolation ────────────────────────────────────────────────

/**
 * Interpolate color stops into an RGBA Uint8Array.
 * Alpha is read from each stop's `alpha` field (default 255) and
 * interpolated linearly (not in OKLAB — alpha is perceptually linear).
 */
export function interpolateColorRamp(stops: ColorStop[], size = 1024): Uint8Array {
  const data = new Uint8Array(size * 4)

  for (let i = 0; i < size; i++) {
    const t = i / (size - 1)

    // Find surrounding stops
    let lower = 0
    for (let j = 0; j < stops.length - 1; j++) {
      if (stops[j + 1].position >= t) {
        lower = j
        break
      }
      if (j === stops.length - 2) {
        lower = j
      }
    }

    const s0 = stops[lower]
    const s1 = stops[lower + 1]
    const range = s1.position - s0.position
    const frac = range === 0 ? 0 : Math.max(0, Math.min(1, (t - s0.position) / range))

    // Interpolate in OKLAB perceptual color space for smooth gradients
    const [L0, a0, b0] = linearRgbToOklab(srgbToLinear(s0.color[0]), srgbToLinear(s0.color[1]), srgbToLinear(s0.color[2]))
    const [L1, a1, b1] = linearRgbToOklab(srgbToLinear(s1.color[0]), srgbToLinear(s1.color[1]), srgbToLinear(s1.color[2]))
    const [lr, lg, lb] = oklabToLinearRgb(L0 + (L1 - L0) * frac, a0 + (a1 - a0) * frac, b0 + (b1 - b0) * frac)

    // Interpolate alpha linearly (not in OKLAB — alpha is perceptually linear)
    const alpha0 = s0.alpha ?? 255
    const alpha1 = s1.alpha ?? 255

    const idx = i * 4
    data[idx + 0] = linearToSrgb(lr)
    data[idx + 1] = linearToSrgb(lg)
    data[idx + 2] = linearToSrgb(lb)
    data[idx + 3] = Math.round(alpha0 + (alpha1 - alpha0) * frac)
  }

  return data
}

// ── WebGL texture creation ───────────────────────────────────────

/**
 * Create a 1024x1 RGBA texture from a color ramp definition.
 * Uses LINEAR filtering so the shader gets smooth interpolation
 * between color entries for free.
 */
export function createColorRampTexture(
  gl: WebGL2RenderingContext,
  ramp: ColorRampDef,
): WebGLTexture {
  const texture = gl.createTexture()
  if (!texture) throw new Error('Failed to create color ramp texture')

  const data = interpolateColorRamp(ramp.stops)

  gl.bindTexture(gl.TEXTURE_2D, texture)
  gl.texImage2D(
    gl.TEXTURE_2D, 0, gl.RGBA,
    1024, 1, 0,
    gl.RGBA, gl.UNSIGNED_BYTE,
    data,
  )
  // LINEAR for smooth color interpolation between the 1024 entries
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR)
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR)
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE)
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE)
  gl.bindTexture(gl.TEXTURE_2D, null)

  return texture
}
