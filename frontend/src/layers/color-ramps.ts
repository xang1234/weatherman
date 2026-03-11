/**
 * Color ramp definitions and WebGL texture generation.
 *
 * Defines color stops for each weather layer (matching the Python
 * colormaps.py definitions) and provides utilities to interpolate
 * them into 1024-entry RGBA arrays for upload as 1024x1 GL textures.
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
// These match the Python colormaps.py definitions exactly.
// The backend also exposes /tiles/colormaps.json for dynamic fetch.

const TEMPERATURE_STOPS: ColorStop[] = [
  { position: 0.00, color: [45, 0, 75] },
  { position: 0.10, color: [60, 10, 150] },
  { position: 0.18, color: [30, 50, 200] },
  { position: 0.27, color: [0, 90, 230] },
  { position: 0.36, color: [0, 180, 210] },
  { position: 0.45, color: [0, 200, 80] },
  { position: 0.50, color: [80, 220, 20] },
  { position: 0.55, color: [220, 220, 0] },
  { position: 0.64, color: [255, 180, 0] },
  { position: 0.73, color: [255, 100, 0] },
  { position: 0.82, color: [230, 30, 15] },
  { position: 0.91, color: [180, 0, 0] },
  { position: 1.00, color: [130, 0, 50] },
]

const WIND_SPEED_STOPS: ColorStop[] = [
  { position: 0.00, color: [30, 50, 200] },
  { position: 0.07, color: [0, 90, 230] },
  { position: 0.12, color: [0, 180, 210] },
  { position: 0.18, color: [0, 200, 80] },
  { position: 0.23, color: [80, 220, 20] },
  { position: 0.27, color: [220, 220, 0] },
  { position: 0.31, color: [255, 180, 0] },
  { position: 0.34, color: [255, 100, 0] },
  { position: 0.36, color: [230, 30, 15] },
  { position: 0.50, color: [180, 0, 0] },
  { position: 0.70, color: [130, 0, 80] },
  { position: 0.85, color: [90, 0, 140] },
  { position: 1.00, color: [60, 0, 160] },
]

const PRECIPITATION_STOPS: ColorStop[] = [
  { position: 0.000, color: [255, 255, 255], alpha: 0 },    // transparent at zero
  { position: 0.002, color: [255, 255, 255], alpha: 255 },  // opaque at ~0.5 mm
  { position: 0.15, color: [199, 233, 192] },
  { position: 0.30, color: [120, 198, 168] },
  { position: 0.50, color: [65, 171, 93] },
  { position: 0.70, color: [35, 132, 67] },
  { position: 0.85, color: [0, 90, 50] },
  { position: 1.00, color: [0, 50, 30] },
]

const PRESSURE_STOPS: ColorStop[] = [
  { position: 0.00, color: [60, 0, 160] },     // deep purple (920 hPa, deep low)
  { position: 0.15, color: [30, 50, 200] },     // blue (940 hPa)
  { position: 0.30, color: [0, 150, 200] },     // cyan (960 hPa)
  { position: 0.45, color: [0, 200, 80] },      // green (980 hPa)
  { position: 0.57, color: [180, 220, 40] },    // yellow-green (1000 hPa)
  { position: 0.64, color: [220, 220, 0] },     // yellow (1010 hPa, standard)
  { position: 0.75, color: [255, 180, 0] },     // orange (1025 hPa)
  { position: 0.85, color: [255, 100, 0] },     // red-orange (1040 hPa)
  { position: 1.00, color: [180, 0, 0] },       // red (1060 hPa, strong high)
]

const CLOUD_COVER_STOPS: ColorStop[] = [
  { position: 0.00, color: [240, 248, 255] },   // near-white (clear sky)
  { position: 0.25, color: [200, 210, 220] },   // light gray
  { position: 0.50, color: [160, 170, 180] },   // mid gray
  { position: 0.75, color: [110, 120, 130] },   // dark gray
  { position: 1.00, color: [60, 65, 75] },      // very dark gray (overcast)
]

const WAVE_HEIGHT_STOPS: ColorStop[] = [
  { position: 0.00, color: [30, 50, 200] },     // blue (calm, 0 m)
  { position: 0.10, color: [0, 130, 220] },     // bright blue (~1.5 m)
  { position: 0.20, color: [0, 190, 180] },     // cyan (~3 m)
  { position: 0.33, color: [0, 200, 80] },      // green (~5 m)
  { position: 0.47, color: [220, 220, 0] },     // yellow (~7 m)
  { position: 0.60, color: [255, 160, 0] },     // orange (~9 m)
  { position: 0.73, color: [230, 30, 15] },     // red (~11 m)
  { position: 0.87, color: [180, 0, 0] },       // deep red (~13 m)
  { position: 1.00, color: [130, 0, 80] },      // purple (15 m, extreme)
]

const WAVE_PERIOD_STOPS: ColorStop[] = [
  { position: 0.00, color: [30, 50, 200] },     // blue (short period, 0 s)
  { position: 0.15, color: [0, 160, 210] },     // cyan (~3.75 s)
  { position: 0.30, color: [0, 200, 80] },      // green (~7.5 s)
  { position: 0.50, color: [220, 220, 0] },     // yellow (~12.5 s)
  { position: 0.70, color: [255, 140, 0] },     // orange (~17.5 s)
  { position: 0.85, color: [230, 30, 15] },     // red (~21.25 s)
  { position: 1.00, color: [130, 0, 80] },      // purple (25 s, long swell)
]

const WAVE_DIRECTION_STOPS: ColorStop[] = [
  { position: 0.00, color: [230, 30, 15] },     // red (N, 0°)
  { position: 0.125, color: [255, 160, 0] },    // orange (NE, 45°)
  { position: 0.25, color: [220, 220, 0] },     // yellow (E, 90°)
  { position: 0.375, color: [0, 200, 80] },     // green (SE, 135°)
  { position: 0.50, color: [0, 160, 210] },     // cyan (S, 180°)
  { position: 0.625, color: [30, 50, 200] },    // blue (SW, 225°)
  { position: 0.75, color: [100, 0, 180] },     // purple (W, 270°)
  { position: 0.875, color: [180, 0, 100] },    // magenta (NW, 315°)
  { position: 1.00, color: [230, 30, 15] },     // red (N, 360° = 0°)
]

/** Registry of all color ramp definitions by layer name. */
export const COLOR_RAMPS: Record<string, ColorRampDef> = {
  temperature: {
    name: 'temperature',
    unit: '°C',
    valueMin: -55,
    valueMax: 55,
    stops: TEMPERATURE_STOPS,
  },
  wind_speed: {
    name: 'wind_speed',
    unit: 'kt',
    valueMin: 0,
    valueMax: 50,
    stops: WIND_SPEED_STOPS,
  },
  precipitation: {
    name: 'precipitation',
    unit: 'kg/m²',
    valueMin: 0,
    valueMax: 250,
    stops: PRECIPITATION_STOPS,
  },
  pressure: {
    name: 'pressure',
    unit: 'Pa',
    valueMin: 92000,
    valueMax: 106000,
    stops: PRESSURE_STOPS,
  },
  cloud_cover: {
    name: 'cloud_cover',
    unit: '%',
    valueMin: 0,
    valueMax: 100,
    stops: CLOUD_COVER_STOPS,
  },
  wave_height: {
    name: 'wave_height',
    unit: 'm',
    valueMin: 0,
    valueMax: 15,
    stops: WAVE_HEIGHT_STOPS,
  },
  wave_period: {
    name: 'wave_period',
    unit: 's',
    valueMin: 0,
    valueMax: 25,
    stops: WAVE_PERIOD_STOPS,
  },
  wave_direction: {
    name: 'wave_direction',
    unit: 'degree',
    valueMin: 0,
    valueMax: 360,
    stops: WAVE_DIRECTION_STOPS,
  },
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
