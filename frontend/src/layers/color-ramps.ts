/**
 * Color ramp definitions and WebGL texture generation.
 *
 * Defines color stops for each weather layer (matching the Python
 * colormaps.py definitions) and provides utilities to interpolate
 * them into 256-entry RGBA arrays for upload as 256x1 GL textures.
 *
 * The fragment shader samples this 1D texture to map decoded float
 * values to colors — essentially a GPU-native color lookup table.
 */

/** A color stop: position in [0,1] and RGB values in [0,255]. */
export interface ColorStop {
  position: number
  color: [number, number, number]
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
  { position: 0.00, color: [255, 255, 255] },
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

// ── Interpolation ────────────────────────────────────────────────

/**
 * Interpolate color stops into a 256-entry RGBA Uint8Array (1024 bytes).
 * Alpha is always 255 (fully opaque).
 */
export function interpolateColorRamp(stops: ColorStop[]): Uint8Array {
  const size = 256
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

    const idx = i * 4
    data[idx + 0] = Math.round(s0.color[0] + (s1.color[0] - s0.color[0]) * frac)
    data[idx + 1] = Math.round(s0.color[1] + (s1.color[1] - s0.color[1]) * frac)
    data[idx + 2] = Math.round(s0.color[2] + (s1.color[2] - s0.color[2]) * frac)
    data[idx + 3] = 255
  }

  return data
}

// ── WebGL texture creation ───────────────────────────────────────

/**
 * Create a 256x1 RGBA texture from a color ramp definition.
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
    256, 1, 0,
    gl.RGBA, gl.UNSIGNED_BYTE,
    data,
  )
  // LINEAR for smooth color interpolation between the 256 entries
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR)
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR)
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE)
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE)
  gl.bindTexture(gl.TEXTURE_2D, null)

  return texture
}
