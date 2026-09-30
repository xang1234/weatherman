/**
 * Frame-rate-independent motion constants for the particle layers.
 *
 * Kept free of imports so tests can load it outside the browser bundle.
 */

/**
 * Screen speed of a wind particle in a reference 10 m/s wind, in CSS pixels
 * per second. Independent of zoom and of frame rate.
 */
const TARGET_SPEED_PX_PER_S = 30
/** Reference wind speed (m/s) for TARGET_SPEED_PX_PER_S. */
const REF_WIND_MPS = 10.0

/** Factor converting wind speed (m/s) to mercator units per second. */
export function windSpeedScale(worldSize: number): number {
  return TARGET_SPEED_PX_PER_S / (REF_WIND_MPS * worldSize)
}

/**
 * Convert a fade factor defined per 1/60 s into the factor for a frame that
 * took `dt` seconds, so a trail lasts the same time at any frame rate.
 */
export function fadeForFrame(fadePer60th: number, dt: number): number {
  return Math.pow(fadePer60th, dt * 60)
}

/** Per-frame trail decay: the buffer is multiplied by `fade`, then `epsilon` is subtracted. */
export interface TrailDecay {
  fade: number
  epsilon: number
}

/**
 * Shortest interval over which a decay step is applied. Below it the
 * subtraction would be under half an 8-bit level and low values would stop
 * decaying, so faster frames pass the trail through and the time is carried
 * over to the next step.
 */
const MIN_DECAY_STEP_S = 1 / 110

/**
 * Frame-rate-independent decay for a trail kept in an RGBA8 buffer.
 *
 * A pure multiplicative fade stalls at 8 bits (v * fade rounds back to v
 * for small v), so a constant is subtracted as well: 1/255 per 1/60 s. Both
 * parts are scaled to the elapsed time — together they follow
 * v' = (v + B) * fade - B, which gives the same curve however the time is
 * sliced into frames.
 *
 * Call the returned function once per frame with that frame's duration.
 */
export function createTrailDecay(fadePer60th: number): (dt: number) => TrailDecay {
  const offset = 1 / 255 / (1 - fadePer60th) // B above
  let pending = 0
  return (dt) => {
    pending += dt
    if (pending < MIN_DECAY_STEP_S) return { fade: 1, epsilon: 0 }
    const fade = fadeForFrame(fadePer60th, pending)
    pending = 0
    return { fade, epsilon: offset * (1 - fade) }
  }
}

