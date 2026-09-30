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
