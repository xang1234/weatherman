/**
 * GPU tier detection for adaptive particle count.
 *
 * Classifies the GPU into tiers based on the WebGL renderer string
 * (via WEBGL_debug_renderer_info). Falls back to a conservative default
 * if the extension is unavailable (e.g., privacy-focused browsers).
 *
 * Tier → particle state-texture size (particles = size²):
 *   HIGH   → 80   (6,400 particles)
 *   MEDIUM → 50   (2,500 particles)
 *   LOW    → 26   (676 particles)
 */

export type GpuTier = 'high' | 'medium' | 'low'

export interface GpuTierResult {
  tier: GpuTier
  /** State texture dimension. Particle count = stateSize². */
  stateSize: number
  /** Raw renderer string, or 'unknown' if extension unavailable. */
  renderer: string
}

const TIER_STATE_SIZES: Record<GpuTier, number> = {
  high: 80,
  medium: 50,
  low: 26,
}

// ── Renderer string patterns ─────────────────────────────────────────
// Matched case-insensitively against UNMASKED_RENDERER_WEBGL.

/** GPUs known to handle 160² particles at 60fps easily. */
const HIGH_PATTERNS = [
  /apple m[2-9]/i,                   // Apple Silicon M2+
  /apple m\d\d/i,                    // Apple M10+ (future-proof)
  /nvidia geforce (rtx|gtx 1[6-9]|gtx [2-9])/i, // NVIDIA GTX 1660+, RTX series
  /nvidia a\d{3,4}/i,               // NVIDIA datacenter (A100, etc.)
  /radeon rx \d{4}/i,               // AMD RX 5000+ (4-digit models: 5700, 6800, 7900)
  /radeon pro [wv]/i,               // AMD Pro workstation
]

/** GPUs where 80² is appropriate. */
const MEDIUM_PATTERNS = [
  /apple m1/i,                       // Apple M1 (still good, but not 512²)
  /apple gpu/i,                      // Generic Apple (A-series iPad/iPhone)
  /intel iris (plus|pro|xe)/i,       // Intel Iris integrated (decent)
  /intel uhd [6-9]\d{2}/i,          // Intel UHD 630+
  /nvidia geforce gtx 1[0-5]/i,     // NVIDIA GTX 1050-1550
  /nvidia geforce mx/i,             // NVIDIA MX mobile
  /radeon rx [3-5]\d{2}/i,          // AMD RX 400/500 series (3-digit models: 480, 580, 590)
  /radeon vega/i,                    // AMD Vega integrated
  /mali-g[7-9]/i,                   // ARM Mali high-end mobile
  /adreno 6[3-9]\d/i,              // Qualcomm Adreno 630+
]

// Everything else (Intel HD, old AMD, software renderers, unknown) → LOW.

/**
 * Detect GPU tier from a WebGL2 context.
 *
 * Should be called once during layer initialization. The result is
 * deterministic for a given device, so caching across instances is safe.
 */
export function detectGpuTier(gl: WebGL2RenderingContext): GpuTierResult {
  const ext = gl.getExtension('WEBGL_debug_renderer_info')
  const renderer = ext
    ? gl.getParameter(ext.UNMASKED_RENDERER_WEBGL) as string
    : 'unknown'

  // Check high-tier patterns first
  for (const pattern of HIGH_PATTERNS) {
    if (pattern.test(renderer)) {
      return { tier: 'high', stateSize: TIER_STATE_SIZES.high, renderer }
    }
  }

  // Then medium-tier
  for (const pattern of MEDIUM_PATTERNS) {
    if (pattern.test(renderer)) {
      return { tier: 'medium', stateSize: TIER_STATE_SIZES.medium, renderer }
    }
  }

  // Unknown or weak GPU — be conservative
  return { tier: 'low', stateSize: TIER_STATE_SIZES.low, renderer }
}

/**
 * Clamp a user-provided state size to a valid range [16, 512].
 * Returns the value rounded to the nearest multiple of 8.
 */
export function clampStateSize(size: number): number {
  const clamped = Math.max(16, Math.min(512, size))
  return Math.round(clamped / 8) * 8
}
