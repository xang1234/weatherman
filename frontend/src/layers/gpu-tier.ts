/**
 * GPU tier detection for adaptive particle count.
 *
 * Classifies the GPU into tiers based on the WebGL renderer string
 * (via WEBGL_debug_renderer_info). Falls back to a conservative default
 * if the extension is unavailable (e.g., privacy-focused browsers).
 *
 * Tier → particle state-texture size (particles = size²):
 *   HIGH   → 512  (262,144 particles)
 *   MEDIUM → 256  (65,536 particles)
 *   LOW    → 128  (16,384 particles)
 */

export type GpuTier = 'high' | 'medium' | 'low'

export interface GpuTierResult {
  tier: GpuTier
  /** State texture dimension (power of 2). Particle count = stateSize². */
  stateSize: number
  /** Raw renderer string, or 'unknown' if extension unavailable. */
  renderer: string
}

const TIER_STATE_SIZES: Record<GpuTier, number> = {
  high: 512,
  medium: 256,
  low: 128,
}

// ── Renderer string patterns ─────────────────────────────────────────
// Matched case-insensitively against UNMASKED_RENDERER_WEBGL.

/** GPUs known to handle 512² particles at 60fps easily. */
const HIGH_PATTERNS = [
  /apple m[2-9]/i,                   // Apple Silicon M2+
  /apple m\d\d/i,                    // Apple M10+ (future-proof)
  /nvidia geforce (rtx|gtx 1[6-9]|gtx [2-9])/i, // NVIDIA GTX 1660+, RTX series
  /nvidia a\d{3,4}/i,               // NVIDIA datacenter (A100, etc.)
  /radeon rx [5-9]\d{2,3}/i,        // AMD RX 5000+
  /radeon rx \d{4}/i,               // AMD RX 7000+
  /radeon pro [wv]/i,               // AMD Pro workstation
]

/** GPUs where 256² is appropriate. */
const MEDIUM_PATTERNS = [
  /apple m1/i,                       // Apple M1 (still good, but not 512²)
  /apple gpu/i,                      // Generic Apple (A-series iPad/iPhone)
  /intel iris (plus|pro|xe)/i,       // Intel Iris integrated (decent)
  /intel uhd [6-9]\d{2}/i,          // Intel UHD 630+
  /nvidia geforce gtx 1[0-5]/i,     // NVIDIA GTX 1050-1550
  /nvidia geforce mx/i,             // NVIDIA MX mobile
  /radeon rx [34]\d{2}/i,           // AMD RX 400/500 series (older but capable)
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
 * Clamp a user-provided state size to a valid power-of-two in [64, 1024].
 * Returns the nearest power of 2.
 */
export function clampStateSize(size: number): number {
  const clamped = Math.max(64, Math.min(1024, size))
  return Math.pow(2, Math.round(Math.log2(clamped)))
}
