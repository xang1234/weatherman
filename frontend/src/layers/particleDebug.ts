export type ParticleDebugLayer = 'wind' | 'wave'

export interface ParticleDebugState {
  mounts: number
  active: boolean
  atlasBlits: number
  atlasClears: number
  atlasFlushes: number
  pendingDirtyTiles: number
  /** Wave only: the last frame needed more grid cells than there are dash slots. */
  gridTruncated?: boolean
}

/** What the weather colour layer drew in its last frame (for the e2e suite). */
export interface WeatherDebugState {
  /** Quads drawn. */
  drawn: number
  /** Of those, drawn from a stand-in: an ancestor, child, or the previous hour. */
  fallback: number
}

interface ParticleDebugRoot {
  wind?: ParticleDebugState
  wave?: ParticleDebugState
  weather?: WeatherDebugState
}

const DEFAULT_STATE: ParticleDebugState = {
  mounts: 0,
  active: false,
  atlasBlits: 0,
  atlasClears: 0,
  atlasFlushes: 0,
  pendingDirtyTiles: 0,
}

export function ensureParticleDebugState(layer: ParticleDebugLayer): ParticleDebugState {
  const globalWithDebug = globalThis as typeof globalThis & { __weathermanDebug?: ParticleDebugRoot }
  const root = globalWithDebug.__weathermanDebug ?? {}
  const state = root[layer] ?? { ...DEFAULT_STATE }
  root[layer] = state
  globalWithDebug.__weathermanDebug = root
  return state
}

export function ensureWeatherDebugState(): WeatherDebugState {
  const globalWithDebug = globalThis as typeof globalThis & { __weathermanDebug?: ParticleDebugRoot }
  const root = (globalWithDebug.__weathermanDebug ??= {})
  return (root.weather ??= { drawn: 0, fallback: 0 })
}
