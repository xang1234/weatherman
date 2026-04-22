export type ParticleDebugLayer = 'wind' | 'wave'

export interface ParticleDebugState {
  mounts: number
  active: boolean
  atlasBlits: number
  atlasClears: number
  atlasFlushes: number
  pendingDirtyTiles: number
}

interface ParticleDebugRoot {
  wind?: ParticleDebugState
  wave?: ParticleDebugState
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
