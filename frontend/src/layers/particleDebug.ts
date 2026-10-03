export type ParticleDebugLayer = 'wind' | 'wave'

export interface ParticleDebugState {
  mounts: number
  active: boolean
  atlasBlits: number
  atlasClears: number
  atlasFlushes: number
  pendingDirtyTiles: number
  /** Opacity last set on the layer. */
  opacity?: number
  /** Wind only: forecast hour of the current (T0) tiles. */
  hour?: number
  /** Wind only: particles drawn in the last frame. */
  drawnParticles?: number
  /** Trail buffer size, and its pixels per CSS pixel (#93). */
  trailWidth?: number
  trailHeight?: number
  trailPixelRatio?: number
  /** Wave only: the last frame needed more grid cells than there are dash slots. */
  gridTruncated?: boolean
}

/** What the weather colour layer drew in its last frame (for the e2e suite). */
export interface WeatherDebugState {
  /** Quads drawn. */
  drawn: number
  /** Of those, drawn from a stand-in: an ancestor, child, or the previous hour. */
  fallback: number
  /** Forecast hour the current (T0) tiles are fetched for. */
  hour?: number
  /** Opacity last set on the layer. */
  opacity?: number
  /** Blend towards the next hour actually drawn (0 when not blending). */
  mix?: number
  /** Frames that redrew the tiles into the offscreen buffer. */
  tilePasses: number
  /** Frames that composited the buffer onto the map. */
  composites: number
}

interface ParticleDebugRoot {
  wind?: ParticleDebugState
  wave?: ParticleDebugState
  weather?: WeatherDebugState
  isobars?: IsobarsDebugState
  tiles?: TileDebugState
}

/** Data-tile fetches started by the shared tile store, per URL. */
export interface TileDebugState {
  fetches: Record<string, number>
}

/** What the isobar overlay last put on the map. */
export interface IsobarsDebugState {
  features: number
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
  return (root.weather ??= { drawn: 0, fallback: 0, tilePasses: 0, composites: 0 })
}

export function ensureIsobarsDebugState(): IsobarsDebugState {
  const globalWithDebug = globalThis as typeof globalThis & { __weathermanDebug?: ParticleDebugRoot }
  const root = (globalWithDebug.__weathermanDebug ??= {})
  return (root.isobars ??= { features: 0 })
}

export function ensureTileDebugState(): TileDebugState {
  const globalWithDebug = globalThis as typeof globalThis & { __weathermanDebug?: ParticleDebugRoot }
  const root = (globalWithDebug.__weathermanDebug ??= {})
  return (root.tiles ??= { fetches: {} })
}
