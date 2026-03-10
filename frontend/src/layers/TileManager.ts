/**
 * WebGL tile manager for data-encoded weather tiles.
 *
 * Manages the lifecycle of fetching data tile PNGs and uploading them
 * as WebGL textures. Handles tile URL construction, Image-based fetch,
 * texture upload, LRU eviction, and provides texture lookups for the
 * weather fragment shader.
 *
 * Data tiles are RGBA PNGs where float32 values are encoded as:
 *   R = low byte of uint16, G = high byte, B = nodata flag (0xFF), A = 0xFF
 * The shader decodes these back to physical values on the GPU.
 */

/** Loading state for a single tile. */
export type TileState = 'pending' | 'loaded' | 'error'

/** A single cached tile with its WebGL texture and metadata. */
interface TileEntry {
  key: string
  texture: WebGLTexture
  state: TileState
  /** Monotonically increasing access counter for LRU ordering. */
  lastAccess: number
  /** Image element used for loading (kept to allow abort via src = ''). */
  image: HTMLImageElement | null
}

/** Tile coordinate triplet. */
export interface TileCoord {
  z: number
  x: number
  y: number
}

export interface TileManagerOptions {
  /** Base URL for the data tile API (e.g. '' for same-origin). */
  apiBase?: string
  /** Maximum number of textures to keep in GPU memory. Default: 128. */
  maxTextures?: number
}

/**
 * Manages data tile fetching and WebGL texture lifecycle.
 *
 * Usage:
 *   const mgr = new TileManager(gl, { apiBase: '' })
 *   mgr.setLayer('gfs', 'latest', 'temperature', 0)
 *   mgr.updateVisibleTiles(visibleCoords)
 *   // In render loop:
 *   const tex = mgr.getTexture(z, x, y)
 */
export class TileManager {
  private _gl: WebGL2RenderingContext
  private _apiBase: string
  private _maxTextures: number

  /** Current dataset parameters. Changing these invalidates the cache. */
  private _model = ''
  private _runId = ''
  private _layer = ''
  private _forecastHour = 0

  /** All cached tiles keyed by "z/x/y". */
  private _tiles = new Map<string, TileEntry>()

  /** Monotonically increasing counter for LRU tracking. */
  private _accessCounter = 0

  /** Set of tile keys currently being fetched. */
  private _pending = new Set<string>()

  /** Callback invoked when a tile finishes loading (for triggering repaint). */
  onTileLoaded: (() => void) | null = null

  constructor(gl: WebGL2RenderingContext, options: TileManagerOptions = {}) {
    this._gl = gl
    this._apiBase = options.apiBase ?? ''
    this._maxTextures = options.maxTextures ?? 128
  }

  /**
   * Set the current dataset to fetch tiles for.
   * Clears the cache if any parameter changed.
   */
  setLayer(model: string, runId: string, layer: string, forecastHour: number): void {
    if (
      model === this._model &&
      runId === this._runId &&
      layer === this._layer &&
      forecastHour === this._forecastHour
    ) {
      return
    }
    this.clear()
    this._model = model
    this._runId = runId
    this._layer = layer
    this._forecastHour = forecastHour
  }

  /**
   * Request tiles for the given visible coordinates.
   * Starts fetching any tiles not already cached or pending.
   */
  updateVisibleTiles(coords: TileCoord[]): void {
    for (const { z, x, y } of coords) {
      const key = tileKey(z, x, y)
      const existing = this._tiles.get(key)
      if (existing) {
        existing.lastAccess = ++this._accessCounter
        continue
      }
      if (this._pending.has(key)) continue
      this._fetchTile(z, x, y)
    }
    this._evict()
  }

  /**
   * Get the texture for a tile, or null if not yet loaded.
   * Updates the LRU access counter.
   */
  getTexture(z: number, x: number, y: number): WebGLTexture | null {
    const entry = this._tiles.get(tileKey(z, x, y))
    if (!entry || entry.state !== 'loaded') return null
    entry.lastAccess = ++this._accessCounter
    return entry.texture
  }

  /** Get the loading state for a tile. */
  getTileState(z: number, x: number, y: number): TileState | null {
    const key = tileKey(z, x, y)
    if (this._pending.has(key)) return 'pending'
    const entry = this._tiles.get(key)
    return entry?.state ?? null
  }

  /** Returns true if any tiles are currently loading. */
  get isLoading(): boolean {
    return this._pending.size > 0
  }

  /** Number of textures currently cached. */
  get cacheSize(): number {
    return this._tiles.size
  }

  /** Clear all cached textures and abort pending fetches. */
  clear(): void {
    // Abort pending loads
    for (const entry of this._tiles.values()) {
      if (entry.image) {
        entry.image.onload = null
        entry.image.onerror = null
        entry.image.src = ''
        entry.image = null
      }
      this._gl.deleteTexture(entry.texture)
    }
    this._tiles.clear()
    this._pending.clear()
    this._accessCounter = 0
  }

  /** Release all GL resources. Call when done with this manager. */
  destroy(): void {
    this.clear()
    this.onTileLoaded = null
  }

  // ── Private ──────────────────────────────────────────────────────

  private _buildUrl(z: number, x: number, y: number): string {
    return `${this._apiBase}/tiles/${this._model}/${this._runId}/${this._layer}/${this._forecastHour}/data/${z}/${x}/${y}.png`
  }

  private _fetchTile(z: number, x: number, y: number): void {
    const key = tileKey(z, x, y)
    this._pending.add(key)

    const gl = this._gl
    const url = this._buildUrl(z, x, y)

    // Create a placeholder texture (1x1 transparent) so we can upload later
    const texture = gl.createTexture()
    if (!texture) {
      this._pending.delete(key)
      return
    }

    // Initialize with 1x1 transparent pixel as placeholder
    gl.bindTexture(gl.TEXTURE_2D, texture)
    gl.texImage2D(
      gl.TEXTURE_2D, 0, gl.RGBA,
      1, 1, 0,
      gl.RGBA, gl.UNSIGNED_BYTE,
      new Uint8Array([0, 0, 0, 0]),
    )
    // GL_NEAREST — no interpolation of encoded data bytes
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.NEAREST)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.NEAREST)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE)
    gl.bindTexture(gl.TEXTURE_2D, null)

    const img = new Image()
    img.crossOrigin = 'anonymous'

    img.onload = () => {
      // Check we haven't been cleared/destroyed while loading
      if (!this._pending.has(key)) {
        gl.deleteTexture(texture)
        return
      }
      this._pending.delete(key)

      // Upload image data to texture
      gl.bindTexture(gl.TEXTURE_2D, texture)
      gl.texImage2D(
        gl.TEXTURE_2D, 0, gl.RGBA,
        gl.RGBA, gl.UNSIGNED_BYTE,
        img,
      )
      gl.bindTexture(gl.TEXTURE_2D, null)

      const entry: TileEntry = {
        key,
        texture,
        state: 'loaded',
        lastAccess: ++this._accessCounter,
        image: null,
      }
      this._tiles.set(key, entry)
      this.onTileLoaded?.()
    }

    img.onerror = () => {
      if (!this._pending.has(key)) {
        gl.deleteTexture(texture)
        return
      }
      this._pending.delete(key)

      const entry: TileEntry = {
        key,
        texture,
        state: 'error',
        lastAccess: ++this._accessCounter,
        image: null,
      }
      this._tiles.set(key, entry)
    }

    img.src = url
  }

  /** Evict least-recently-used tiles when over the cache limit. */
  private _evict(): void {
    if (this._tiles.size <= this._maxTextures) return

    // Collect entries sorted by last access (ascending = oldest first)
    const entries = [...this._tiles.values()]
      .sort((a, b) => a.lastAccess - b.lastAccess)

    const toRemove = this._tiles.size - this._maxTextures
    for (let i = 0; i < toRemove; i++) {
      const entry = entries[i]
      if (entry.image) {
        entry.image.onload = null
        entry.image.onerror = null
        entry.image.src = ''
      }
      this._gl.deleteTexture(entry.texture)
      this._tiles.delete(entry.key)
    }
  }
}

// ── Utility ──────────────────────────────────────────────────────

function tileKey(z: number, x: number, y: number): string {
  return `${z}/${x}/${y}`
}

// ── Visible tile computation ─────────────────────────────────────
// Extracted from useWeatherLayer for reuse by the GL pipeline.

function wrapLon(lng: number): number {
  return ((lng + 180) % 360 + 360) % 360 - 180
}

function lngLatToTile(lng: number, lat: number, z: number): { x: number; y: number } {
  const n = 2 ** z
  const wrappedLng = wrapLon(lng)
  const x = Math.floor(((wrappedLng + 180) / 360) * n)
  const latRad = (lat * Math.PI) / 180
  const merc = Math.log(Math.tan(Math.PI / 4 + latRad / 2))
  const y = Math.floor(((1 - merc / Math.PI) / 2) * n)
  return {
    x: Math.max(0, Math.min(n - 1, x)),
    y: Math.max(0, Math.min(n - 1, y)),
  }
}

/**
 * Compute visible tile coordinates for a given map viewport and zoom level.
 * Handles antimeridian wrapping.
 */
export function computeVisibleTiles(
  bounds: { west: number; north: number; east: number; south: number },
  z: number,
): TileCoord[] {
  const northWest = lngLatToTile(bounds.west, bounds.north, z)
  const southEast = lngLatToTile(bounds.east, bounds.south, z)
  const n = 2 ** z
  const xs: number[] = []

  if (northWest.x <= southEast.x) {
    for (let x = northWest.x; x <= southEast.x; x++) xs.push(x)
  } else {
    // Antimeridian crossing
    for (let x = northWest.x; x < n; x++) xs.push(x)
    for (let x = 0; x <= southEast.x; x++) xs.push(x)
  }

  const tiles: TileCoord[] = []
  const yStart = Math.min(northWest.y, southEast.y)
  const yEnd = Math.max(northWest.y, southEast.y)
  for (const x of xs) {
    for (let y = yStart; y <= yEnd; y++) {
      tiles.push({ z, x, y })
    }
  }
  return tiles
}
