/**
 * WebGL tile manager for data-encoded weather tiles.
 *
 * Manages the lifecycle of fetching data tiles and uploading them
 * as WebGL textures. Handles tile URL construction, fetch, texture upload,
 * LRU eviction, and provides texture lookups for the weather fragment shader.
 *
 * Supports two tile formats:
 *   - **PNG** (default): RGBA where float32 values are encoded as 16-bit uint
 *     in R (low) + G (high), B = nodata flag. Shader decodes manually.
 *   - **Float16**: Raw IEEE 754 half-precision binary. Uploaded as R16F
 *     textures with physical values stored directly. Nodata = -9999.0.
 */

/** Loading state for a single tile. */
export type TileState = 'pending' | 'loaded' | 'error'

/** Tile data format. */
export type TileFormat = 'png' | 'f16'

/** A single cached tile with its WebGL texture and metadata. */
interface TileEntry {
  key: string
  texture: WebGLTexture
  state: TileState
  /** Monotonically increasing access counter for LRU ordering. */
  lastAccess: number
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
  /** Tile format: 'png' (default) or 'f16' (Float16 binary). */
  format?: TileFormat
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
  private _format: TileFormat

  /** Current dataset parameters. Changing these invalidates the cache. */
  private _model = ''
  private _runId = ''
  private _layer = ''
  private _forecastHour = 0

  /** All cached tiles keyed by "z/x/y". */
  private _tiles = new Map<string, TileEntry>()

  /** Monotonically increasing counter for LRU tracking. */
  private _accessCounter = 0

  /** Guard to emit the all-tiles-error warning only once. */
  private _allErrorWarned = false


  /** In-flight fetches keyed by tile key, with Image + texture refs for cleanup. */
  private _pending = new Map<string, { image: HTMLImageElement; texture: WebGLTexture }>()

  /** In-flight Float16 fetches with AbortController for cancellation. */
  private _pendingF16 = new Map<string, { abort: AbortController; texture: WebGLTexture }>()

  /** Callback invoked when a tile finishes loading (for triggering repaint). */
  onTileLoaded: (() => void) | null = null

  /** Whether this manager fetches Float16 binary tiles. */
  get isFloat16(): boolean {
    return this._format === 'f16'
  }

  constructor(gl: WebGL2RenderingContext, options: TileManagerOptions = {}) {
    this._gl = gl
    this._apiBase = options.apiBase ?? ''
    this._maxTextures = options.maxTextures ?? 128
    this._format = options.format ?? 'png'
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
        // Don't bump access counter for errored tiles — let them be
        // evicted so they can be retried on the next updateVisibleTiles call.
        if (existing.state === 'error') continue
        existing.lastAccess = ++this._accessCounter
        continue
      }
      if (this._pending.has(key) || this._pendingF16.has(key)) continue
      this._fetchTile(z, x, y)
    }
    this._evict()

    // One-time warning when all requested tiles are in error state
    if (coords.length > 0 && !this._allErrorWarned) {
      const allError = coords.every(({ z, x, y }) => {
        const state = this.getTileState(z, x, y)
        return state === 'error'
      })
      if (allError) {
        this._allErrorWarned = true
        console.warn(
          `[TileManager] All ${coords.length} visible tiles are in error state — check tile server and COG paths`,
        )
      } else {
        this._allErrorWarned = false
      }
    }
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
    if (this._pending.has(key) || this._pendingF16.has(key)) return 'pending'
    return this._tiles.get(key)?.state ?? null
  }

  /** Returns true if any tiles are currently loading. */
  get isLoading(): boolean {
    return this._pending.size > 0 || this._pendingF16.size > 0
  }

  /** Number of textures currently cached. */
  get cacheSize(): number {
    return this._tiles.size
  }

  /** Current layer name this manager is fetching. */
  get currentLayer(): string {
    return this._layer
  }

  /** Current forecast hour this manager is fetching. */
  get currentForecastHour(): number {
    return this._forecastHour
  }

  /** Clear all cached textures and abort pending fetches. */
  clear(): void {
    // Abort in-flight Image loads and delete their placeholder textures
    for (const { image, texture } of this._pending.values()) {
      image.onload = null
      image.onerror = null
      image.src = ''
      this._gl.deleteTexture(texture)
    }
    this._pending.clear()

    // Abort in-flight Float16 fetches
    for (const { abort, texture } of this._pendingF16.values()) {
      abort.abort()
      this._gl.deleteTexture(texture)
    }
    this._pendingF16.clear()

    // Delete all cached textures
    for (const entry of this._tiles.values()) {
      this._gl.deleteTexture(entry.texture)
    }
    this._tiles.clear()
    this._accessCounter = 0
    this._allErrorWarned = false
  }

  /** Release all GL resources. Call when done with this manager. */
  destroy(): void {
    this.clear()
    this.onTileLoaded = null
  }

  // ── Private ──────────────────────────────────────────────────────

  private _buildUrl(z: number, x: number, y: number): string {
    const ext = this._format === 'f16' ? 'bin' : 'png'
    return `${this._apiBase}/tiles/${this._model}/${this._runId}/${this._layer}/${this._forecastHour}/data/${z}/${x}/${y}.${ext}`
  }

  private _fetchTile(z: number, x: number, y: number): void {
    if (this._format === 'f16') {
      this._fetchTileF16(z, x, y)
    } else {
      this._fetchTilePng(z, x, y)
    }
  }

  /** Fetch a Float16 binary tile and upload as R16F texture. */
  private _fetchTileF16(z: number, x: number, y: number): void {
    const key = tileKey(z, x, y)
    const gl = this._gl
    const url = this._buildUrl(z, x, y)

    const texture = gl.createTexture()
    if (!texture) return

    // Initialize with 1x1 placeholder (R16F)
    gl.bindTexture(gl.TEXTURE_2D, texture)
    gl.texImage2D(
      gl.TEXTURE_2D, 0, gl.R16F,
      1, 1, 0,
      gl.RED, gl.HALF_FLOAT,
      new Uint16Array([0]),
    )
    // GL_NEAREST — manual bilinear in shader for nodata handling
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.NEAREST)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.NEAREST)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE)
    gl.bindTexture(gl.TEXTURE_2D, null)

    const abort = new AbortController()
    this._pendingF16.set(key, { abort, texture })

    fetch(url, { signal: abort.signal })
      .then(resp => {
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`)
        return resp.arrayBuffer()
      })
      .then(buffer => {
        if (!this._pendingF16.has(key)) {
          gl.deleteTexture(texture)
          return
        }
        this._pendingF16.delete(key)

        // Determine tile dimensions from buffer size (assumes square tiles)
        const pixelCount = buffer.byteLength / 2  // 2 bytes per float16
        const side = Math.sqrt(pixelCount)
        if (side !== Math.floor(side)) {
          console.warn(`[TileManager] Float16 tile has non-square size: ${pixelCount} pixels`)
          this._tiles.set(key, { key, texture, state: 'error', lastAccess: ++this._accessCounter })
          return
        }

        // Upload Float16 data as R16F texture
        gl.bindTexture(gl.TEXTURE_2D, texture)
        gl.texImage2D(
          gl.TEXTURE_2D, 0, gl.R16F,
          side, side, 0,
          gl.RED, gl.HALF_FLOAT,
          new Uint16Array(buffer),
        )
        gl.bindTexture(gl.TEXTURE_2D, null)

        this._tiles.set(key, {
          key,
          texture,
          state: 'loaded',
          lastAccess: ++this._accessCounter,
        })
        this.onTileLoaded?.()
      })
      .catch(err => {
        if (err.name === 'AbortError') return
        console.warn(`[TileManager] Failed to load Float16 tile: ${url}`, err)
        if (!this._pendingF16.has(key)) {
          gl.deleteTexture(texture)
          return
        }
        this._pendingF16.delete(key)
        this._tiles.set(key, { key, texture, state: 'error', lastAccess: ++this._accessCounter })
      })
  }

  /** Fetch a PNG data tile and upload as RGBA texture (original path). */
  private _fetchTilePng(z: number, x: number, y: number): void {
    const key = tileKey(z, x, y)
    const gl = this._gl
    const url = this._buildUrl(z, x, y)

    const texture = gl.createTexture()
    if (!texture) {
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

    // Store in pending map so clear() can abort and delete the texture
    this._pending.set(key, { image: img, texture })

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

      this._tiles.set(key, {
        key,
        texture,
        state: 'loaded',
        lastAccess: ++this._accessCounter,
      })
      this.onTileLoaded?.()
    }

    img.onerror = () => {
      console.warn(`[TileManager] Failed to load tile: ${url}`)
      if (!this._pending.has(key)) {
        gl.deleteTexture(texture)
        return
      }
      this._pending.delete(key)

      this._tiles.set(key, {
        key,
        texture,
        state: 'error',
        lastAccess: ++this._accessCounter,
      })
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
