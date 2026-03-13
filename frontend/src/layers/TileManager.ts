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
 *
 * When a TileFetchClient is provided, all network fetches are delegated to
 * a Web Worker — keeping fetch callbacks off the main thread to prevent
 * jank during rapid playback or panning. The worker returns decoded data
 * via Transferable objects (zero-copy); TileManager handles the final
 * texture upload since WebGL contexts are thread-bound.
 */

import type { TilePriority } from '@/workers/tile-fetch-protocol'
import type { TileFetchClient, TileFetchResult, TileFetchError } from '@/workers/TileFetchClient'

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
  /** Shared Web Worker client for off-thread fetching. When provided,
   *  all fetches go through the worker instead of the main thread. */
  fetchClient?: TileFetchClient
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
let _nextManagerId = 0

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

  /** Shared Web Worker client for off-thread fetching (optional). */
  private _fetchClient: TileFetchClient | null = null

  /** Unique ID for this manager instance, used to namespace worker keys. */
  private _id: string

  /** Pending worker-initiated fetches: tile key → pre-allocated placeholder texture. */
  private _pendingWorker = new Map<string, WebGLTexture>()

  /** Unsubscribe functions for worker listener cleanup. */
  private _unsubLoaded: (() => void) | null = null
  private _unsubError: (() => void) | null = null

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
    this._fetchClient = options.fetchClient ?? null
    this._id = String(_nextManagerId++)
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
   *
   * @param priority Fetch priority for the worker queue (0=highest, 2=lowest).
   *   Defaults to 0 (current viewport, current time).
   */
  updateVisibleTiles(coords: TileCoord[], priority: TilePriority = 0): void {
    // Lazily wire up worker callbacks on first use (not in constructor,
    // because onTileLoaded may not be set yet at construction time).
    if (this._fetchClient && !this._unsubLoaded) {
      this._wireWorkerCallbacks()
    }

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
      if (this._pending.has(key) || this._pendingF16.has(key) || this._pendingWorker.has(key)) continue
      this._fetchTile(z, x, y, priority)
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
    if (this._pending.has(key) || this._pendingF16.has(key) || this._pendingWorker.has(key)) return 'pending'
    return this._tiles.get(key)?.state ?? null
  }

  /** Returns true if any tiles are currently loading. */
  get isLoading(): boolean {
    return this._pending.size > 0 || this._pendingF16.size > 0 || this._pendingWorker.size > 0
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

    // Cancel and clean up worker-pending fetches
    if (this._fetchClient && this._pendingWorker.size > 0) {
      for (const [key, texture] of this._pendingWorker) {
        this._fetchClient.cancel(this._workerKey(key))
        this._gl.deleteTexture(texture)
      }
      this._pendingWorker.clear()
    }

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
    // Unsubscribe from worker events
    this._unsubLoaded?.()
    this._unsubError?.()
    this._unsubLoaded = null
    this._unsubError = null
  }

  // ── Private ──────────────────────────────────────────────────────

  private _buildUrl(z: number, x: number, y: number): string {
    const ext = this._format === 'f16' ? 'bin' : 'png'
    return `${this._apiBase}/tiles/${this._model}/${this._runId}/${this._layer}/${this._forecastHour}/data/${z}/${x}/${y}.${ext}`
  }

  private _fetchTile(z: number, x: number, y: number, priority: TilePriority = 0): void {
    if (this._fetchClient) {
      this._fetchTileViaWorker(z, x, y, priority)
    } else if (this._format === 'f16') {
      this._fetchTileF16(z, x, y)
    } else {
      this._fetchTilePng(z, x, y)
    }
  }

  /** Build a worker-namespaced key to avoid collisions between managers. */
  private _workerKey(localKey: string): string {
    return `${this._id}:${localKey}`
  }

  /** Extract the tile key from a worker-namespaced key, or null if not ours. */
  private _parseWorkerKey(workerKey: string): string | null {
    const prefix = `${this._id}:`
    return workerKey.startsWith(prefix) ? workerKey.slice(prefix.length) : null
  }

  /**
   * Delegate tile fetch to the Web Worker.
   * Creates a placeholder texture and sends the fetch request;
   * the worker callback handles texture upload when data arrives.
   */
  private _fetchTileViaWorker(z: number, x: number, y: number, priority: TilePriority = 0): void {
    const key = tileKey(z, x, y)
    const gl = this._gl
    const url = this._buildUrl(z, x, y)

    const texture = gl.createTexture()
    if (!texture) return

    // Initialize 1x1 placeholder (same as direct-fetch paths)
    gl.bindTexture(gl.TEXTURE_2D, texture)
    if (this._format === 'f16') {
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.R16F, 1, 1, 0, gl.RED, gl.HALF_FLOAT, new Uint16Array([0]))
    } else {
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, 1, 1, 0, gl.RGBA, gl.UNSIGNED_BYTE, new Uint8Array([0, 0, 0, 0]))
    }
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.NEAREST)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.NEAREST)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE)
    gl.bindTexture(gl.TEXTURE_2D, null)

    this._pendingWorker.set(key, texture)
    // Use namespaced key in worker to avoid collisions between managers
    this._fetchClient!.fetch(this._workerKey(key), url, this._format, priority)
  }

  /**
   * Wire up worker callbacks. Called once lazily from updateVisibleTiles
   * so that onTileLoaded is already set by the time callbacks fire.
   *
   * Because TileFetchClient is a singleton shared across TileManagers,
   * each manager registers its own listener and filters results by
   * checking _pendingWorker membership (only the manager that requested
   * a tile will have that key in its pending map).
   */
  private _wireWorkerCallbacks(): void {
    const client = this._fetchClient!

    this._unsubLoaded = client.addLoadedListener((result: TileFetchResult) => {
      const localKey = this._parseWorkerKey(result.key)
      if (!localKey) return // Not our namespace
      const texture = this._pendingWorker.get(localKey)
      if (!texture) {
        // Tile was cancelled/cleared while in-flight — free transferred data
        if (result.format === 'png' && result.data instanceof ImageBitmap) {
          result.data.close()
        }
        return
      }

      this._pendingWorker.delete(localKey)
      this._uploadWorkerResult(localKey, texture, result)
    })

    this._unsubError = client.addErrorListener((error: TileFetchError) => {
      const localKey = this._parseWorkerKey(error.key)
      if (!localKey) return
      const texture = this._pendingWorker.get(localKey)
      if (!texture) return

      this._pendingWorker.delete(localKey)
      console.warn(`[TileManager] Worker fetch failed: ${localKey} — ${error.error}`)
      this._tiles.set(localKey, {
        key: localKey,
        texture,
        state: 'error',
        lastAccess: ++this._accessCounter,
      })
    })
  }

  /**
   * Upload tile data received from the worker into the pre-allocated texture.
   * Handles both PNG (ImageBitmap) and Float16 (ArrayBuffer) formats.
   */
  private _uploadWorkerResult(key: string, texture: WebGLTexture, result: TileFetchResult): void {
    const gl = this._gl

    if (result.format === 'f16') {
      const buffer = result.data as ArrayBuffer
      const side = result.side ?? -1

      if (side <= 0) {
        console.warn(`[TileManager] Float16 tile from worker has non-square size`)
        this._tiles.set(key, { key, texture, state: 'error', lastAccess: ++this._accessCounter })
        return
      }

      gl.bindTexture(gl.TEXTURE_2D, texture)
      gl.texImage2D(
        gl.TEXTURE_2D, 0, gl.R16F,
        side, side, 0,
        gl.RED, gl.HALF_FLOAT,
        new Uint16Array(buffer),
      )
      gl.bindTexture(gl.TEXTURE_2D, null)
    } else {
      // PNG: ImageBitmap — can be uploaded directly via texImage2D
      const bitmap = result.data as ImageBitmap
      gl.bindTexture(gl.TEXTURE_2D, texture)
      gl.texImage2D(
        gl.TEXTURE_2D, 0, gl.RGBA,
        gl.RGBA, gl.UNSIGNED_BYTE,
        bitmap,
      )
      gl.bindTexture(gl.TEXTURE_2D, null)
      bitmap.close()
    }

    this._tiles.set(key, {
      key,
      texture,
      state: 'loaded',
      lastAccess: ++this._accessCounter,
    })
    this.onTileLoaded?.()
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

// ── Pan prefetch ─────────────────────────────────────────────────

/** Direction of viewport movement in map coordinate space. */
export interface PanDirection {
  /** Positive = east, negative = west (degrees longitude). */
  dx: number
  /** Positive = north, negative = south (degrees latitude). */
  dy: number
}

/**
 * Tracks viewport center movement between frames to determine pan direction.
 *
 * Returns a non-null PanDirection when the viewport center has moved by
 * more than a small threshold since the previous update — indicating
 * the user is actively panning.
 */
export class PanVelocityTracker {
  private _prevLng = NaN
  private _prevLat = NaN

  /** Minimum center movement in degrees to register as panning. */
  private static readonly THRESHOLD = 0.001

  /**
   * Feed the current viewport center and get the movement direction.
   * Returns null on the first call or when the viewport is stationary.
   */
  update(centerLng: number, centerLat: number): PanDirection | null {
    const prevLng = this._prevLng
    const prevLat = this._prevLat
    this._prevLng = centerLng
    this._prevLat = centerLat

    if (isNaN(prevLng)) return null

    let dx = centerLng - prevLng
    const dy = centerLat - prevLat

    // Handle antimeridian wrapping
    if (dx > 180) dx -= 360
    if (dx < -180) dx += 360

    if (Math.abs(dx) < PanVelocityTracker.THRESHOLD &&
        Math.abs(dy) < PanVelocityTracker.THRESHOLD) {
      return null
    }

    return { dx, dy }
  }

  /** Reset tracking (e.g. on config change or layer swap). */
  reset(): void {
    this._prevLng = NaN
    this._prevLat = NaN
  }
}

/**
 * Compute one ring of tiles beyond the visible set in the direction of
 * viewport movement. Used during panning to prevent blank tiles at
 * viewport edges.
 *
 * Returns only tiles that are not already in the visible set.
 * Handles antimeridian wrapping for x coordinates and clamps y to
 * valid tile range (no tiles beyond the poles).
 */
export function computePanPrefetchTiles(
  visible: TileCoord[],
  direction: PanDirection,
  z: number,
): TileCoord[] {
  if (visible.length === 0) return []

  const n = 2 ** z

  // Find bounding box of visible tiles
  let xMin = Infinity, xMax = -Infinity
  let yMin = Infinity, yMax = -Infinity
  for (const { x, y } of visible) {
    if (x < xMin) xMin = x
    if (x > xMax) xMax = x
    if (y < yMin) yMin = y
    if (y > yMax) yMax = y
  }

  const visibleSet = new Set(visible.map(c => `${c.x},${c.y}`))
  const prefetch: TileCoord[] = []

  const addIfNew = (x: number, y: number) => {
    if (y < 0 || y >= n) return // beyond poles
    x = ((x % n) + n) % n // antimeridian wrap
    const key = `${x},${y}`
    if (visibleSet.has(key)) return
    visibleSet.add(key) // prevent duplicates in prefetch set
    prefetch.push({ z, x, y })
  }

  // One column/row in the direction of movement
  if (direction.dx > 0) {
    // Panning east → prefetch east column
    for (let y = yMin; y <= yMax; y++) addIfNew(xMax + 1, y)
  }
  if (direction.dx < 0) {
    // Panning west → prefetch west column
    for (let y = yMin; y <= yMax; y++) addIfNew(xMin - 1, y)
  }
  if (direction.dy > 0) {
    // Panning north → lower tile-y → prefetch row above
    for (let x = xMin; x <= xMax; x++) addIfNew(x, yMin - 1)
  }
  if (direction.dy < 0) {
    // Panning south → higher tile-y → prefetch row below
    for (let x = xMin; x <= xMax; x++) addIfNew(x, yMax + 1)
  }

  // Diagonal corner tiles (if panning diagonally)
  if (direction.dx > 0 && direction.dy > 0) addIfNew(xMax + 1, yMin - 1)
  if (direction.dx > 0 && direction.dy < 0) addIfNew(xMax + 1, yMax + 1)
  if (direction.dx < 0 && direction.dy > 0) addIfNew(xMin - 1, yMin - 1)
  if (direction.dx < 0 && direction.dy < 0) addIfNew(xMin - 1, yMax + 1)

  return prefetch
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
