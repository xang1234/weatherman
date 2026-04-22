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

/** Tile coordinate with world-copy tracking for antimeridian support. */
export interface TileCoord {
  z: number
  x: number  // canonical tile X [0, n-1] — used for fetching and cache lookup
  y: number
  wrap: number  // world copy offset: 0 = primary, 1 = next copy east
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

interface DatasetConfig {
  model: string
  runId: string
  layer: string
  forecastHour: number
}

interface DatasetState {
  config: DatasetConfig
  tiles: Map<string, TileEntry>
  pending: Map<string, { image: HTMLImageElement; texture: WebGLTexture }>
  pendingF16: Map<string, { abort: AbortController; texture: WebGLTexture }>
  pendingWorker: Map<string, WebGLTexture>
  allErrorWarned: boolean
  lastUsed: number
}

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

  /** Dataset caches keyed by "model/run/layer/hour". */
  private _datasets = new Map<string, DatasetState>()

  /** Monotonically increasing counter for LRU tracking. */
  private _accessCounter = 0

  /** Shared Web Worker client for off-thread fetching (optional). */
  private _fetchClient: TileFetchClient | null = null

  /** Unique ID for this manager instance, used to namespace worker keys. */
  private _id: string

  /** Unsubscribe functions for worker listener cleanup. */
  private _unsubLoaded: (() => void) | null = null
  private _unsubError: (() => void) | null = null

  /** Callback invoked when a tile for the current dataset finishes loading. */
  onTileLoaded: ((key: string) => void) | null = null

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
   * Switching datasets reuses any cached tiles already kept for that key.
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
    this._model = model
    this._runId = runId
    this._layer = layer
    this._forecastHour = forecastHour
    this._ensureCurrentState()
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

    const state = this._ensureCurrentState()
    if (!state) return
    state.lastUsed = ++this._accessCounter

    for (const { z, x, y } of coords) {
      const key = tileKey(z, x, y)
      const existing = state.tiles.get(key)
      if (existing) {
        // Don't bump access counter for errored tiles — let them be
        // evicted so they can be retried on the next updateVisibleTiles call.
        if (existing.state === 'error') continue
        existing.lastAccess = ++this._accessCounter
        continue
      }
      if (state.pending.has(key) || state.pendingF16.has(key) || state.pendingWorker.has(key)) continue
      this._fetchTile(state, z, x, y, priority)
    }
    this._evict()

    // One-time warning when all requested tiles are in error state
    if (coords.length > 0 && !state.allErrorWarned) {
      const allError = coords.every(({ z, x, y }) => {
        const tileState = this.getTileState(z, x, y)
        return tileState === 'error'
      })
      if (allError) {
        state.allErrorWarned = true
        console.warn(
          `[TileManager] All ${coords.length} visible tiles are in error state — check tile server and COG paths`,
        )
      } else {
        state.allErrorWarned = false
      }
    }
  }

  /**
   * Get the texture for a tile, or null if not yet loaded.
   * Updates the LRU access counter.
   */
  getTexture(z: number, x: number, y: number): WebGLTexture | null {
    const entry = this._currentState()?.tiles.get(tileKey(z, x, y))
    if (!entry || entry.state !== 'loaded') return null
    entry.lastAccess = ++this._accessCounter
    return entry.texture
  }

  /** Get the loading state for a tile. */
  getTileState(z: number, x: number, y: number): TileState | null {
    const key = tileKey(z, x, y)
    const state = this._currentState()
    if (!state) return null
    if (state.pending.has(key) || state.pendingF16.has(key) || state.pendingWorker.has(key)) return 'pending'
    return state.tiles.get(key)?.state ?? null
  }

  /** Returns true if any tiles are currently loading. */
  get isLoading(): boolean {
    const state = this._currentState()
    return state != null && (
      state.pending.size > 0 ||
      state.pendingF16.size > 0 ||
      state.pendingWorker.size > 0
    )
  }

  /** Number of textures currently cached. */
  get cacheSize(): number {
    return this._currentState()?.tiles.size ?? 0
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
    for (const [datasetKey, state] of this._datasets) {
      this._disposeDatasetState(datasetKey, state)
    }
    this._datasets.clear()
    this._accessCounter = 0
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

  private _datasetKey(config: DatasetConfig): string {
    return `${config.model}/${config.runId}/${config.layer}/${config.forecastHour}`
  }

  private _currentDatasetKey(): string | null {
    if (!this._model || !this._runId || !this._layer) return null
    return this._datasetKey({
      model: this._model,
      runId: this._runId,
      layer: this._layer,
      forecastHour: this._forecastHour,
    })
  }

  private _currentState(): DatasetState | null {
    if (!this._model || !this._runId || !this._layer) return null
    return this._datasets.get(this._datasetKey({
      model: this._model,
      runId: this._runId,
      layer: this._layer,
      forecastHour: this._forecastHour,
    })) ?? null
  }

  private _ensureCurrentState(): DatasetState | null {
    if (!this._model || !this._runId || !this._layer) return null
    const config: DatasetConfig = {
      model: this._model,
      runId: this._runId,
      layer: this._layer,
      forecastHour: this._forecastHour,
    }
    const datasetKey = this._datasetKey(config)
    let state = this._datasets.get(datasetKey)
    if (!state) {
      state = {
        config,
        tiles: new Map(),
        pending: new Map(),
        pendingF16: new Map(),
        pendingWorker: new Map(),
        allErrorWarned: false,
        lastUsed: ++this._accessCounter,
      }
      this._datasets.set(datasetKey, state)
    } else {
      state.config = config
      state.lastUsed = ++this._accessCounter
    }
    return state
  }

  private _notifyTileLoaded(state: DatasetState, key: string): void {
    const currentDatasetKey = this._currentDatasetKey()
    if (!currentDatasetKey) return
    if (this._datasetKey(state.config) !== currentDatasetKey) return
    this.onTileLoaded?.(key)
  }

  private _buildUrl(config: DatasetConfig, z: number, x: number, y: number): string {
    const ext = this._format === 'f16' ? 'bin' : 'png'
    return `${this._apiBase}/tiles/${config.model}/${config.runId}/${config.layer}/${config.forecastHour}/data/${z}/${x}/${y}.${ext}`
  }

  private _fetchTile(state: DatasetState, z: number, x: number, y: number, priority: TilePriority = 0): void {
    if (this._fetchClient) {
      this._fetchTileViaWorker(state, z, x, y, priority)
    } else if (this._format === 'f16') {
      this._fetchTileF16(state, z, x, y)
    } else {
      this._fetchTilePng(state, z, x, y)
    }
  }

  /** Build a worker-namespaced key to avoid collisions between managers. */
  private _workerKey(datasetKey: string, localKey: string): string {
    return `${this._id}::${datasetKey}::${localKey}`
  }

  /** Extract the dataset/tile key pair from a worker-namespaced key, or null if not ours. */
  private _parseWorkerKey(workerKey: string): { datasetKey: string; localKey: string } | null {
    const prefix = `${this._id}::`
    if (!workerKey.startsWith(prefix)) return null
    const rest = workerKey.slice(prefix.length)
    const separator = rest.indexOf('::')
    if (separator === -1) return null
    return {
      datasetKey: rest.slice(0, separator),
      localKey: rest.slice(separator + 2),
    }
  }

  /**
   * Delegate tile fetch to the Web Worker.
   * Creates a placeholder texture and sends the fetch request;
   * the worker callback handles texture upload when data arrives.
   */
  private _fetchTileViaWorker(
    state: DatasetState,
    z: number,
    x: number,
    y: number,
    priority: TilePriority = 0,
  ): void {
    const key = tileKey(z, x, y)
    const gl = this._gl
    const datasetKey = this._datasetKey(state.config)
    const url = this._buildUrl(state.config, z, x, y)

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

    state.pendingWorker.set(key, texture)
    // Use namespaced key in worker to avoid collisions between managers
    this._fetchClient!.fetch(this._workerKey(datasetKey, key), url, this._format, priority)
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
      const parsed = this._parseWorkerKey(result.key)
      if (!parsed) return
      const state = this._datasets.get(parsed.datasetKey)
      const texture = state?.pendingWorker.get(parsed.localKey)
      if (!texture) {
        // Tile was cancelled/cleared while in-flight — free transferred data
        if (result.format === 'png' && result.data instanceof ImageBitmap) {
          result.data.close()
        }
        return
      }

      state!.pendingWorker.delete(parsed.localKey)
      this._uploadWorkerResult(state!, parsed.localKey, texture, result)
    })

    this._unsubError = client.addErrorListener((error: TileFetchError) => {
      const parsed = this._parseWorkerKey(error.key)
      if (!parsed) return
      const state = this._datasets.get(parsed.datasetKey)
      const texture = state?.pendingWorker.get(parsed.localKey)
      if (!texture) return

      state!.pendingWorker.delete(parsed.localKey)
      console.warn(`[TileManager] Worker fetch failed: ${parsed.localKey} — ${error.error}`)
      state!.tiles.set(parsed.localKey, {
        key: parsed.localKey,
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
  private _uploadWorkerResult(
    state: DatasetState,
    key: string,
    texture: WebGLTexture,
    result: TileFetchResult,
  ): void {
    const gl = this._gl

    if (result.format === 'f16') {
      const buffer = result.data as ArrayBuffer
      const side = result.side ?? -1

      if (side <= 0) {
        console.warn(`[TileManager] Float16 tile from worker has non-square size`)
        state.tiles.set(key, { key, texture, state: 'error', lastAccess: ++this._accessCounter })
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

    state.tiles.set(key, {
      key,
      texture,
      state: 'loaded',
      lastAccess: ++this._accessCounter,
    })
    this._notifyTileLoaded(state, key)
  }

  /** Fetch a Float16 binary tile and upload as R16F texture. */
  private _fetchTileF16(state: DatasetState, z: number, x: number, y: number): void {
    const key = tileKey(z, x, y)
    const gl = this._gl
    const url = this._buildUrl(state.config, z, x, y)

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
    state.pendingF16.set(key, { abort, texture })

    fetch(url, { signal: abort.signal })
      .then(resp => {
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`)
        return resp.arrayBuffer()
      })
      .then(buffer => {
        if (!state.pendingF16.has(key)) {
          gl.deleteTexture(texture)
          return
        }
        state.pendingF16.delete(key)

        // Determine tile dimensions from buffer size (assumes square tiles)
        const pixelCount = buffer.byteLength / 2  // 2 bytes per float16
        const side = Math.sqrt(pixelCount)
        if (side !== Math.floor(side)) {
          console.warn(`[TileManager] Float16 tile has non-square size: ${pixelCount} pixels`)
          state.tiles.set(key, { key, texture, state: 'error', lastAccess: ++this._accessCounter })
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

        state.tiles.set(key, {
          key,
          texture,
          state: 'loaded',
          lastAccess: ++this._accessCounter,
        })
        this._notifyTileLoaded(state, key)
      })
      .catch(err => {
        if (err.name === 'AbortError') return
        console.warn(`[TileManager] Failed to load Float16 tile: ${url}`, err)
        if (!state.pendingF16.has(key)) {
          gl.deleteTexture(texture)
          return
        }
        state.pendingF16.delete(key)
        state.tiles.set(key, { key, texture, state: 'error', lastAccess: ++this._accessCounter })
      })
  }

  /** Fetch a PNG data tile and upload as RGBA texture (original path). */
  private _fetchTilePng(state: DatasetState, z: number, x: number, y: number): void {
    const key = tileKey(z, x, y)
    const gl = this._gl
    const url = this._buildUrl(state.config, z, x, y)

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
    state.pending.set(key, { image: img, texture })

    img.onload = () => {
      // Check we haven't been cleared/destroyed while loading
      if (!state.pending.has(key)) {
        gl.deleteTexture(texture)
        return
      }
      state.pending.delete(key)

      // Upload image data to texture
      gl.bindTexture(gl.TEXTURE_2D, texture)
      gl.texImage2D(
        gl.TEXTURE_2D, 0, gl.RGBA,
        gl.RGBA, gl.UNSIGNED_BYTE,
        img,
      )
      gl.bindTexture(gl.TEXTURE_2D, null)

      state.tiles.set(key, {
        key,
        texture,
        state: 'loaded',
        lastAccess: ++this._accessCounter,
      })
      this._notifyTileLoaded(state, key)
    }

    img.onerror = () => {
      console.warn(`[TileManager] Failed to load tile: ${url}`)
      if (!state.pending.has(key)) {
        gl.deleteTexture(texture)
        return
      }
      state.pending.delete(key)

      state.tiles.set(key, {
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
    const currentDatasetKey = this._currentDatasetKey()
    const entries: Array<{ datasetKey: string; state: DatasetState; entry: TileEntry }> = []
    for (const [datasetKey, state] of this._datasets) {
      for (const entry of state.tiles.values()) {
        entries.push({ datasetKey, state, entry })
      }
    }
    if (entries.length <= this._maxTextures) return

    entries.sort((a, b) => a.entry.lastAccess - b.entry.lastAccess)

    const toRemove = entries.length - this._maxTextures
    for (let i = 0; i < toRemove; i++) {
      const { datasetKey, state, entry } = entries[i]
      this._gl.deleteTexture(entry.texture)
      state.tiles.delete(entry.key)
      if (
        state.tiles.size === 0 &&
        state.pending.size === 0 &&
        state.pendingF16.size === 0 &&
        state.pendingWorker.size === 0 &&
        datasetKey !== currentDatasetKey
      ) {
        this._datasets.delete(datasetKey)
      }
    }
  }

  private _disposeDatasetState(datasetKey: string, state: DatasetState): void {
    for (const { image, texture } of state.pending.values()) {
      image.onload = null
      image.onerror = null
      image.src = ''
      this._gl.deleteTexture(texture)
    }
    state.pending.clear()

    for (const { abort, texture } of state.pendingF16.values()) {
      abort.abort()
      this._gl.deleteTexture(texture)
    }
    state.pendingF16.clear()

    if (this._fetchClient && state.pendingWorker.size > 0) {
      for (const [localKey, texture] of state.pendingWorker) {
        this._fetchClient.cancel(this._workerKey(datasetKey, localKey))
        this._gl.deleteTexture(texture)
      }
      state.pendingWorker.clear()
    }

    for (const entry of state.tiles.values()) {
      this._gl.deleteTexture(entry.texture)
    }
    state.tiles.clear()
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

  // Find bounding box using wrap-aware render-X to keep it compact
  // at the antimeridian (same approach as WindParticleLayer._packAtlas).
  let rxMin = Infinity, rxMax = -Infinity
  let yMin = Infinity, yMax = -Infinity
  for (const c of visible) {
    const rx = c.x + c.wrap * n
    if (rx < rxMin) rxMin = rx
    if (rx > rxMax) rxMax = rx
    if (c.y < yMin) yMin = c.y
    if (c.y > yMax) yMax = c.y
  }

  const visibleSet = new Set(visible.map(c => `${c.x},${c.y}`))
  const prefetch: TileCoord[] = []

  const addIfNew = (rx: number, y: number) => {
    if (y < 0 || y >= n) return // beyond poles
    const x = ((rx % n) + n) % n // canonical tile X
    const key = `${x},${y}`
    if (visibleSet.has(key)) return
    visibleSet.add(key) // prevent duplicates in prefetch set
    prefetch.push({ z, x, y, wrap: 0 })
  }

  // One column/row in the direction of movement
  if (direction.dx > 0) {
    // Panning east → prefetch east column
    for (let y = yMin; y <= yMax; y++) addIfNew(rxMax + 1, y)
  }
  if (direction.dx < 0) {
    // Panning west → prefetch west column
    for (let y = yMin; y <= yMax; y++) addIfNew(rxMin - 1, y)
  }
  if (direction.dy > 0) {
    // Panning north → lower tile-y → prefetch row above
    for (let rx = rxMin; rx <= rxMax; rx++) addIfNew(rx, yMin - 1)
  }
  if (direction.dy < 0) {
    // Panning south → higher tile-y → prefetch row below
    for (let rx = rxMin; rx <= rxMax; rx++) addIfNew(rx, yMax + 1)
  }

  // Diagonal corner tiles (if panning diagonally)
  if (direction.dx > 0 && direction.dy > 0) addIfNew(rxMax + 1, yMin - 1)
  if (direction.dx > 0 && direction.dy < 0) addIfNew(rxMax + 1, yMax + 1)
  if (direction.dx < 0 && direction.dy > 0) addIfNew(rxMin - 1, yMin - 1)
  if (direction.dx < 0 && direction.dy < 0) addIfNew(rxMin - 1, yMax + 1)

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
  const xs: { x: number; wrap: number }[] = []

  if (northWest.x <= southEast.x) {
    // Normal: no wrapping
    for (let x = northWest.x; x <= southEast.x; x++) xs.push({ x, wrap: 0 })
  } else {
    // Antimeridian crossing: east hemisphere tiles are primary, west hemisphere tiles are wrap=1
    for (let x = northWest.x; x < n; x++) xs.push({ x, wrap: 0 })
    for (let x = 0; x <= southEast.x; x++) xs.push({ x, wrap: 1 })
  }

  const tiles: TileCoord[] = []
  const yStart = Math.min(northWest.y, southEast.y)
  const yEnd = Math.max(northWest.y, southEast.y)
  for (const { x, wrap } of xs) {
    for (let y = yStart; y <= yEnd; y++) {
      tiles.push({ z, x, y, wrap })
    }
  }
  return tiles
}
