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
import type { TileFetchClient } from '@/workers/TileFetchClient'
import { acquireSharedTileStore, recordTileSide, type SharedTileStore } from './shared-tiles'

/** Loading state for a single tile. */
export type TileState = 'pending' | 'loaded' | 'error'

/** Tile data format. */
export type TileFormat = 'png' | 'f16'

/** A single cached tile with its WebGL texture and metadata. */
interface TileEntry {
  key: string
  /** Null for a failed fetch through the shared store, which keeps no texture. */
  texture: WebGLTexture | null
  /** Set when the texture belongs to the shared store and must be released there. */
  url?: string
  state: TileState
  /** Monotonically increasing access counter for LRU ordering. */
  lastAccess: number
  /** Error entries only: performance.now() time after which to fetch again. */
  retryAt?: number
}

/** Tile coordinate with world-copy tracking for antimeridian support. */
export interface TileCoord {
  z: number
  x: number  // canonical tile X [0, n-1] — used for fetching and cache lookup
  y: number
  wrap: number  // world copy to draw on: 0 = primary, 1 = next copy east, -1 = next copy west
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
  /** Ask the owner to render a frame. Failed tiles are retried from
   *  updateVisibleTiles(), which only runs when something renders. */
  requestRender?: () => void
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
/**
 * How long the dataset replaced by setLayer(…, keepStale) stays available as
 * a stand-in. Bounded so a tile that never arrives cannot leave the old
 * hour on screen under the new hour's label.
 */
const STALE_FALLBACK_MS = 4000

/** Wait before re-fetching a failed tile, by consecutive failure count; the last value repeats. */
const RETRY_DELAYS_MS = [2_000, 5_000, 15_000, 60_000]

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
  pendingWorker: Map<string, { url: string; priority: TilePriority; cancel: () => void }>
  /** Consecutive failures per tile key; cleared when the tile loads. */
  failures: Map<string, number>
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

  /** Dataset shown before the last setLayer(), usable until _staleUntil. */
  private _staleKey: string | null = null
  private _staleUntil = 0

  /** Monotonically increasing counter for LRU tracking. */
  private _accessCounter = 0

  /** Shared Web Worker client for off-thread fetching (optional). */
  private _fetchClient: TileFetchClient | null = null

  private _requestRender: (() => void) | null = null

  /** Textures shared by URL with the other managers on this GL context (worker path). */
  private _store: SharedTileStore | null = null
  private _releaseStore: (() => void) | null = null

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
    this._requestRender = options.requestRender ?? null
    if (this._fetchClient) {
      const { store, done } = acquireSharedTileStore(gl, this._fetchClient, this._format)
      this._store = store
      this._releaseStore = done
    }
  }

  /**
   * Set the current dataset to fetch tiles for.
   * Switching datasets reuses any cached tiles already kept for that key.
   *
   * @param keepStale Keep the replaced dataset readable through
   *   getTexture(…, true) for a few seconds, so the caller can go on drawing
   *   it while the new one loads. Only honoured when just the hour or run
   *   changes — another layer's tiles would be drawn with the wrong ramp.
   */
  setLayer(model: string, runId: string, layer: string, forecastHour: number, keepStale = false): void {
    if (
      model === this._model &&
      runId === this._runId &&
      layer === this._layer &&
      forecastHour === this._forecastHour
    ) {
      return
    }
    if (!keepStale || model !== this._model || layer !== this._layer) {
      this._staleKey = null
    } else if (this._staleState() == null) {
      // The dataset being replaced had taken over the screen (its own stand-in
      // was dropped or timed out), so it becomes the stand-in.
      const previous = this._currentState()
      const shown = previous != null && [...previous.tiles.values()].some((t) => t.state === 'loaded')
      this._staleKey = shown ? this._datasetKey(previous.config) : null
      this._staleUntil = performance.now() + STALE_FALLBACK_MS
    }
    // Otherwise a stand-in is still up (rapid scrubbing A→B→C with B not yet
    // loaded): A is what the user is looking at, so keep it rather than
    // promoting a partly loaded B. Its time limit is not extended.
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
    const state = this._ensureCurrentState()
    if (!state) return
    state.lastUsed = ++this._accessCounter

    const now = performance.now()
    for (const { z, x, y } of coords) {
      const key = tileKey(z, x, y)
      const existing = state.tiles.get(key)
      if (existing?.state === 'error') {
        if (now < (existing.retryAt ?? 0)) continue
        // Retry is due: drop the failed entry and fall through to fetch again.
        this._freeEntry(existing)
        state.tiles.delete(key)
      } else if (existing) {
        existing.lastAccess = ++this._accessCounter
        continue
      }
      const inWorker = state.pendingWorker.get(key)
      if (inWorker) {
        // A tile first requested as a prefetch may now be on screen: tell
        // the worker, or it stays behind every visible tile in the queue.
        if (priority < inWorker.priority) {
          inWorker.priority = priority
          this._store!.upgrade(inWorker.url, priority)
        }
        continue
      }
      if (state.pending.has(key) || state.pendingF16.has(key)) continue
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

  /** Whether the dataset replaced by the last setLayer() can still be drawn. */
  get hasStale(): boolean {
    return this._staleState() != null
  }

  /** Forget the replaced dataset — call once the current one has taken over. */
  dropStale(): void {
    this._staleKey = null
  }

  /**
   * Get the texture for a tile, or null if not yet loaded.
   * Updates the LRU access counter.
   *
   * @param stale Read from the dataset replaced by the last setLayer()
   *   instead of the current one (see setLayer's keepStale).
   */
  getTexture(z: number, x: number, y: number, stale = false): WebGLTexture | null {
    const state = stale ? this._staleState() : this._currentState()
    const entry = state?.tiles.get(tileKey(z, x, y))
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

  /**
   * Whether every one of `coords` is loaded in the current dataset — or,
   * with `orFailed`, has at least stopped loading (failed tiles count).
   */
  allLoaded(coords: TileCoord[], orFailed = false): boolean {
    const state = this._currentState()
    if (!state) return coords.length === 0
    return coords.every(({ z, x, y }) => {
      const tile = state.tiles.get(tileKey(z, x, y))?.state
      return tile === 'loaded' || (orFailed && tile === 'error')
    })
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
    for (const state of this._datasets.values()) {
      this._disposeDatasetState(state)
    }
    this._datasets.clear()
    this._staleKey = null
    this._accessCounter = 0
  }

  /** Release all GL resources. Call when done with this manager. */
  destroy(): void {
    this.clear()
    this.onTileLoaded = null
    this._requestRender = null
    this._releaseStore?.()
    this._releaseStore = null
    this._store = null
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

  private _staleState(): DatasetState | null {
    if (!this._staleKey || performance.now() > this._staleUntil) return null
    return this._datasets.get(this._staleKey) ?? null
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
        failures: new Map(),
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

  /** Store a freshly uploaded tile and tell the owner. */
  private _markLoaded(state: DatasetState, key: string, texture: WebGLTexture, url?: string): void {
    state.failures.delete(key)
    state.tiles.set(key, { key, texture, url, state: 'loaded', lastAccess: ++this._accessCounter })
    this._notifyTileLoaded(state, key)
  }

  /**
   * Record a failed tile and schedule its retry. The entry stays in `tiles`
   * as an error until updateVisibleTiles() finds the retry due.
   */
  private _markError(state: DatasetState, key: string, texture: WebGLTexture | null): void {
    const failures = (state.failures.get(key) ?? 0) + 1
    state.failures.set(key, failures)
    const delay = RETRY_DELAYS_MS[Math.min(failures, RETRY_DELAYS_MS.length) - 1]
    state.tiles.set(key, {
      key,
      texture,
      state: 'error',
      lastAccess: ++this._accessCounter,
      retryAt: performance.now() + delay,
    })
    setTimeout(() => this._requestRender?.(), delay)
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

  /**
   * Fetch through the shared store: the first manager to want a URL fetches
   * it in the worker, the others get the same texture.
   */
  private _fetchTileViaWorker(
    state: DatasetState,
    z: number,
    x: number,
    y: number,
    priority: TilePriority = 0,
  ): void {
    const key = tileKey(z, x, y)
    const url = this._buildUrl(state.config, z, x, y)
    const store = this._store!
    const pending = { url, priority, cancel: () => {} }
    state.pendingWorker.set(key, pending)
    // May call back at once if another manager already has the tile.
    pending.cancel = store.request(url, priority, (texture) => {
      if (state.pendingWorker.get(key) !== pending) {
        // Withdrawn meanwhile (dataset cleared): give the reference back.
        if (texture) store.release(url)
        return
      }
      state.pendingWorker.delete(key)
      if (texture) {
        this._markLoaded(state, key, texture, url)
        return
      }
      // Warn once per tile, not on every failed retry.
      if (!state.failures.has(key)) {
        console.warn(`[TileManager] Worker fetch failed: ${key} (will retry)`)
      }
      this._markError(state, key, null)
    })
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
          this._markError(state, key, texture)
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
        recordTileSide(texture, side)
        gl.bindTexture(gl.TEXTURE_2D, null)

        this._markLoaded(state, key, texture)
      })
      .catch(err => {
        if (err.name === 'AbortError') return
        console.warn(`[TileManager] Failed to load Float16 tile: ${url}`, err)
        if (!state.pendingF16.has(key)) {
          gl.deleteTexture(texture)
          return
        }
        state.pendingF16.delete(key)
        this._markError(state, key, texture)
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
      // Data bytes, not colour: upload them untouched (as the worker path does).
      gl.pixelStorei(gl.UNPACK_COLORSPACE_CONVERSION_WEBGL, gl.NONE)
      gl.texImage2D(
        gl.TEXTURE_2D, 0, gl.RGBA,
        gl.RGBA, gl.UNSIGNED_BYTE,
        img,
      )
      gl.pixelStorei(gl.UNPACK_COLORSPACE_CONVERSION_WEBGL, gl.BROWSER_DEFAULT_WEBGL)
      recordTileSide(texture, img.width)
      gl.bindTexture(gl.TEXTURE_2D, null)

      this._markLoaded(state, key, texture)
    }

    img.onerror = () => {
      console.warn(`[TileManager] Failed to load tile: ${url}`)
      if (!state.pending.has(key)) {
        gl.deleteTexture(texture)
        return
      }
      state.pending.delete(key)

      this._markError(state, key, texture)
    }

    img.src = url
  }

  /** Evict least-recently-used tiles when over the cache limit. */
  private _evict(): void {
    // Called every frame: count first, and only list and sort when over the limit.
    let count = 0
    for (const state of this._datasets.values()) count += state.tiles.size
    if (count <= this._maxTextures) return

    const currentDatasetKey = this._currentDatasetKey()
    const entries: Array<{ datasetKey: string; state: DatasetState; entry: TileEntry }> = []
    for (const [datasetKey, state] of this._datasets) {
      for (const entry of state.tiles.values()) {
        entries.push({ datasetKey, state, entry })
      }
    }
    entries.sort((a, b) => a.entry.lastAccess - b.entry.lastAccess)

    const toRemove = entries.length - this._maxTextures
    for (let i = 0; i < toRemove; i++) {
      const { datasetKey, state, entry } = entries[i]
      this._freeEntry(entry)
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

  private _disposeDatasetState(state: DatasetState): void {
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

    for (const { cancel } of state.pendingWorker.values()) cancel()
    state.pendingWorker.clear()

    for (const entry of state.tiles.values()) {
      this._freeEntry(entry)
    }
    state.tiles.clear()
  }

  /** Give a tile's texture back: to the shared store, or delete our own. */
  private _freeEntry(entry: TileEntry): void {
    if (entry.url) this._store?.release(entry.url)
    else if (entry.texture) this._gl.deleteTexture(entry.texture)
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
  // at the antimeridian (same approach as ParticleLayer._packAtlas).
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

/**
 * Highest zoom with pre-generated data tiles. Must match MAX_DATA_TILE_ZOOM in
 * src/weatherman/processing/data_tiles.py — above it every tile is a TiTiler
 * round trip that only upsamples the 0.25° grid, which the shaders do anyway.
 */
export const MAX_DATA_TILE_ZOOM = 5

/** Data-tile zoom level to request for a given map zoom. */
export function dataTileZoom(mapZoom: number): number {
  return Math.max(0, Math.min(MAX_DATA_TILE_ZOOM, Math.floor(mapZoom)))
}

/** Tile row containing a latitude, clamped to the valid range. */
function latToTileY(lat: number, z: number): number {
  const n = 2 ** z
  const latRad = (lat * Math.PI) / 180
  const merc = Math.log(Math.tan(Math.PI / 4 + latRad / 2))
  const y = Math.floor(((1 - merc / Math.PI) / 2) * n)
  return Math.max(0, Math.min(n - 1, y))
}

/**
 * Compute visible tile coordinates for a given map viewport and zoom level.
 *
 * MapLibre reports the viewport's longitudes unwrapped around the map centre:
 * west can be below -180 and east above 180, and the span can exceed 360 when
 * several copies of the world are on screen. Each tile column is therefore
 * returned with the world copy (`wrap`) it has to be drawn on: -1 for the
 * copy west of the primary world, 1 for the one east of it, and so on.
 */
export function computeVisibleTiles(
  bounds: { west: number; north: number; east: number; south: number },
  z: number,
): TileCoord[] {
  const n = 2 ** z
  const west = bounds.west
  const east = bounds.east < west ? bounds.east + 360 : bounds.east
  // Column index counted continuously across world copies (can be < 0 or >= n)
  const firstColumn = Math.floor(((west + 180) / 360) * n)
  const lastColumn = Math.floor(((east + 180) / 360) * n)
  const yA = latToTileY(bounds.north, z)
  const yB = latToTileY(bounds.south, z)
  const yStart = Math.min(yA, yB)
  const yEnd = Math.max(yA, yB)

  const tiles: TileCoord[] = []
  for (let column = firstColumn; column <= lastColumn; column++) {
    const wrap = Math.floor(column / n)
    const x = column - wrap * n
    for (let y = yStart; y <= yEnd; y++) {
      tiles.push({ z, x, y, wrap })
    }
  }
  return tiles
}
