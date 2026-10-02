/**
 * Data-tile textures shared by URL across every TileManager on a GL context.
 *
 * The colour layer and the particle layers fetch the same tiles (wind U/V,
 * wave height). Without sharing, each tile is decoded, uploaded and kept in
 * GPU memory once per layer (#43). Here the first request for a URL fetches
 * it through the worker; later requests, while it loads or after, get the
 * same texture. A loaded texture is freed when its last holder releases it;
 * a pending fetch is cancelled when its last waiter gives up.
 */

import type { TileFetchClient, TileFetchError, TileFetchResult } from '@/workers/TileFetchClient'
import type { TilePriority } from '@/workers/tile-fetch-protocol'

/** Receives the tile's texture, or null if the fetch failed. */
export type SharedTileListener = (texture: WebGLTexture | null) => void

interface SharedTile {
  texture: WebGLTexture
  loaded: boolean
  /** Loaded: managers holding the texture. */
  refs: number
  /** Pending: managers waiting for it. */
  listeners: Set<SharedTileListener>
  priority: TilePriority
}

export class SharedTileStore {
  private _tiles = new Map<string, SharedTile>()
  private _gl: WebGL2RenderingContext
  private _client: TileFetchClient
  private _format: 'png' | 'f16'

  constructor(gl: WebGL2RenderingContext, client: TileFetchClient, format: 'png' | 'f16') {
    this._gl = gl
    this._client = client
    this._format = format
    client.addLoadedListener((result) => this._onLoaded(result))
    client.addErrorListener((error) => this._onError(error))
  }

  /**
   * Ask for a tile. The listener is called with its texture — at once if it
   * is already loaded — and the caller then holds a reference to release().
   * Returns a function that withdraws a request still pending.
   */
  request(url: string, priority: TilePriority, listener: SharedTileListener): () => void {
    let tile = this._tiles.get(url)
    if (tile?.loaded) {
      tile.refs++
      listener(tile.texture)
      return () => {}
    }
    if (!tile) {
      tile = { texture: this._placeholder(), loaded: false, refs: 0, listeners: new Set(), priority }
      this._tiles.set(url, tile)
      this._client.fetch(url, url, this._format, priority)
    } else if (priority < tile.priority) {
      // A tile first wanted as a prefetch may now be on screen.
      tile.priority = priority
      this._client.fetch(url, url, this._format, priority)
    }
    const pending = tile
    pending.listeners.add(listener)
    return () => {
      if (pending.loaded || !pending.listeners.delete(listener) || pending.listeners.size > 0) return
      this._client.cancel(url)
      this._gl.deleteTexture(pending.texture)
      this._tiles.delete(url)
    }
  }

  /** Raise the fetch priority of a pending tile. */
  upgrade(url: string, priority: TilePriority): void {
    const tile = this._tiles.get(url)
    if (!tile || tile.loaded || priority >= tile.priority) return
    tile.priority = priority
    this._client.fetch(url, url, this._format, priority)
  }

  /** Drop one reference to a loaded tile; the last one frees the texture. */
  release(url: string): void {
    const tile = this._tiles.get(url)
    if (!tile?.loaded || --tile.refs > 0) return
    this._gl.deleteTexture(tile.texture)
    this._tiles.delete(url)
  }

  /** Loaded tiles currently held (for tests). */
  get size(): number {
    let loaded = 0
    for (const tile of this._tiles.values()) if (tile.loaded) loaded++
    return loaded
  }

  private _onLoaded(result: TileFetchResult): void {
    const tile = this._tiles.get(result.key)
    if (!tile || tile.loaded) {
      // Withdrawn while in flight.
      if (typeof ImageBitmap !== 'undefined' && result.data instanceof ImageBitmap) result.data.close()
      return
    }
    if (!this._upload(tile.texture, result)) {
      this._fail(result.key, tile)
      return
    }
    tile.loaded = true
    tile.refs = tile.listeners.size
    const listeners = [...tile.listeners]
    tile.listeners.clear()
    for (const listener of listeners) listener(tile.texture)
  }

  private _onError(error: TileFetchError): void {
    const tile = this._tiles.get(error.key)
    if (tile && !tile.loaded) this._fail(error.key, tile)
  }

  /** Tell every waiter the fetch failed; each retries on its own schedule. */
  private _fail(url: string, tile: SharedTile): void {
    this._tiles.delete(url)
    this._gl.deleteTexture(tile.texture)
    for (const listener of tile.listeners) listener(null)
  }

  private _upload(texture: WebGLTexture, result: TileFetchResult): boolean {
    const gl = this._gl
    gl.bindTexture(gl.TEXTURE_2D, texture)
    if (result.format === 'f16') {
      const side = result.side ?? -1
      if (side <= 0) {
        console.warn('[TileManager] Float16 tile from worker has non-square size')
        gl.bindTexture(gl.TEXTURE_2D, null)
        return false
      }
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.R16F, side, side, 0, gl.RED, gl.HALF_FLOAT, new Uint16Array(result.data as ArrayBuffer))
    } else {
      const bitmap = result.data as ImageBitmap
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, bitmap)
      bitmap.close()
    }
    gl.bindTexture(gl.TEXTURE_2D, null)
    return true
  }

  /** 1×1 texture with nearest filtering, filled in when the data arrives. */
  private _placeholder(): WebGLTexture {
    const gl = this._gl
    const texture = gl.createTexture()!
    gl.bindTexture(gl.TEXTURE_2D, texture)
    if (this._format === 'f16') {
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.R16F, 1, 1, 0, gl.RED, gl.HALF_FLOAT, new Uint16Array([0]))
    } else {
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, 1, 1, 0, gl.RGBA, gl.UNSIGNED_BYTE, new Uint8Array([0, 0, 0, 0]))
    }
    // NEAREST: the shaders interpolate themselves, so they can respect nodata.
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.NEAREST)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.NEAREST)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE)
    gl.bindTexture(gl.TEXTURE_2D, null)
    return texture
  }
}

const stores = new WeakMap<WebGL2RenderingContext, Map<string, SharedTileStore>>()

/** The store for a GL context and tile format. */
export function sharedTileStore(gl: WebGL2RenderingContext, client: TileFetchClient, format: 'png' | 'f16'): SharedTileStore {
  let byFormat = stores.get(gl)
  if (!byFormat) stores.set(gl, (byFormat = new Map()))
  let store = byFormat.get(format)
  if (!store) byFormat.set(format, (store = new SharedTileStore(gl, client, format)))
  return store
}
