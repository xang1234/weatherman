/**
 * Main-thread client for the tile-fetch Web Worker.
 *
 * Wraps the Worker's postMessage API into a listener-based interface
 * that multiple TileManagers can subscribe to. Manages the worker
 * lifecycle and provides methods to request/cancel tile fetches.
 *
 * A single TileFetchClient instance is shared across all TileManagers
 * to avoid spawning multiple workers. Each manager registers its own
 * listener; the manager filters results by checking its pending map.
 */

import type {
  MainToWorkerMessage,
  TilePriority,
  WorkerToMainMessage,
} from './tile-fetch-protocol'

export interface TileFetchResult {
  key: string
  format: 'png' | 'f16'
  data: ArrayBuffer | ImageBitmap
  /** Tile side length (f16 only, -1 if non-square). */
  side?: number
}

export interface TileFetchError {
  key: string
  error: string
}

export type TileLoadedListener = (result: TileFetchResult) => void
export type TileErrorListener = (error: TileFetchError) => void

export class TileFetchClient {
  private _worker: Worker
  private _loadedListeners = new Set<TileLoadedListener>()
  private _errorListeners = new Set<TileErrorListener>()

  constructor() {
    // Vite handles the Worker URL at build time via import.meta.url
    this._worker = new Worker(
      new URL('./tile-fetch.worker.ts', import.meta.url),
      { type: 'module' },
    )

    this._worker.onmessage = (e: MessageEvent<WorkerToMainMessage>) => {
      const msg = e.data
      switch (msg.type) {
        case 'tile-loaded': {
          const result: TileFetchResult = {
            key: msg.key,
            format: msg.format,
            data: msg.data,
            side: msg.side,
          }
          for (const listener of this._loadedListeners) {
            listener(result)
          }
          break
        }
        case 'tile-error': {
          const error: TileFetchError = {
            key: msg.key,
            error: msg.error,
          }
          for (const listener of this._errorListeners) {
            listener(error)
          }
          break
        }
      }
    }

    this._worker.onerror = (e) => {
      console.error('[TileFetchClient] Worker error:', e)
    }
  }

  /** Subscribe to successful tile fetch results. Returns unsubscribe function. */
  addLoadedListener(listener: TileLoadedListener): () => void {
    this._loadedListeners.add(listener)
    return () => this._loadedListeners.delete(listener)
  }

  /** Subscribe to tile fetch errors. Returns unsubscribe function. */
  addErrorListener(listener: TileErrorListener): () => void {
    this._errorListeners.add(listener)
    return () => this._errorListeners.delete(listener)
  }

  /** Request the worker to fetch a tile with a given priority. */
  fetch(key: string, url: string, format: 'png' | 'f16', priority: TilePriority = 0): void {
    const msg: MainToWorkerMessage = { type: 'fetch', key, url, format, priority }
    this._worker.postMessage(msg)
  }

  /** Update worker configuration (e.g. max concurrent fetches). */
  configure(options: { maxConcurrent?: number }): void {
    const msg: MainToWorkerMessage = { type: 'configure', ...options }
    this._worker.postMessage(msg)
  }

  /** Cancel a specific in-flight fetch. */
  cancel(key: string): void {
    const msg: MainToWorkerMessage = { type: 'cancel', key }
    this._worker.postMessage(msg)
  }

  /** Cancel all in-flight fetches. */
  cancelAll(): void {
    const msg: MainToWorkerMessage = { type: 'cancel-all' }
    this._worker.postMessage(msg)
  }

  /** Terminate the worker. Call when done (e.g. app unmount). */
  destroy(): void {
    this._worker.terminate()
    this._loadedListeners.clear()
    this._errorListeners.clear()
  }
}

// ── Singleton ────────────────────────────────────────────────────

let _instance: TileFetchClient | null = null

/**
 * Get the shared TileFetchClient singleton.
 * Lazily creates the worker on first call.
 */
export function getTileFetchClient(): TileFetchClient {
  if (!_instance) {
    _instance = new TileFetchClient()
  }
  return _instance
}
