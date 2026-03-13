/**
 * Message protocol for the tile-fetch Web Worker.
 *
 * The main thread sends fetch/cancel commands; the worker returns
 * decoded tile data via transferable objects (zero-copy).
 */

// ── Main → Worker ─────────────────────────────────────────────────

export interface FetchTileMessage {
  type: 'fetch'
  /** Unique key for this tile (e.g. "z/x/y"). */
  key: string
  /** Full URL to fetch the tile from. */
  url: string
  /** Tile format determines decode strategy. */
  format: 'png' | 'f16'
}

export interface CancelTileMessage {
  type: 'cancel'
  /** Tile key to cancel. */
  key: string
}

export interface CancelAllMessage {
  type: 'cancel-all'
}

export type MainToWorkerMessage =
  | FetchTileMessage
  | CancelTileMessage
  | CancelAllMessage

// ── Worker → Main ─────────────────────────────────────────────────

export interface TileLoadedMessage {
  type: 'tile-loaded'
  key: string
  format: 'png' | 'f16'
  /** For f16: raw ArrayBuffer (transferred). For png: ImageBitmap (transferred). */
  data: ArrayBuffer | ImageBitmap
  /** Tile dimensions (square). Only set for f16 (computed from buffer size). */
  side?: number
}

export interface TileErrorMessage {
  type: 'tile-error'
  key: string
  error: string
}

export type WorkerToMainMessage =
  | TileLoadedMessage
  | TileErrorMessage
