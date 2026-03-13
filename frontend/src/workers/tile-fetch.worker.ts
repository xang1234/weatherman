/**
 * Web Worker for tile fetching.
 *
 * Owns the fetch lifecycle: receives requests from the main thread,
 * manages AbortControllers for cancellation, decodes tile data
 * (ArrayBuffer for Float16, ImageBitmap for PNG), and transfers
 * results back via postMessage with zero-copy Transferable objects.
 *
 * This keeps all network callback processing off the main thread,
 * preventing jank during rapid playback or panning.
 */

import type {
  MainToWorkerMessage,
  TileLoadedMessage,
  TileErrorMessage,
} from './tile-fetch-protocol'

/** In-flight fetches keyed by tile key, with AbortController for cancellation. */
const pending = new Map<string, AbortController>()

self.onmessage = (e: MessageEvent<MainToWorkerMessage>) => {
  const msg = e.data

  switch (msg.type) {
    case 'fetch':
      fetchTile(msg.key, msg.url, msg.format)
      break

    case 'cancel':
      cancelTile(msg.key)
      break

    case 'cancel-all':
      cancelAll()
      break
  }
}

function cancelTile(key: string): void {
  const ctrl = pending.get(key)
  if (ctrl) {
    ctrl.abort()
    pending.delete(key)
  }
}

function cancelAll(): void {
  for (const ctrl of pending.values()) {
    ctrl.abort()
  }
  pending.clear()
}

async function fetchTile(key: string, url: string, format: 'png' | 'f16'): Promise<void> {
  // Cancel any existing fetch for this key (e.g. stale request from previous viewport)
  cancelTile(key)

  const abort = new AbortController()
  pending.set(key, abort)

  try {
    const resp = await fetch(url, { signal: abort.signal })
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`)

    if (format === 'f16') {
      const buffer = await resp.arrayBuffer()

      // Verify we haven't been cancelled while awaiting
      if (!pending.has(key)) return
      pending.delete(key)

      // Compute tile dimensions from buffer size (2 bytes per float16 pixel)
      const pixelCount = buffer.byteLength / 2
      const side = Math.sqrt(pixelCount)

      const msg: TileLoadedMessage = {
        type: 'tile-loaded',
        key,
        format: 'f16',
        data: buffer,
        side: side === Math.floor(side) ? side : -1,
      }
      // Transfer the ArrayBuffer (zero-copy)
      self.postMessage(msg, { transfer: [buffer] })
    } else {
      // PNG: fetch as blob, decode to ImageBitmap in the worker
      const blob = await resp.blob()

      // Verify we haven't been cancelled while awaiting
      if (!pending.has(key)) return

      const bitmap = await createImageBitmap(blob)

      if (!pending.has(key)) {
        bitmap.close()
        return
      }
      pending.delete(key)

      const msg: TileLoadedMessage = {
        type: 'tile-loaded',
        key,
        format: 'png',
        data: bitmap,
      }
      // Transfer the ImageBitmap (zero-copy)
      self.postMessage(msg, { transfer: [bitmap] })
    }
  } catch (err: unknown) {
    if (err instanceof DOMException && err.name === 'AbortError') return

    pending.delete(key)

    const msg: TileErrorMessage = {
      type: 'tile-error',
      key,
      error: err instanceof Error ? err.message : String(err),
    }
    self.postMessage(msg)
  }
}
