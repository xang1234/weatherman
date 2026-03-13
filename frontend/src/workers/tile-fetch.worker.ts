/**
 * Web Worker for tile fetching with priority queue.
 *
 * Owns the fetch lifecycle: receives requests from the main thread,
 * manages AbortControllers for cancellation, decodes tile data
 * (ArrayBuffer for Float16, ImageBitmap for PNG), and transfers
 * results back via postMessage with zero-copy Transferable objects.
 *
 * Implements a priority queue with bounded concurrency:
 *   Priority 0 — current viewport, current time (highest)
 *   Priority 1 — current viewport, next time step (temporal blend)
 *   Priority 2 — adjacent / prefetch tiles (lowest)
 *
 * When at max concurrency and a higher-priority request arrives,
 * the lowest-priority in-flight fetch is cancelled to make room.
 */

import type {
  MainToWorkerMessage,
  TilePriority,
  TileLoadedMessage,
  TileErrorMessage,
} from './tile-fetch-protocol'

// ── Configuration ───────────────────────────────────────────────

let maxConcurrent = 6

// ── Queue & in-flight tracking ──────────────────────────────────

interface QueueEntry {
  key: string
  url: string
  format: 'png' | 'f16'
  priority: TilePriority
}

interface InFlightEntry {
  abort: AbortController
  priority: TilePriority
}

/** Waiting requests, drained by priority (lower number first). */
const queue: QueueEntry[] = []

/** Currently executing fetches. */
const inFlight = new Map<string, InFlightEntry>()

// ── Message handler ─────────────────────────────────────────────

self.onmessage = (e: MessageEvent<MainToWorkerMessage>) => {
  const msg = e.data

  switch (msg.type) {
    case 'fetch':
      enqueue(msg.key, msg.url, msg.format, msg.priority)
      break

    case 'cancel':
      cancelTile(msg.key)
      break

    case 'cancel-all':
      cancelAll()
      break

    case 'configure':
      if (msg.maxConcurrent != null && msg.maxConcurrent > 0) {
        maxConcurrent = msg.maxConcurrent
      }
      drain()
      break
  }
}

// ── Enqueue & drain ─────────────────────────────────────────────

function enqueue(key: string, url: string, format: 'png' | 'f16', priority: TilePriority): void {
  // If already in-flight with same or better priority, skip
  const existing = inFlight.get(key)
  if (existing) {
    if (existing.priority <= priority) return
    // Re-enqueue at higher priority: cancel old fetch
    existing.abort.abort()
    inFlight.delete(key)
  }

  // Remove any queued entry for this key (de-dup)
  const idx = queue.findIndex(e => e.key === key)
  if (idx !== -1) queue.splice(idx, 1)

  queue.push({ key, url, format, priority })

  drain()
}

function drain(): void {
  // Sort queue: lower priority number first (highest priority)
  // Stable sort within same priority preserves insertion order (FIFO)
  queue.sort((a, b) => a.priority - b.priority)

  while (queue.length > 0 && inFlight.size < maxConcurrent) {
    const entry = queue.shift()!
    startFetch(entry)
  }

  // Preemption: if queue head has higher priority than worst in-flight, swap
  if (queue.length > 0 && inFlight.size >= maxConcurrent) {
    const bestQueued = queue[0] // Already sorted, so [0] is highest priority
    let worstKey: string | null = null
    let worstPriority: TilePriority = -1 as TilePriority

    for (const [k, entry] of inFlight) {
      if (entry.priority > worstPriority) {
        worstPriority = entry.priority
        worstKey = k
      }
    }

    if (worstKey && bestQueued.priority < worstPriority) {
      // Cancel the lowest-priority in-flight to make room.
      // The cancelled tile is simply dropped — the main thread will
      // re-request it on the next frame if it's still needed.
      const victim = inFlight.get(worstKey)!
      victim.abort.abort()
      inFlight.delete(worstKey)

      const next = queue.shift()!
      startFetch(next)
    }
  }
}

// ── Fetch execution ─────────────────────────────────────────────

function startFetch(entry: QueueEntry): void {
  const { key, url, format, priority } = entry
  const abort = new AbortController()
  inFlight.set(key, { abort, priority })
  executeFetch(key, url, format, abort)
}

async function executeFetch(
  key: string,
  url: string,
  format: 'png' | 'f16',
  abort: AbortController,
): Promise<void> {
  try {
    const resp = await fetch(url, { signal: abort.signal })
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`)

    if (format === 'f16') {
      const buffer = await resp.arrayBuffer()

      // Verify we haven't been cancelled while awaiting
      if (!inFlight.has(key)) return
      inFlight.delete(key)

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
      self.postMessage(msg, { transfer: [buffer] })
    } else {
      // PNG: fetch as blob, decode to ImageBitmap in the worker
      const blob = await resp.blob()

      if (!inFlight.has(key)) return

      const bitmap = await createImageBitmap(blob)

      if (!inFlight.has(key)) {
        bitmap.close()
        return
      }
      inFlight.delete(key)

      const msg: TileLoadedMessage = {
        type: 'tile-loaded',
        key,
        format: 'png',
        data: bitmap,
      }
      self.postMessage(msg, { transfer: [bitmap] })
    }
  } catch (err: unknown) {
    if (err instanceof DOMException && err.name === 'AbortError') {
      // Aborted fetches don't free a slot here — they were already
      // removed from inFlight by the canceller. Just drain.
      drain()
      return
    }

    inFlight.delete(key)

    const msg: TileErrorMessage = {
      type: 'tile-error',
      key,
      error: err instanceof Error ? err.message : String(err),
    }
    self.postMessage(msg)
  }

  // A slot freed up — drain queue
  drain()
}

// ── Cancellation ────────────────────────────────────────────────

function cancelTile(key: string): void {
  // Remove from queue if queued
  const idx = queue.findIndex(e => e.key === key)
  if (idx !== -1) queue.splice(idx, 1)

  // Cancel if in-flight
  const entry = inFlight.get(key)
  if (entry) {
    entry.abort.abort()
    inFlight.delete(key)
    drain()
  }
}

function cancelAll(): void {
  queue.length = 0
  for (const entry of inFlight.values()) {
    entry.abort.abort()
  }
  inFlight.clear()
}
