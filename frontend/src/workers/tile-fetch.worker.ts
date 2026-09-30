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
 * the lowest-priority in-flight fetch is aborted and put back on the queue
 * to make room. A fetch that takes longer than FETCH_TIMEOUT_MS is aborted
 * and reported as an error, so a request that never completes cannot hold a
 * slot forever.
 */

import type {
  MainToWorkerMessage,
  TilePriority,
  TileLoadedMessage,
  TileErrorMessage,
} from './tile-fetch-protocol'

// ── Configuration ───────────────────────────────────────────────

let maxConcurrent = 12

/** Abort a fetch that has not completed by then and report it as an error. */
const FETCH_TIMEOUT_MS = 15_000

// ── Queue & in-flight tracking ──────────────────────────────────

interface QueueEntry {
  key: string
  url: string
  format: 'png' | 'f16'
  priority: TilePriority
}

interface InFlightEntry extends QueueEntry {
  abort: AbortController
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
  // Already in flight: keep the fetch, just raise its priority so it is not
  // the one picked for preemption.
  const existing = inFlight.get(key)
  if (existing) {
    if (priority < existing.priority) existing.priority = priority
    return
  }

  // Replace any queued entry for this key (de-dup), keeping the better priority
  const idx = queue.findIndex(e => e.key === key)
  if (idx !== -1) {
    priority = Math.min(priority, queue[idx].priority) as TilePriority
    queue.splice(idx, 1)
  }

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
    let worstPriority = -1

    for (const [k, entry] of inFlight) {
      if (entry.priority > worstPriority) {
        worstPriority = entry.priority
        worstKey = k
      }
    }

    if (worstKey != null && bestQueued.priority < worstPriority) {
      // Abort the lowest-priority in-flight to make room and put it back on
      // the queue. The main thread still counts it as pending and will not
      // ask again, so dropping it here would lose the tile for good.
      const victim = inFlight.get(worstKey)!
      victim.abort.abort()
      inFlight.delete(worstKey)

      const next = queue.shift()!
      queue.push({ key: victim.key, url: victim.url, format: victim.format, priority: victim.priority })
      startFetch(next)
    }
  }
}

// ── Fetch execution ─────────────────────────────────────────────

function startFetch(entry: QueueEntry): void {
  const abort = new AbortController()
  inFlight.set(entry.key, { ...entry, abort })
  executeFetch(entry.key, entry.url, entry.format, abort)
}

/** Whether this fetch still owns its key (not cancelled, preempted or restarted). */
function isCurrent(key: string, abort: AbortController): boolean {
  return inFlight.get(key)?.abort === abort
}

async function executeFetch(
  key: string,
  url: string,
  format: 'png' | 'f16',
  abort: AbortController,
): Promise<void> {
  let timedOut = false
  const timer = setTimeout(() => {
    timedOut = true
    abort.abort()
  }, FETCH_TIMEOUT_MS)
  try {
    const resp = await fetch(url, { signal: abort.signal })
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`)

    if (format === 'f16') {
      const buffer = await resp.arrayBuffer()

      // Verify we haven't been cancelled while awaiting
      if (!isCurrent(key, abort)) return
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

      if (!isCurrent(key, abort)) return

      const bitmap = await createImageBitmap(blob)

      if (!isCurrent(key, abort)) {
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
    if (err instanceof DOMException && err.name === 'AbortError' && !timedOut) {
      // Aborted fetches don't free a slot here — they were already
      // removed from inFlight by the canceller. Just drain.
      drain()
      return
    }

    // Cancelled or restarted while failing: the key is no longer ours to report.
    if (!isCurrent(key, abort)) return
    inFlight.delete(key)

    const msg: TileErrorMessage = {
      type: 'tile-error',
      key,
      error: timedOut
        ? `timed out after ${FETCH_TIMEOUT_MS / 1000}s`
        : err instanceof Error ? err.message : String(err),
    }
    self.postMessage(msg)
  } finally {
    clearTimeout(timer)
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
