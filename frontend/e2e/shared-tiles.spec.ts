/**
 * SharedTileStore with a fake GL context and worker client: one fetch and
 * one texture per URL, however many managers ask for it (#43).
 */
import { expect, test } from '@playwright/test'
import { SharedTileStore, acquireSharedTileStore } from '../src/layers/shared-tiles'
import type { TileFetchClient, TileFetchError, TileFetchResult } from '../src/workers/TileFetchClient'

function fakeGl() {
  let nextTexture = 0
  const deleted: number[] = []
  const options = { contextLost: false }
  const gl = new Proxy({
    createTexture: () => (options.contextLost ? null : { id: ++nextTexture }),
    deleteTexture: (texture: { id: number }) => { deleted.push(texture.id) },
  } as Record<string, unknown>, {
    // Any other GL call or constant is a no-op here.
    get: (target, name: string) => (name in target ? target[name] : () => 0),
  }) as unknown as WebGL2RenderingContext
  return { gl, deleted, options }
}

/** A worker client that, like the real one, broadcasts results to every listener. */
function fakeClient() {
  const fetched: string[] = []
  const cancelled: string[] = []
  const loaded = new Set<(result: TileFetchResult) => void>()
  const errored = new Set<(error: TileFetchError) => void>()
  const client = {
    fetch: (key: string) => { fetched.push(key) },
    cancel: (key: string) => { cancelled.push(key) },
    addLoadedListener: (listener: (result: TileFetchResult) => void) => { loaded.add(listener); return () => loaded.delete(listener) },
    addErrorListener: (listener: (error: TileFetchError) => void) => { errored.add(listener); return () => errored.delete(listener) },
  } as unknown as TileFetchClient
  /** Deliver the result for the last fetch of `url`; returns how often the bitmap got closed. */
  const load = (url: string, key = fetched.findLast((k) => k.endsWith(`::${url}`))!) => {
    let closes = 0
    const result = { key, format: 'png', data: { close() { closes++ } } } as unknown as TileFetchResult
    for (const listener of [...loaded]) listener(result)
    return closes
  }
  const fail = (url: string) => {
    const key = fetched.findLast((k) => k.endsWith(`::${url}`))!
    for (const listener of [...errored]) listener({ key, error: 'HTTP 503' } as TileFetchError)
  }
  return { client, fetched, cancelled, load, fail, listeners: () => loaded.size + errored.size }
}

function setup() {
  const { gl, deleted, options } = fakeGl()
  const fake = fakeClient()
  const store = new SharedTileStore(gl, fake.client, 'png')
  return { store, deleted, options, ...fake }
}

const URL_A = '/tiles/gfs/run/wind_u/0/data/3/2/2.png'

test('two managers asking for one tile share one fetch and one texture', () => {
  const { store, fetched, deleted, load } = setup()
  const got: unknown[] = []
  store.request(URL_A, 1, (t) => got.push(t))
  store.request(URL_A, 0, (t) => got.push(t)) // the second asks with a higher priority

  expect(fetched).toHaveLength(2) // fetched once, then raised to priority 0
  expect(new Set(fetched).size).toBe(1)
  load(URL_A)
  expect(got).toHaveLength(2)
  expect(got[0]).toBe(got[1])
  expect(store.size).toBe(1)

  // The texture lives until both have released it.
  store.release(URL_A)
  expect(deleted).toEqual([])
  store.release(URL_A)
  expect(deleted).toHaveLength(1)
  expect(store.size).toBe(0)
})

test('a tile already loaded is handed over at once, without fetching', () => {
  const { store, fetched, load } = setup()
  let first: unknown
  store.request(URL_A, 0, (t) => { first = t })
  load(URL_A)

  let second: unknown
  store.request(URL_A, 0, (t) => { second = t })
  expect(second).toBe(first)
  expect(fetched).toHaveLength(1)
})

test('a pending fetch is cancelled only when its last waiter withdraws', () => {
  const { store, cancelled, deleted } = setup()
  const withdrawA = store.request(URL_A, 0, () => {})
  const withdrawB = store.request(URL_A, 0, () => {})

  withdrawA()
  expect(cancelled).toEqual([])
  withdrawB()
  expect(cancelled).toHaveLength(1)
  expect(cancelled[0]).toMatch(/::\/tiles\/gfs\/run\/wind_u/)
  expect(deleted).toHaveLength(1) // the placeholder texture
})

test('a failed fetch tells every waiter, and the next request fetches again', () => {
  const { store, fetched, fail } = setup()
  const got: unknown[] = []
  store.request(URL_A, 0, (t) => got.push(t))
  store.request(URL_A, 0, (t) => got.push(t))
  fail(URL_A)
  expect(got).toEqual([null, null])

  store.request(URL_A, 0, () => {})
  expect(fetched).toHaveLength(2)
})

test('stores sharing one worker client only take their own results', () => {
  // E.g. PNG and Float16 stores, or a remounted map's new context.
  const fake = fakeClient()
  const storeA = new SharedTileStore(fakeGl().gl, fake.client, 'png')
  const storeB = new SharedTileStore(fakeGl().gl, fake.client, 'png')
  const gotA: unknown[] = []
  const gotB: unknown[] = []
  storeA.request(URL_A, 0, (t) => gotA.push(t))
  storeB.request(URL_A, 0, (t) => gotB.push(t))
  const [keyA, keyB] = fake.fetched
  expect(keyA).not.toBe(keyB)

  // A's result reaches B's listener too: B must neither use nor close it,
  // so it is closed once, by A after uploading it.
  expect(fake.load(URL_A, keyA)).toBe(1)
  expect(gotA).toHaveLength(1)
  expect(gotB).toHaveLength(0)
  fake.load(URL_A, keyB)
  expect(gotB).toHaveLength(1)
})

test('the last user of a store disposes of it', () => {
  const { gl, deleted } = fakeGl()
  const fake = fakeClient()
  const first = acquireSharedTileStore(gl, fake.client, 'png')
  const second = acquireSharedTileStore(gl, fake.client, 'png')
  expect(second.store).toBe(first.store)
  first.store.request(URL_A, 0, () => {}) // a tile left loading

  first.done()
  first.done() // a second call from the same user changes nothing
  expect(fake.listeners()).toBe(2)
  second.done()
  expect(fake.listeners()).toBe(0) // no longer kept alive by the client
  expect(fake.cancelled).toHaveLength(1)
  expect(deleted).toHaveLength(1)

  // A new user gets a fresh store.
  expect(acquireSharedTileStore(gl, fake.client, 'png').store).not.toBe(first.store)
})

test('no texture (context lost) is a failure, and nothing is cached', () => {
  const { store, fetched, options } = setup()
  options.contextLost = true
  const got: unknown[] = []
  store.request(URL_A, 0, (t) => got.push(t))
  expect(got).toEqual([null])
  expect(fetched).toEqual([])

  // Once textures can be made again, the retry fetches.
  options.contextLost = false
  store.request(URL_A, 0, () => {})
  expect(fetched).toHaveLength(1)
})
