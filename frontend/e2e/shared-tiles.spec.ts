/**
 * SharedTileStore with a fake GL context and worker client: one fetch and
 * one texture per URL, however many managers ask for it (#43).
 */
import { expect, test } from '@playwright/test'
import { SharedTileStore } from '../src/layers/shared-tiles'
import type { TileFetchClient, TileFetchError, TileFetchResult } from '../src/workers/TileFetchClient'

function setup() {
  let nextTexture = 0
  const deleted: number[] = []
  const gl = new Proxy({
    createTexture: () => ({ id: ++nextTexture }),
    deleteTexture: (texture: { id: number }) => { deleted.push(texture.id) },
  } as Record<string, unknown>, {
    // Any other GL call or constant is a no-op here.
    get: (target, name: string) => (name in target ? target[name] : () => 0),
  }) as unknown as WebGL2RenderingContext

  const fetched: string[] = []
  const cancelled: string[] = []
  let onLoaded!: (result: TileFetchResult) => void
  let onError!: (error: TileFetchError) => void
  const client = {
    fetch: (key: string) => { fetched.push(key) },
    cancel: (key: string) => { cancelled.push(key) },
    addLoadedListener: (listener: (result: TileFetchResult) => void) => { onLoaded = listener; return () => {} },
    addErrorListener: (listener: (error: TileFetchError) => void) => { onError = listener; return () => {} },
  } as unknown as TileFetchClient

  const store = new SharedTileStore(gl, client, 'png')
  const load = (url: string) => onLoaded({ key: url, format: 'png', data: { close() {} } } as unknown as TileFetchResult)
  const fail = (url: string) => onError({ key: url, error: 'HTTP 503' } as TileFetchError)
  return { store, fetched, cancelled, deleted, load, fail }
}

const URL_A = '/tiles/gfs/run/wind_u/0/data/3/2/2.png'

test('two managers asking for one tile share one fetch and one texture', () => {
  const { store, fetched, deleted, load } = setup()
  const got: unknown[] = []
  store.request(URL_A, 1, (t) => got.push(t))
  store.request(URL_A, 0, (t) => got.push(t)) // the second asks with a higher priority

  expect(fetched).toEqual([URL_A, URL_A]) // fetched once, then raised to priority 0
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
  expect(fetched).toEqual([URL_A])
})

test('a pending fetch is cancelled only when its last waiter withdraws', () => {
  const { store, cancelled, deleted } = setup()
  const withdrawA = store.request(URL_A, 0, () => {})
  const withdrawB = store.request(URL_A, 0, () => {})

  withdrawA()
  expect(cancelled).toEqual([])
  withdrawB()
  expect(cancelled).toEqual([URL_A])
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
  expect(fetched).toEqual([URL_A, URL_A])
})
