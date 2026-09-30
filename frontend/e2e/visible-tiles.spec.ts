/** Pure tile maths — no browser page needed. */
import { expect, test } from '@playwright/test'
import { computeVisibleTiles } from '../src/layers/TileManager'

/** Distinct columns as "x@wrap", in order. */
const columns = (bounds: { west: number; east: number }, z: number) =>
  [...new Set(computeVisibleTiles({ north: 40, south: 10, ...bounds }, z).map((t) => `${t.x}@${t.wrap}`))]

test('tiles around the antimeridian land on the world copy that is on screen', () => {
  // z4 has 16 columns of 22.5°. MapLibre unwraps longitudes around the map centre.

  // Centre just east of the line (say 180°): the view runs past +180.
  expect(columns({ west: 153.6, east: 206.4 }, 4)).toEqual(['14@0', '15@0', '0@1', '1@1'])

  // Centre just west of it (-180°): the same ground, but reported below -180.
  // The columns west of the line belong to the copy west of the primary world.
  expect(columns({ west: -206.4, east: -153.6 }, 4)).toEqual(['14@-1', '15@-1', '0@0', '1@0'])

  // Nowhere near the line: one world copy.
  expect(columns({ west: -40, east: 10 }, 4)).toEqual(['6@0', '7@0', '8@0'])
})

test('a view wider than the world covers every copy in it', () => {
  // z1 has 2 columns of 180°; 720° of longitude is two whole worlds plus the column the edge falls in.
  expect(columns({ west: -360, east: 360 }, 1)).toEqual(['1@-1', '0@0', '1@0', '0@1', '1@1'])
})
