import { expect, test } from '@playwright/test'
import { formatLatLon } from '../src/utils/format'

test('coordinates wrap into ±180° and use hemispheres', () => {
  expect(formatLatLon(0, 194.12)).toBe('0.00°N, 165.88°W') // one world copy east (#65)
  expect(formatLatLon(-33.87, 151.21)).toBe('33.87°S, 151.21°E')
  expect(formatLatLon(51.5, -0.12)).toBe('51.50°N, 0.12°W')
  expect(formatLatLon(12.3456, -525.5, 4)).toBe('12.3456°N, 165.5000°W') // two copies west
})
