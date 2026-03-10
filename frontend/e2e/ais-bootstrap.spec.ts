import { test, expect } from '@playwright/test'
import { AIS_DATE, CATALOG_RESPONSE, MANIFEST_RESPONSE, RUN_ID } from './fixtures'

test('AIS layer bootstraps from /ais/tiles/latest on load', async ({ page }) => {
  // Track whether the latest-AIS endpoint was called
  let latestCalled = false
  let tileRequested = false

  await page.route('**/api/catalog/gfs', (route) =>
    route.fulfill({ json: CATALOG_RESPONSE }),
  )
  await page.route(`**/api/manifest/gfs/${RUN_ID}`, (route) =>
    route.fulfill({ json: MANIFEST_RESPONSE }),
  )
  await page.route('**/ais/tiles/latest', (route) => {
    latestCalled = true
    return route.fulfill({ json: { snapshot_date: AIS_DATE } })
  })
  await page.route('**/ais/tiles/*/[0-9]*/**', (route) => {
    tileRequested = true
    return route.fulfill({
      body: Buffer.alloc(0),
      contentType: 'application/x-protobuf',
    })
  })
  await page.route('**/tiles/gfs/**', (route) =>
    route.fulfill({
      body: Buffer.from(
        'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVQI12NgAAIABQAB' +
          'Nl7BcQAAAABJRU5ErkJggg==',
        'base64',
      ),
      contentType: 'image/png',
    }),
  )
  await page.route('**/events/stream', (route) =>
    route.fulfill({
      status: 200,
      headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' },
      body: ':ok\n\n',
    }),
  )
  await page.route('**/v1/edr/**', (route) =>
    route.fulfill({ json: {} }),
  )

  await page.goto('/')
  // Wait for the map to fully load and render
  await expect(page.getByText('F000')).toBeVisible({ timeout: 10_000 })

  // The app should have fetched /ais/tiles/latest
  expect(latestCalled).toBe(true)
})

test('AIS layer handles missing snapshot gracefully', async ({ page }) => {
  await page.route('**/api/catalog/gfs', (route) =>
    route.fulfill({ json: CATALOG_RESPONSE }),
  )
  await page.route(`**/api/manifest/gfs/${RUN_ID}`, (route) =>
    route.fulfill({ json: MANIFEST_RESPONSE }),
  )
  // Return 404 for latest AIS — no snapshot available
  await page.route('**/ais/tiles/latest', (route) =>
    route.fulfill({ status: 404, body: 'Not Found' }),
  )
  await page.route('**/tiles/gfs/**', (route) =>
    route.fulfill({
      body: Buffer.from(
        'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVQI12NgAAIABQAB' +
          'Nl7BcQAAAABJRU5ErkJggg==',
        'base64',
      ),
      contentType: 'image/png',
    }),
  )
  await page.route('**/events/stream', (route) =>
    route.fulfill({
      status: 200,
      headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' },
      body: ':ok\n\n',
    }),
  )

  await page.goto('/')
  // App should still render forecast controls without crashing
  await expect(page.getByText('F000')).toBeVisible({ timeout: 10_000 })

  // No error messages in the UI
  await expect(page.locator('text=Error')).not.toBeVisible()
})
