import { test, expect } from '@playwright/test'
import { AIS_DATE, forecastLabel, mockApiRoutes } from './fixtures'

test('AIS layer bootstraps from /ais/tiles/latest on load', async ({ page }) => {
  let latestCalled = false

  await mockApiRoutes(page)
  // Override after mockApiRoutes — later routes take priority in Playwright
  await page.route('**/ais/tiles/latest', (route) => {
    latestCalled = true
    return route.fulfill({ json: { snapshot_date: AIS_DATE } })
  })

  await page.goto('/')
  await expect(page.getByText(forecastLabel(0))).toBeVisible({ timeout: 10_000 })

  expect(latestCalled).toBe(true)
})

test('AIS layer handles missing snapshot gracefully', async ({ page }) => {
  await mockApiRoutes(page)
  // Override after mockApiRoutes — later routes take priority in Playwright
  await page.route('**/ais/tiles/latest', (route) =>
    route.fulfill({ status: 404, body: 'Not Found' }),
  )

  await page.goto('/')
  // App should still render forecast controls without crashing
  await expect(page.getByText(forecastLabel(0))).toBeVisible({ timeout: 10_000 })

  // No error messages in the UI
  await expect(page.locator('text=Error')).not.toBeVisible()
})

test('a same-day AIS rebuild reloads the vessel tiles (#72)', async ({ page }) => {
  await mockApiRoutes(page)
  await page.route('**/ais/tiles/latest', (route) =>
    route.fulfill({ json: { snapshot_date: AIS_DATE, revision: 1 } }),
  )
  // Each SSE connection delivers one ais.refreshed and ends; EventSource then
  // reconnects (after `retry` ms) and gets the next. Same date, new revision.
  let connections = 0
  await page.route('**/events/stream', (route) => {
    connections++
    const revision = connections === 1 ? 1 : 2
    return route.fulfill({
      status: 200,
      headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' },
      body: `retry: 200\nid: ${connections}\nevent: ais.refreshed\ndata: ${JSON.stringify({
        ais_date: AIS_DATE,
        tile_url_template: `/ais/tiles/${AIS_DATE}/{z}/{x}/{y}.pbf`,
        revision,
      })}\n\n`,
    })
  })
  const revisions = new Set<string>()
  page.on('request', (r) => {
    const m = r.url().match(new RegExp(`/ais/tiles/${AIS_DATE}/\\d+/\\d+/\\d+\\.pbf\\?.*rev=(\\d+)`))
    if (m) revisions.add(m[1])
  })

  await page.goto('/')
  await expect.poll(() => [...revisions].sort(), { timeout: 15_000 }).toEqual(['1', '2'])
})

test('AIS events without a revision still reload the tiles each time', async ({ page }) => {
  // notify_ais.py and older producers send no revision.
  await mockApiRoutes(page)
  let connections = 0
  await page.route('**/events/stream', (route) => {
    connections++
    return route.fulfill({
      status: 200,
      headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' },
      body: `retry: 200\nid: ${connections}\nevent: ais.refreshed\ndata: ${JSON.stringify({
        ais_date: AIS_DATE,
        tile_url_template: `/ais/tiles/${AIS_DATE}/{z}/{x}/{y}.pbf`,
      })}\n\n`,
    })
  })
  const revisions = new Set<string>()
  page.on('request', (r) => {
    const m = r.url().match(new RegExp(`/ais/tiles/${AIS_DATE}/\\d+/\\d+/\\d+\\.pbf\\?.*rev=(-\\d+)`))
    if (m) revisions.add(m[1])
  })

  await page.goto('/')
  await expect.poll(() => revisions.size, { timeout: 15_000 }).toBeGreaterThanOrEqual(2)
})
