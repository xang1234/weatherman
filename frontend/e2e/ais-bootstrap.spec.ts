import { test, expect } from '@playwright/test'
import { AIS_DATE, mockApiRoutes } from './fixtures'

test('AIS layer bootstraps from /ais/tiles/latest on load', async ({ page }) => {
  let latestCalled = false

  await mockApiRoutes(page)
  // Override after mockApiRoutes — later routes take priority in Playwright
  await page.route('**/ais/tiles/latest', (route) => {
    latestCalled = true
    return route.fulfill({ json: { snapshot_date: AIS_DATE } })
  })

  await page.goto('/')
  await expect(page.getByText('F000')).toBeVisible({ timeout: 10_000 })

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
  await expect(page.getByText('F000')).toBeVisible({ timeout: 10_000 })

  // No error messages in the UI
  await expect(page.locator('text=Error')).not.toBeVisible()
})
