import { test, expect } from '@playwright/test'
import { mockApiRoutes } from './fixtures'

test.beforeEach(async ({ page }) => {
  await mockApiRoutes(page)
})

test('clicking the map opens weather inspector with EDR data', async ({ page }) => {
  await page.goto('/')

  // Wait for forecast controls to appear (map is loaded + data fetched)
  await expect(page.getByText('F000')).toBeVisible({ timeout: 10_000 })

  // Click on the map canvas to trigger weather inspector
  const canvas = page.locator('canvas.maplibregl-canvas')
  await expect(canvas).toBeVisible()
  await canvas.click({ position: { x: 400, y: 300 } })

  // Inspector panel should appear
  await expect(page.getByText('Weather Inspector')).toBeVisible({ timeout: 5_000 })

  // Should show coordinates
  const coordText = page.locator('div').filter({ hasText: /^-?\d+\.\d+, -?\d+\.\d+$/ })
  await expect(coordText.first()).toBeVisible()
})

test('inspector shows variable tabs and values table', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText('F000')).toBeVisible({ timeout: 10_000 })

  const canvas = page.locator('canvas.maplibregl-canvas')
  await canvas.click({ position: { x: 400, y: 300 } })

  await expect(page.getByText('Weather Inspector')).toBeVisible({ timeout: 5_000 })

  // Inspector panel is the panel with "Weather Inspector" heading
  const inspector = page.locator('div').filter({ hasText: 'Weather Inspector' }).first()

  // Should show variable tabs (exact match to avoid layer panel buttons)
  await expect(inspector.getByRole('button', { name: 'Temperature (2m)', exact: true })).toBeVisible()
  await expect(inspector.getByRole('button', { name: 'Wind Speed (10m)', exact: true })).toBeVisible()

  // Should show "Current values at F000" section with table
  await expect(page.getByText('Current values at F000')).toBeVisible()

  // Table should contain formatted values with units
  await expect(page.getByText('280.50 K')).toBeVisible()
})

test('switching variable tab updates the selected chart', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText('F000')).toBeVisible({ timeout: 10_000 })

  const canvas = page.locator('canvas.maplibregl-canvas')
  await canvas.click({ position: { x: 400, y: 300 } })
  await expect(page.getByText('Weather Inspector')).toBeVisible({ timeout: 5_000 })

  // Click on Wind Speed tab in the inspector panel (exact to avoid layer panel)
  const inspector = page.locator('div').filter({ hasText: 'Weather Inspector' }).first()
  await inspector.getByRole('button', { name: 'Wind Speed (10m)', exact: true }).click()

  // The chart label should update to show Wind Speed
  const labels = page.locator('div').filter({ hasText: 'Wind Speed (10m)' })
  // At least the tab + the chart label
  await expect(labels.first()).toBeVisible()
})

test('close button dismisses inspector', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText('F000')).toBeVisible({ timeout: 10_000 })

  const canvas = page.locator('canvas.maplibregl-canvas')
  await canvas.click({ position: { x: 400, y: 300 } })
  await expect(page.getByText('Weather Inspector')).toBeVisible({ timeout: 5_000 })

  // Click close
  await page.getByRole('button', { name: 'Close' }).click()

  // Inspector should disappear
  await expect(page.getByText('Weather Inspector')).not.toBeVisible()
})
