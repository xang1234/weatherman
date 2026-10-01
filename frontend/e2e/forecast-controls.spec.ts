import { test, expect } from '@playwright/test'
import { FORECAST_HOURS, forecastLabel, mockApiRoutes } from './fixtures'

test.beforeEach(async ({ page }) => {
  await mockApiRoutes(page)
})

test('forecast controls render with first hour selected', async ({ page }) => {
  await page.goto('/')
  const bar = page.locator('input[type="range"]')
  await expect(bar).toBeVisible({ timeout: 10_000 })

  // Slider should be at position 0 (first forecast hour)
  await expect(bar).toHaveValue('0')

  // Label shows the valid time of the first hour
  await expect(page.getByText(forecastLabel(0))).toBeVisible()
})

test('step forward button advances forecast hour', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText(forecastLabel(0))).toBeVisible({ timeout: 10_000 })

  // Click step-forward (❯ = \u276F)
  const forwardBtn = page.locator('button').filter({ hasText: '❯' })
  await forwardBtn.click()

  // Should now show the next hour's valid time
  await expect(page.getByText(forecastLabel(FORECAST_HOURS[1]))).toBeVisible()
  await expect(page.locator('input[type="range"]')).toHaveValue('1')
})

test('step back button is disabled at first hour', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText(forecastLabel(0))).toBeVisible({ timeout: 10_000 })

  const backBtn = page.locator('button').filter({ hasText: '❮' })
  await expect(backBtn).toBeDisabled()
})

test('step forward button is disabled at last hour', async ({ page }) => {
  // Navigate with last forecast hour pre-selected
  await page.goto(`/?fh=${FORECAST_HOURS[FORECAST_HOURS.length - 1]}`)
  const lastLabel = forecastLabel(FORECAST_HOURS[FORECAST_HOURS.length - 1])
  await expect(page.getByText(lastLabel)).toBeVisible({ timeout: 10_000 })

  const forwardBtn = page.locator('button').filter({ hasText: '❯' })
  await expect(forwardBtn).toBeDisabled()
})

test('slider change updates forecast hour label', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText(forecastLabel(0))).toBeVisible({ timeout: 10_000 })

  const slider = page.locator('input[type="range"]')
  await slider.fill('3')

  await expect(page.getByText(forecastLabel(FORECAST_HOURS[3]))).toBeVisible()
})

test('play button toggles to pause icon', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText(forecastLabel(0))).toBeVisible({ timeout: 10_000 })

  // Play button should show ▶ initially
  const playBtn = page.locator('button').filter({ hasText: '▶' })
  await expect(playBtn).toBeVisible()
  await playBtn.click()

  // Should now show ⏸
  await expect(page.locator('button').filter({ hasText: '⏸' })).toBeVisible()
})

test('forecast hour is persisted in URL', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText(forecastLabel(0))).toBeVisible({ timeout: 10_000 })

  const forwardBtn = page.locator('button').filter({ hasText: '❯' })
  await forwardBtn.click()
  await expect(page.getByText(forecastLabel(3))).toBeVisible()

  // URL should have ?fh=3
  expect(page.url()).toContain('fh=3')
})
