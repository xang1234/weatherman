import { test, expect } from '@playwright/test'
import { mockApiRoutes, FORECAST_HOURS } from './fixtures'

test.beforeEach(async ({ page }) => {
  await mockApiRoutes(page)
})

test('forecast controls render with first hour selected', async ({ page }) => {
  await page.goto('/')
  const bar = page.locator('input[type="range"]')
  await expect(bar).toBeVisible({ timeout: 10_000 })

  // Slider should be at position 0 (first forecast hour)
  await expect(bar).toHaveValue('0')

  // F-hour label should show F000
  await expect(page.getByText('F000')).toBeVisible()
})

test('step forward button advances forecast hour', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText('F000')).toBeVisible({ timeout: 10_000 })

  // Click step-forward (❯ = \u276F)
  const forwardBtn = page.locator('button').filter({ hasText: '❯' })
  await forwardBtn.click()

  // Should now show F003
  await expect(page.getByText(`F${FORECAST_HOURS[1].toString().padStart(3, '0')}`)).toBeVisible()
  await expect(page.locator('input[type="range"]')).toHaveValue('1')
})

test('step back button is disabled at first hour', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText('F000')).toBeVisible({ timeout: 10_000 })

  const backBtn = page.locator('button').filter({ hasText: '❮' })
  await expect(backBtn).toBeDisabled()
})

test('step forward button is disabled at last hour', async ({ page }) => {
  // Navigate with last forecast hour pre-selected
  await page.goto(`/?fh=${FORECAST_HOURS[FORECAST_HOURS.length - 1]}`)
  const lastLabel = `F${FORECAST_HOURS[FORECAST_HOURS.length - 1].toString().padStart(3, '0')}`
  await expect(page.getByText(lastLabel)).toBeVisible({ timeout: 10_000 })

  const forwardBtn = page.locator('button').filter({ hasText: '❯' })
  await expect(forwardBtn).toBeDisabled()
})

test('slider change updates forecast hour label', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText('F000')).toBeVisible({ timeout: 10_000 })

  const slider = page.locator('input[type="range"]')
  await slider.fill('3')

  await expect(page.getByText(`F${FORECAST_HOURS[3].toString().padStart(3, '0')}`)).toBeVisible()
})

test('play button toggles to pause icon', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText('F000')).toBeVisible({ timeout: 10_000 })

  // Play button should show ▶ initially
  const playBtn = page.locator('button').filter({ hasText: '▶' })
  await expect(playBtn).toBeVisible()
  await playBtn.click()

  // Should now show ⏸
  await expect(page.locator('button').filter({ hasText: '⏸' })).toBeVisible()
})

test('forecast hour is persisted in URL', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText('F000')).toBeVisible({ timeout: 10_000 })

  const forwardBtn = page.locator('button').filter({ hasText: '❯' })
  await forwardBtn.click()
  await expect(page.getByText('F003')).toBeVisible()

  // URL should have ?fh=3
  expect(page.url()).toContain('fh=3')
})
