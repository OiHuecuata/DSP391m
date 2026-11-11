import { test, expect } from '@playwright/test';

test('compact header has no global search and navigation works on desktop and mobile', async ({ page }) => {
  await page.goto('/');
  await expect(page.getByRole('search')).toHaveCount(0);
  const nav = page.getByRole('navigation', { name: 'Main navigation' });
  await expect(nav).toBeVisible();
  await page.screenshot({ path: '/home/coara/Documents/Codex/2026-10-26/i-j/outputs/tracker-header-desktop.png', fullPage: true });
  await page.getByRole('button', { name: 'Collectors', exact: true }).hover();
  await nav.getByRole('link', { name: 'YouTube', exact: true }).click();
  await expect(page).toHaveURL(/collectors\/youtube$/);
  await expect(page.locator('#collector-options')).toBeHidden();
  await page.getByRole('link', { name: 'PickleballDB home' }).click();
  await expect(page).toHaveURL(/\/$/);
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(nav).toBeHidden();
  await expect(page.getByRole('search')).toHaveCount(0);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  await page.screenshot({ path: '/home/coara/Documents/Codex/2026-10-26/i-j/outputs/tracker-header-mobile.png', fullPage: true });
  await page.getByRole('button', { name: 'Menu', exact: true }).click();
  await page.getByRole('button', { name: 'Collectors', exact: true }).click();
  await nav.getByRole('link', { name: 'Threads', exact: true }).click();
  await expect(page).toHaveURL(/collectors\/threads$/);
  await expect(nav).toBeHidden();
});
