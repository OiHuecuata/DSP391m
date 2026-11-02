import { test, expect } from '@playwright/test';
import { normalizeSnapshot } from '../src/features/courts/normalize';

test('normalization counts usable coordinates and rejects unsafe links', () => {
  const result = normalizeSnapshot({ posts: [
    { id: 'one', court_name: '<script>test</script>', address: 'Hà Nội', latitude: 0, longitude: 0, url: 'javascript:alert(1)' },
    { id: 'one', latitude: 10, longitude: 106 },
    { id: 'two', latitude: 0, longitude: 0 },
    { id: 'three', latitude: 91, longitude: 106 },
    { id: 'four', latitude: null, longitude: null },
  ] }, [{ canonical_name: 'Hà Nội', aliases: ['ha noi'] }], '2026-10-28', 'snapshot.json');
  expect(result.total).toBe(4);
  expect(result.mapped).toBe(2);
  expect(result.unmapped).toBe(2);
  expect(result.duplicateCoordinates).toBe(1);
  expect(result.venues[0].url).toBeNull();
  expect(result.venues[0].area).toBe('Hà Nội');
  expect(result.venues[0].name).toBe('<script>test</script>');
});

test('real snapshot endpoint reports the supplied local file', async ({ request }) => {
  const response = await request.get('/api/pbplus/venues');
  expect(response.ok()).toBeTruthy();
  const data = await response.json();
  expect(data).toMatchObject({ mode: 'local_snapshot', total: 337, mapped: 303, unmapped: 34 });
  expect(data.venues).toHaveLength(337);
  expect(response.headers()['cache-control']).toBe('no-store');
});

test('court page shows map, CSV preview and downloads in order without filters', async ({ page }) => {
  await page.goto('/collectors/pickleballplus');
  await expect(page.getByText('303 mapped / 337 total listings', { exact: false })).toBeVisible();
  const map = page.getByRole('region', {name:'Map of geocoded PickleballPlus venue listings'});
  const table = page.getByRole('table');
  const download = page.getByRole('button', {name:'Download file'});
  await expect(page.getByRole('searchbox')).toHaveCount(0);
  await expect(page.getByRole('checkbox')).toHaveCount(0);
  const positions = await Promise.all([map,table,download].map(locator=>locator.evaluate(el=>el.getBoundingClientRect().top+scrollY)));
  expect(positions[0]).toBeLessThan(positions[1]); expect(positions[1]).toBeLessThan(positions[2]);
  await table.getByRole('button', {name:'Show on map'}).first().click();
  await expect(page.getByRole('complementary', {name:'Selected venue'})).toBeVisible();
  await page.setViewportSize({width:390,height:844});
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
});

test('snapshot failure offers a retry', async ({ page }) => {
  await page.route('**/api/pbplus/venues', route => route.fulfill({ status: 503, json: { error: 'Snapshot unavailable' } }));
  await page.goto('/collectors/pickleballplus');
  await expect(page.getByRole('alert').filter({ hasText: 'Snapshot unavailable' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Refresh snapshot' })).toBeEnabled();
});
