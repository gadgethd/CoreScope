'use strict';

const assert = require('node:assert/strict');
const { readFile } = require('node:fs/promises');
const path = require('node:path');
const { chromium } = require('playwright');

async function main() {
  const publicDir = path.resolve(__dirname, '../../public');
  const index = (await readFile(path.join(publicDir, 'index.html'), 'utf8')).replaceAll('__BUST__', 'fixture');
  const candidate = index
    .replace(/(<(?:link|script) [^<>\n]*?(?:href|src)=")([a-zA-Z][^"<>:\s]*")/g, '$1/corescope/static/$2')
    .replace('<!-- UKMESH_APP_CONFIG -->', '<script src="/corescope/config.js" data-ukmesh-app-config></script>');
  const browser = await chromium.launch({ args: ['--no-sandbox', '--disable-dev-shm-usage'] });
  try {
    const context = await browser.newContext({ viewport: { width: 390, height: 844 } });
    const page = await context.newPage();
    const unexpected = [];
    const socketPaths = [];
    const networkPaths = [];
    const rasterPaths = [];
    let generation = 1;
    let privateReady = true;
    let readyTiles = 1;
    let authorityHold;
    const origin = 'https://candidate.example.test';
    const sampleRaster = path.join(publicDir, 'vendor/leaflet-1.9.4/images/layers.png');
    const rasterTier = tier => ({
      tiles: [
        { image: `tiles/${tier}/0-0.png`, bounds: { South: 51, North: 54, West: -3, East: 1 } },
        { image: `tiles/${tier}/0-1.png`, bounds: { South: 51, North: 54, West: -2, East: 2 } },
      ].slice(0, readyTiles),
    });
    const nodes = [{
      public_key: 'a'.repeat(64), name: 'Sample repeater', role: 'repeater', lat: 52.3, lon: -1.3,
    }];
    const response = pathname => ({
      '/api/config/map': { center: [54.5, -3.2], zoom: 7 },
      '/api/nodes': nodes,
      ['/api/nodes/' + nodes[0].public_key]: { node: nodes[0] },
      '/api/observers': [],
      '/api/packets': { packets: [] },
      '/api/packets/timestamps': [],
      '/api/channels': [],
      '/api/stats': {},
    })[pathname] ?? {};

    page.on('pageerror', error => unexpected.push(error.message));
    await page.routeWebSocket('**/*', socket => {
      socketPaths.push(new URL(socket.url()).pathname + new URL(socket.url()).search);
    });
    await page.route('**/*', async route => {
      const url = new URL(route.request().url());
      if (url.origin !== origin) return route.abort();
      let file = url.pathname;
      if (file === '/') return route.fulfill({ contentType: 'text/html', body: candidate });
      if (file === '/standalone') return route.fulfill({ contentType: 'text/html', body: index });
      if (file === '/corescope/config.js') {
        return route.fulfill({ contentType: 'application/javascript', body: `window.CORE_SCOPE_DEPLOYMENT = {
          ukmeshApp: true, apiBase: '/corescope/api', wsPath: '/corescope/ws', privacyUrl: '/api/runtime-config'
        }` });
      }
      if (file === '/api/runtime-config') {
        if (authorityHold) await authorityHold();
        return route.fulfill({ status: privateReady ? 200 : 503, json: { privacyGeneration: generation } });
      }
      if (file === '/rf-coverage/meta.json') {
        const progress = { state: 'computing', completed_tiles: readyTiles, total_tiles: 2 };
        return route.fulfill({ json: {
          run: { id: 'fixture', tiers: { standard: progress, precision: progress } },
          coverage: { standard: rasterTier('standard'), precision: rasterTier('precision') },
        } });
      }
      if (file.startsWith('/rf-coverage/tiles/')) {
        rasterPaths.push(file + url.search);
        return route.fulfill({ path: sampleRaster, contentType: 'image/png' });
      }
      if (file.startsWith('/corescope/api/') || file.startsWith('/api/')) {
        networkPaths.push(file);
        const upstream = file.replace(/^\/corescope/, '');
        return route.fulfill({
          headers: { 'X-CoreScope-Privacy-Generation': String(generation) },
          json: response(upstream),
        });
      }
      file = file.replace(/^\/corescope\/static/, '').slice(1);
      if (!file || file.includes('..')) return route.abort();
      try {
        const asset = path.join(publicDir, file);
        await readFile(asset);
        return route.fulfill({ path: asset });
      } catch {
        return route.fulfill({ status: 404, body: 'missing fixture asset' });
      }
    });

    const oldUrl = origin + '/?map=52.30000,-1.30000,10.25&layers=heatmap,coverage&note=Roof+trial&node=' + nodes[0].public_key;
    await page.goto(oldUrl);
    await page.locator('#liveMap.leaflet-container').waitFor();
    await page.getByRole('button', { name: 'Copy view link' }).waitFor();
    await page.getByText('Roof trial').waitFor();
    assert.ok(await page.locator('#liveFeed').evaluate(feed => feed.classList.contains('hidden')));
    assert.equal(new URL(page.url()).searchParams.get('map'), '52.30000,-1.30000,10.25');
    await page.getByRole('heading', { name: 'Sample repeater' }).waitFor();
    await page.locator('#nodeDetailClose').click();
    assert.ok(networkPaths.includes('/corescope/api/nodes'));
    assert.ok(networkPaths.every(value => value.startsWith('/corescope/api/')));
    if (!socketPaths.includes('/corescope/ws?privacyGeneration=1')) {
      await page.waitForEvent('websocket', { predicate: socket => socket.url().includes('privacyGeneration=1') });
    }
    await page.getByText('1 of 2 tiles ready').waitFor();
    assert.equal(await page.getByRole('combobox', { name: 'RF coverage tier' }).inputValue(), 'standard');
    await page.waitForFunction(() => [...document.querySelectorAll('.ukmesh-rf-tile.leaflet-tile-loaded')]
      .some(canvas => canvas.getContext('2d').getImageData(0, 0, 256, 256).data.some((value, i) => i % 4 === 3 && value)));
    assert.ok(rasterPaths.some(value => value.startsWith('/rf-coverage/tiles/standard/0-0.png')));
    readyTiles = 2;
    await page.waitForResponse(response => response.url().includes('/standard/0-1.png?revision=fixture-2'))
    await page.getByText('2 of 2 tiles ready').waitFor();
    await Promise.all([
      page.waitForResponse(response => response.url().includes('/precision/0-0.png?revision=fixture-2')),
      page.getByRole('combobox', { name: 'RF coverage tier' }).selectOption('precision'),
    ]);
    assert.equal(await page.evaluate(() => localStorage.getItem('meshcore-rf-coverage-tier-v1')), 'precision');
    if (process.env.UKMESH_BROWSER_SCREENSHOT) {
      await page.screenshot({ path: process.env.UKMESH_BROWSER_SCREENSHOT });
    }

    const visibility = hidden => page.evaluate(value => {
      Object.defineProperty(document, 'hidden', { configurable: true, value });
      document.dispatchEvent(new Event('visibilitychange'));
    }, hidden);
    await page.evaluate(() => { document.documentElement.dataset.fixtureDocument = 'original'; });
    await visibility(true);
    let releaseBeforeResume;
    let releaseAfterResume;
    const beforeResume = new Promise(resolve => { releaseBeforeResume = resolve; });
    const afterResume = new Promise(resolve => { releaseAfterResume = resolve; });
    let checks = 0;
    authorityHold = () => ++checks === 1 ? beforeResume : afterResume;
    await Promise.all([
      page.waitForRequest('**/api/runtime-config'),
      page.evaluate(() => { void CoreScopeDeployment.refresh(); }),
    ]);
    await visibility(false);
    const refreshed = page.waitForRequest('**/api/runtime-config');
    releaseBeforeResume();
    await refreshed;
    await page.getByText('Public data is temporarily unavailable.').waitFor({ state: 'visible' });
    assert.equal(await page.locator('#liveMap').isVisible(), false);
    authorityHold = undefined;
    releaseAfterResume();
    await page.locator('#liveMap.leaflet-container').waitFor({ state: 'visible' });
    assert.equal(await page.locator('html').getAttribute('data-fixture-document'), 'original');

    privateReady = false;
    await page.getByText('Public data is temporarily unavailable.').waitFor({ state: 'visible' });
    assert.equal(await page.locator('#liveMap').isVisible(), false);
    generation = 2;
    privateReady = true;
    await page.locator('#liveMap.leaflet-container').waitFor({ state: 'visible' });
    assert.ok(socketPaths.includes('/corescope/ws?privacyGeneration=2'));

    await page.goto(origin + '/?map=52.30000,-1.30000,10.25#region=UKNorth');
    await page.getByText('UK Mesh region UKNorth is unavailable on this map.').waitFor();
    assert.equal(await page.locator('#liveMap').count(), 0);

    await page.goto(origin + '/#/live');
    await page.getByRole('checkbox', { name: 'RF coverage' }).waitFor();

    await page.goto(origin + '/standalone#/live');
    await page.locator('#liveMap.leaflet-container').waitFor();
    assert.ok(networkPaths.some(value => value.startsWith('/api/')));
    assert.ok(socketPaths.includes('/'));
    assert.deepEqual(unexpected, []);
    console.log('UKMesh shell, legacy map state, RF progress and tiers, hidden-tab privacy, generation recovery and standalone routes passed');
  } finally {
    await browser.close();
  }
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
