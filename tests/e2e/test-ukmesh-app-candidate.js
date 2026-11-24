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
    const nativeMapReads = [];
    const nativeLayerReads = [];
    const rasterPaths = [];
    let generation = 1;
    let privateReady = true;
    let regionScoping = true;
    let runtimeVersion;
    let readyTiles = 1;
    let authorityHold;
    const origin = 'https://candidate.example.test';
    const sampleRaster = path.join(publicDir, 'vendor/leaflet-1.9.4/images/layers.png');
    const terrainTile = Buffer.from(await page.evaluate(() => {
      const canvas = document.createElement('canvas');
      canvas.width = canvas.height = 256;
      const context = canvas.getContext('2d');
      const sample = context.createImageData(256, 256);
      for (let pixel = 0; pixel < 256 * 256; pixel++) {
        sample.data[pixel * 4] = 128 + Math.floor(pixel % 256 / 8);
        sample.data[pixel * 4 + 3] = 255;
      }
      context.putImageData(sample, 0, 0);
      return canvas.toDataURL().split(',')[1];
    }), 'base64');
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
      ['/api/nodes/' + 'b'.repeat(64)]: { node: { ...nodes[0], public_key: 'b'.repeat(64), name: 'North relay' } },
      '/api/observers': [],
      '/api/packets': { packets: [] },
      '/api/packets/timestamps': [],
      '/api/channels': [],
      '/api/stats': {},
    })[pathname] ?? {};

    page.on('pageerror', error => unexpected.push(error.message));
    await page.addInitScript(() => {
      window.fixtureWorkerCalls = [];
      Object.defineProperty(navigator, 'serviceWorker', {
        configurable: true,
        value: {
          async getRegistration(scope) {
            window.fixtureWorkerCalls.push(['lookup', scope]);
            return new URLSearchParams(location.search).has('source') ? { scope } : undefined;
          },
          async register(url, options) {
            window.fixtureWorkerCalls.push(['register', url, options.scope]);
          },
        },
      });
    });
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
          ukmeshApp: true, ukmeshPwa: true, apiBase: '/corescope/api', wsPath: '/corescope/ws', privacyUrl: '/api/runtime-config'
        }` });
      }
      if (file === '/api/runtime-config') {
        if (authorityHold) await authorityHold();
        const runtime = runtimeVersion === undefined ? { privacyGeneration: generation } : {
          privacyGeneration: generation, version: runtimeVersion, packetArcs: false,
          heatmap: true, regionScoping, refreshAfterSeconds: 30,
        };
        return route.fulfill({ status: privateReady ? 200 : 503, json: runtime });
      }
      if (file === '/api/regions/summary') {
        assert.equal(url.searchParams.get('network'), 'ukmesh');
        return route.fulfill({ json: { regions: [{ name: 'UKNorth' }, { name: 'UKSouth' }] } });
      }
      if (file === '/api/nodes/map') {
        nativeMapReads.push(url.searchParams);
        const north = url.searchParams.get('region') === 'UKNorth';
        const cursor = url.searchParams.get('cursor');
        const nodeId = north ? (cursor ? 'c' : 'b') : 'd';
        const complete = !north || !!cursor;
        return route.fulfill({ json: {
          nodes: [{ node_id: nodeId.repeat(64), role: 2, lat: cursor ? 52.33 : 52.3, lon: cursor ? -1.25 : -1.3, name: nodeId }],
          page: { complete, nextCursor: complete ? null : 'page2', snapshot: 'fixture-snapshot' },
        } });
      }
      if (file === '/api/observers/health') {
        nativeLayerReads.push(url);
        return route.fulfill({ json: [
          { node_id: 'b'.repeat(64), lat: 52.3, lon: -1.3, name: 'North observer', uptime_pct: 99 },
          { node_id: 'd'.repeat(64), lat: 52.2, lon: -1.1, name: 'South observer', uptime_pct: 90 },
        ] });
      }
      if (file === '/api/path-beta/multibyte-paths') {
        nativeLayerReads.push(url);
        return route.fulfill({ json: { segments: [{ positions: [[52.3, -1.31], [52.33, -1.27]], count: 3 }] } });
      }
      if (file === '/api/packets/recent') {
        nativeLayerReads.push(url);
        const packet = {
          packet_hash: 'c'.repeat(16), time: '2026-11-24T11:00:00.000Z',
          summary: url.searchParams.get('region') + ' traffic',
        };
        return route.fulfill({ json: [packet, packet] });
      }
      if (file === '/api/nodes/' + 'b'.repeat(64) + '/links') {
        nativeLayerReads.push(url);
        return route.fulfill({ json: [{ peer_id: 'c'.repeat(64), observed_count: 3, itm_path_loss_db: 121 }] });
      }
      if (file.startsWith('/api/links/') && file.endsWith('/history')) {
        nativeLayerReads.push(url);
        return route.fulfill({ json: { points: [{ snr: 8.5, path_loss: 121 }] } });
      }
      if (file.startsWith('/terrain-tiles/')) {
        nativeLayerReads.push(url);
        return route.fulfill({ body: terrainTile, contentType: 'image/png' });
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
    await page.getByText('Public data is temporarily unavailable.').waitFor({ state: 'visible' });
    assert.equal(await page.locator('#liveMap').count(), 0);
    runtimeVersion = 1;
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

    await page.setViewportSize({ width: 1024, height: 844 });
    await page.goto(origin + '/?map=52.30000,-1.30000,10.25&layers=feed,observers,paths,links,terrain&node=' + 'b'.repeat(64) + '#region=UKNorth');
    const region = page.getByRole('combobox', { name: 'Region', exact: true });
    await region.waitFor();
    assert.equal(await region.inputValue(), 'UKNorth');
    assert.deepEqual(await page.evaluate(() => Object.keys(window._liveNodeData()).sort()), ['b'.repeat(64), 'c'.repeat(64)]);
    assert.equal(nativeMapReads[1].get('snapshot'), 'fixture-snapshot');
    assert.equal(nativeMapReads[1].get('cursor'), 'page2');
    await page.getByText('UKNorth traffic', { exact: false }).waitFor().catch(async error => {
      console.error('Region feed state', await page.locator('#liveFeed').evaluate(feed => ({
        text: feed.textContent,
        className: feed.className,
        display: getComputedStyle(feed).display,
      })));
      console.error('Region feed requests', nativeLayerReads.filter(url => url.pathname === '/api/packets/recent').map(url => url.search));
      throw error;
    });
    assert.equal(await page.locator('#liveFeed .live-feed-item').count(), 1);
    assert.equal(await page.locator('#vcrBar').isVisible(), false);
    await page.locator('#nodeDetailClose').click();
    await page.getByText('Map layers', { exact: true }).click();
    await page.getByText('1 observer', { exact: true }).waitFor();
    await page.getByText('1 observed path', { exact: true }).waitFor();
    await page.getByText('1 observed link', { exact: true }).waitFor();
    const link = page.getByRole('button', { name: 'Observed link to c' });
    await link.focus();
    await link.press('Enter');
    await page.getByRole('button', { name: 'Load 72-hour link history' }).click();
    await page.getByText(/Last SNR 8.5 dB/).waitFor();
    await page.waitForFunction(() => [...document.querySelectorAll('.ukmesh-terrain-tile.leaflet-tile-loaded')]
      .some(canvas => canvas.getContext('2d').getImageData(0, 0, 256, 256).data.some((value, i) => i % 4 === 3 && value)));
    assert.ok(nativeLayerReads.some(url => url.pathname.endsWith('/links') && url.searchParams.get('region') === 'UKNorth'));
    assert.ok(nativeLayerReads.some(url => url.pathname.endsWith('/multibyte-paths') && url.searchParams.get('region') === 'UKNorth'));
    await region.selectOption('UKSouth');
    await page.waitForFunction(() => Object.keys(window._liveNodeData()).join() === 'd'.repeat(64));
    assert.equal(new URL(page.url()).hash, '#region=UKSouth');
    assert.equal(nativeMapReads.at(-1).get('network'), 'ukmesh');
    assert.equal(nativeMapReads.at(-1).get('region'), 'UKSouth');
    await page.getByText('UKSouth traffic', { exact: false }).waitFor();
    assert.equal(await page.getByText('UKNorth traffic', { exact: false }).count(), 0);
    assert.ok(nativeLayerReads.some(url => url.pathname.endsWith('/packets/recent') && url.searchParams.get('region') === 'UKSouth'));
    const namedReadsBeforeRevocation = nativeMapReads.length;
    regionScoping = false;
    await page.getByText('Region nodes unavailable', { exact: true }).waitFor();
    assert.equal(await page.getByText('UKSouth traffic', { exact: false }).count(), 0);
    assert.deepEqual(await page.evaluate(() => Object.keys(window._liveNodeData())), []);
    assert.equal(nativeMapReads.length, namedReadsBeforeRevocation);
    regionScoping = true;

    await page.goto(origin + '/#/live');
    await page.getByRole('checkbox', { name: 'RF coverage' }).waitFor();
    assert.equal(await page.locator('link[rel="manifest"]').getAttribute('href'), '/manifest.json');
    assert.deepEqual(await page.evaluate(() => window.fixtureWorkerCalls), [['lookup', '/'], ['register', '/sw.js', '/']]);
    await page.goto(origin + '/?source=pwa');
    await page.locator('#liveMap.leaflet-container').waitFor();
    assert.deepEqual(await page.evaluate(() => window.fixtureWorkerCalls), [['lookup', '/']]);
    runtimeVersion = 2;
    await page.getByText('Public data is temporarily unavailable.').waitFor({ state: 'visible' });
    assert.equal(await page.locator('#liveMap').isVisible(), false);
    runtimeVersion = 1;
    await page.locator('#liveMap.leaflet-container').waitFor({ state: 'visible' });

    await page.goto(origin + '/standalone#/live');
    await page.locator('#liveMap.leaflet-container').waitFor();
    assert.ok(networkPaths.some(value => value.startsWith('/api/')));
    assert.ok(socketPaths.includes('/'));
    assert.equal(await page.locator('link[rel="manifest"]').count(), 0);
    assert.deepEqual(unexpected, []);
    console.log('UKMesh shell, named region data and activity, terrain, observations, links and paths, PWA bootstrap, RF progress, privacy recovery and standalone routes passed');
  } finally {
    await browser.close();
  }
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
