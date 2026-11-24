'use strict';

const assert = require('node:assert/strict');
const vm = require('node:vm');
const loadDeployment = require('./load-deployment');
const { readUKMeshMapLink, ukmeshViewLink } = require('../../public/url-state');
const runtime = (privacyGeneration, overrides) => ({
  version: 1, packetArcs: false, heatmap: false, refreshAfterSeconds: 30, privacyGeneration, ...overrides,
});

function browser({ candidate = true, configured = true, load }) {
  const seen = { calls: [], reloads: 0, hidden: false, events: [] };
  const location = {
    origin: 'https://app.example.test', protocol: 'https:', host: 'app.example.test',
    reload() { seen.reloads++; },
  };
  const context = vm.createContext({
    AbortSignal, Event, URL,
    window: {
      location, setTimeout() {}, addEventListener() {},
      dispatchEvent(event) { seen.events.push(event.type); },
      CORE_SCOPE_DEPLOYMENT: candidate && configured ? {
        ukmeshApp: true,
        apiBase: '/corescope/api', wsPath: '/corescope/ws', privacyUrl: '/api/runtime-config',
      } : undefined,
    },
    document: {
      querySelector() { return candidate ? { hasAttribute: () => true } : null; },
      addEventListener() {},
      documentElement: {
        toggleAttribute(_attribute, value) { seen.hidden = value; },
      },
    },
    async fetch(url, options) {
      seen.calls.push({ url, options });
      return load(url, options);
    },
  });
  loadDeployment(context);
  return { seen, app: vm.runInContext('CoreScopeDeployment', context) };
}

async function verify() {
  let generation = 3;
  let responseGeneration = '3';
  const fixture = browser({
    load(url) {
      return url === '/api/runtime-config' ? {
        ok: !!generation,
        json: async () => runtime(generation),
      } : {
        ok: true,
        status: 200,
        headers: new Headers({ 'X-CoreScope-Privacy-Generation': responseGeneration }),
      };
    },
  });
  assert.equal(fixture.seen.hidden, true);
  assert.equal(fixture.app.socketUrl(), null);
  await fixture.app.refresh();
  assert.equal(fixture.seen.hidden, false);
  assert.equal(fixture.app.socketUrl(), 'wss://app.example.test/corescope/ws?privacyGeneration=3');
  await fixture.app.fetch('/api/nodes?limit=1', { credentials: 'include' });
  assert.equal(fixture.seen.calls.at(-1).url, '/corescope/api/nodes?limit=1');
  assert.equal(fixture.seen.calls.at(-1).options.credentials, 'omit');

  responseGeneration = '4';
  await assert.rejects(fixture.app.fetch('/api/nodes'), /temporarily unavailable/);
  assert.equal(fixture.seen.hidden, true);
  assert.equal(fixture.app.socketUrl(), null);
  generation = 4;
  await fixture.app.refresh();
  assert.equal(fixture.seen.reloads, 1);
  assert.deepEqual(fixture.seen.events, ['ukmesh:privacy-ready', 'ukmesh:privacy-unavailable']);

  let nativeGeneration = 7;
  const native = browser({
    load: async url => ({
      ok: true,
      json: async () => {
        if (url === '/api/runtime-config') return runtime(nativeGeneration, { regionScoping: true });
        if (url === '/api/nodes/map') nativeGeneration++;
        return { regions: [] };
      },
    }),
  });
  assert.deepEqual(await native.app.fetchUKMeshJson('/api/regions/summary'), { regions: [] });
  assert.equal(native.app.regionScoping(), true);
  assert.equal(native.seen.calls.find(call => call.url === '/api/regions/summary').options.credentials, 'omit');
  await assert.rejects(native.app.fetchUKMeshJson('/api/nodes/map'), /unavailable/);
  assert.equal(native.seen.hidden, true);
  assert.equal(native.app.socketUrl(), null);

  let regionsEnabled = true;
  const revoked = browser({ load: async () => ({
    ok: true,
    json: async () => runtime(8, { regionScoping: regionsEnabled }),
  }) });
  assert.equal(await revoked.app.refresh(), 8);
  assert.equal(revoked.app.regionScoping(), true);
  regionsEnabled = false;
  assert.equal(await revoked.app.refresh(), null);
  assert.equal(revoked.app.regionScoping(), false);
  assert.equal(revoked.seen.hidden, true);
  assert.equal(revoked.seen.reloads, 1);
  assert.equal(revoked.app.socketUrl(), null);

  for (const malformed of [
    null, { privacyGeneration: 8 }, runtime(8, { version: 2 }),
    runtime(8, { packetArcs: 'true' }), runtime(8, { heatmap: 1 }),
    runtime(8, { refreshAfterSeconds: 4 }), runtime(8, { refreshAfterSeconds: 301 }),
    runtime(0), runtime(8.5),
  ]) {
    const unavailable = browser({ load: async () => ({ ok: true, json: async () => malformed }) });
    assert.equal(await unavailable.app.refresh(), null);
    assert.equal(unavailable.app.socketUrl(), null);
    assert.equal(unavailable.seen.hidden, true);
  }
  let renderedConfig = runtime(8);
  const badUpdate = browser({ load: async () => ({ ok: true, json: async () => renderedConfig }) });
  assert.equal(await badUpdate.app.refresh(), 8);
  renderedConfig = runtime(8, { version: 2 });
  assert.equal(await badUpdate.app.refresh(), null);
  assert.deepEqual(badUpdate.seen.events, ['ukmesh:privacy-ready', 'ukmesh:privacy-unavailable']);
  assert.equal(badUpdate.app.socketUrl(), null);

  const missing = browser({ configured: false, load: () => assert.fail('candidate must fail closed') });
  await assert.rejects(missing.app.fetch('/api/nodes'), /temporarily unavailable/);
  assert.equal(missing.app.socketUrl(), null);

  const standalone = browser({ candidate: false, load: async () => ({ ok: true }) });
  await standalone.app.fetch('/api/stats');
  assert.equal(standalone.seen.calls[0].url, '/api/stats');
  assert.equal(standalone.app.socketUrl(), 'wss://app.example.test');

  const old = new URL('https://app.example.test/?map=51.12345,-1.43210,9.30&mode=traffic&layers=feed,heatmap&note=Hello&node=ABC&clashHops=2#region=UKNorth');
  const parsed = readUKMeshMapLink(old.search, old.hash);
  assert.deepEqual(parsed.viewport, { lat: 51.12345, lon: -1.43210, zoom: 9.30 });
  assert.deepEqual(parsed.layers, new Set(['feed', 'heatmap']));
  assert.equal(parsed.node, 'ABC');
  assert.equal(parsed.note, 'Hello');
  assert.equal(parsed.region, 'UKNorth');
  assert.equal(parsed.mode, 'traffic');
  assert.equal(parsed.clashHops, 2);
  assert.equal(readUKMeshMapLink('', '#/live?lat=0'), null);
  assert.equal(readUKMeshMapLink('', '#region=X&region=Y').region, null);
  assert.equal(readUKMeshMapLink('', '').clashHops, null);
  assert.equal(readUKMeshMapLink('?map=0,200,7', '').viewport, null);
  assert.equal(readUKMeshMapLink('?map=51,,7', '').viewport, null);
  const shared = ukmeshViewLink(old.href, { lat: 53, lon: -1, zoom: 12.35 });
  assert.equal(shared.hash, old.hash);
  assert.equal(shared.searchParams.get('layers'), old.searchParams.get('layers'));
  assert.equal(shared.searchParams.get('map'), '53.00000,-1.00000,12.35');
}

verify().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
